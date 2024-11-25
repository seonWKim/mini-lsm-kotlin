package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.*
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists

class LsmStorageInner private constructor(
    val path: Path,
    val stateManager: LsmStorageStateConcurrencyManager,
    private val compactionController: CompactionController,
    private val manifest: Manifest?,
) {

    companion object {
        private val log = mu.KotlinLogging.logger { }

        fun open(
            path: Path,
            options: LsmStorageOptions
        ): LsmStorageInner {
            if (!path.exists()) {
                log.info { "Creating directories $path" }
                try {
                    Files.createDirectories(path)
                } catch (e: Exception) {
                    log.warn { "Errors creating DB dir: $e" }
                    throw e
                }
            }
            val stateManager = LsmStorageStateConcurrencyManager(
                path = path,
                state = LsmStorageState.create(options),
                options = options.toStateOptions(),
                nextSstId = AtomicInteger(1),
                blockCache = BlockCache(1 shl 20)
            )

            val compactionOptions = options.compactionOptions
            val compactionController: CompactionController = when (compactionOptions) {
                is Leveled -> {
                    LeveledCompactionController(compactionOptions.options)
                }

                is Tiered -> {
                    TieredCompactionController(compactionOptions.options)
                }

                is Simple -> {
                    SimpleCompactionController(compactionOptions.options)
                }

                is NoCompaction -> {
                    NoCompactionController
                }
            }

            val manifestPath = path.resolve("MANIFEST")
            lateinit var manifest: Manifest
            var lastCommitTs = 0L
            if (!manifestPath.exists()) {
                if (options.enableWal) {
                    // TODO("create with WAL")
                }
                manifest = try {
                    Manifest.create(manifestPath)
                } catch (e: Exception) {
                    throw Error("Failed to create manifest file: $e")
                }
                manifest.addRecord(NewMemTable(stateManager.memTableId()))
            } else {
                val (m, records) = Manifest.recover(manifestPath)
                val memTables = mutableSetOf<Int>()
                for (record in records) {
                    when (record) {
                        is Flush -> {
                            val sstId = record.sstId
                            val removed = memTables.remove(sstId)
                            if (!removed) {
                                throw Error("memtable of id $sstId not exists")
                            }
                            if (compactionController.flushTol0()) {
                                stateManager.addNewL0SstableId(sstId)
                            } else {
                                stateManager.addNewSstLevel(SstLevel(sstId, mutableListOf(sstId)))
                            }

                            stateManager.setNextSstId(maxOf(stateManager.memTableId(), sstId))
                        }

                        is NewMemTable -> {
                            stateManager.setNextSstId(maxOf(stateManager.nextSstId(), record.memTableId))
                            memTables.add(record.memTableId)
                        }

                        is Compaction -> {
                            TODO()
                        }
                    }
                }

                var sstCount = 0
                val tableIds = stateManager.getTotalSstableIds()
                for (sstableId in tableIds) {
                    val sst = Sstable.open(
                        id = sstableId,
                        blockCache = stateManager.blockCache.copy(),
                        file = SsTableFile.open(sstPath(path, sstableId))
                    )
                    lastCommitTs = maxOf(lastCommitTs, sst.maxTs)
                    stateManager.setSstable(sstableId, sst)
                    sstCount += 1
                }
                log.info { "$sstCount SSTs opened" }

                stateManager.incrementNextSstId()

                // For leveled compaction, sort SSTs on each level
                if (compactionController is LeveledCompactionController) {
                    stateManager.sortLevels()
                }

                // Recover memTables
                if (options.enableWal) {
                    TODO()
                } else {
                    stateManager.setMemTable(memTable = MemTable.create(stateManager.nextSstId()))
                }
                m.addRecord(NewMemTable(stateManager.memTableId()))
                stateManager.incrementNextSstId()
                manifest = m
            }
            stateManager.setManifest(manifest)

            return LsmStorageInner(
                path = path,
                stateManager = stateManager,
                compactionController = compactionController,
                manifest = manifest,
            )
        }

        fun open(
            path: Path,
            options: LsmStorageOptions,
            stateManager: LsmStorageStateConcurrencyManager,
        ): LsmStorageInner {
            if (!path.exists()) {
                log.info { "Creating directories $path" }
                Files.createDirectories(path)
            }

            return LsmStorageInner(
                path = path,
                stateManager = stateManager,
                compactionController = NoCompactionController,
                manifest = null
            )
        }
    }

    /**
     * Retrieves the value associated with the specified key from the storage.
     *
     * This method first attempts to retrieve the value from the current memTable.
     * If the key is not found or is marked as deleted, it then checks the immutable memTables.
     * If the key is still not found, it finally checks the SSTables.
     *
     * @param key The key to be retrieved. It must be a `ComparableByteArray`.
     * @return The value associated with the key, or `null` if the key is not found or is marked as deleted.
     */
    fun get(key: ComparableByteArray): ComparableByteArray? {
        return get(TimestampedKey(key))
    }

    /**
     * Retrieves the value associated with the specified timestamped key from the storage.
     *
     * This method first attempts to retrieve the value from the current memTable.
     * If the key is not found or is marked as deleted, it then checks the immutable memTables.
     * If the key is still not found, it finally checks the SSTables.
     *
     * @param key The timestamped key to be retrieved. It must be a `TimestampedKey`.
     * @return The value associated with the key, or `null` if the key is not found or is marked as deleted.
     */
    fun get(key: TimestampedKey): ComparableByteArray? {
        stateManager.getFromMemTable(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from immutable memTables
                }
            }
        }

        stateManager.getFromImmutableMemTables(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from l0 sstables
                }
            }
        }

        return stateManager.getFromSstables(key)
    }

    /**
     * Inserts a key-value pair into the current memTable.
     *
     * This method adds the specified key-value pair to the current memTable.
     *
     * @param key The key to be inserted. It must be a `ComparableByteArray` to ensure proper ordering.
     * @param value The value to be associated with the key. It must be a `ComparableByteArray`.
     */
    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        stateManager.put(key, MemtableValue(value))
    }

    /**
     * Deletes a key from the current memTable.
     *
     * This method marks the specified key as deleted in the current memTable.
     *
     * @param key The key to be deleted. It must be a `ComparableByteArray`.
     */
    fun delete(key: ComparableByteArray) {
        stateManager.put(key, MemtableValue(ComparableByteArray.new()))
    }

    /**
     * Freezes the current memTable and creates a new one.
     *
     * This method forces the current memTable to be frozen and moved to the list of immutable memTables.
     * A new memTable is then created and set as the current memTable.
     */
    fun forceFreezeMemTable() {
        val newMemTableId = stateManager.forceFreezeMemTable()
        manifest?.addRecord(NewMemTable(newMemTableId))
    }

    /**
     * Force flushes the earliest created immutable memTable to disk.
     *
     * This method flushes the oldest immutable memTable to disk, creating a new SSTable.
     * The method updates the manifest with the flushed memTable ID.
     *
     * @throws IllegalStateException if no immutable memTables exist.
     */
    fun forceFlushNextImmMemTable() {
        val previousMemtableId = stateManager.forceFlushNextImmutableMemTable()
        manifest?.addRecord(Flush(previousMemtableId))
    }

    /**
     *
     */
    fun forceFullCompaction() {

    }

    /**
     * Triggers a flush operation when the number of immutable memTables exceeds the configured limit.
     *
     * This method checks if the number of immutable memTables has exceeded the limit set in the configuration.
     * If the limit is exceeded, it forces a flush of the earliest created immutable memTable to disk.
     */
    fun triggerFlush() {
        if (stateManager.shouldFlush()) {
            forceFlushNextImmMemTable()
        }
    }

    /**
     * Scans the storage for key-value pairs within the specified bounds.
     *
     * This method creates iterators for the current memTable, immutable memTables, and SSTables at different levels.
     * It then merges these iterators to provide a unified view of the key-value pairs within the specified bounds.
     *
     * @param lower The lower bound of the scan. It must be a `Bound`.
     * @param upper The upper bound of the scan. It must be a `Bound`.
     * @return A `FusedIterator` that iterates over the key-value pairs within the specified bounds.
     */
    fun scan(lower: Bound, upper: Bound): FusedIterator {
        // memTable iterators
        val memTableIters = listOf(stateManager.getMemTableIterator(lower, upper)) +
                stateManager.getImmutableMemTablesIterator(lower, upper)
        val memTableMergeIter = MergeIterator(memTableIters)
        val l0MergeIter = MergeIterator(stateManager.getL0SstablesIterator(lower, upper))
        val levelIters = stateManager.getLevelIterators(lower, upper)
        val `memTableIter plus l0Iter` = TwoMergeIterator.create(memTableMergeIter, l0MergeIter)
        val `levelIter added iter` =
            TwoMergeIterator.create(`memTableIter plus l0Iter`, MergeIterator(levelIters))

        return FusedIterator(
            iter = LsmIterator.new(
                iter = `levelIter added iter`,
                endBound = upper
            )
        )
    }
}
