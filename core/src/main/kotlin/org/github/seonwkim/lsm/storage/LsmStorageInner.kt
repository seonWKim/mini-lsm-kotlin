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
    private val stateManager: LsmStorageStateConcurrencyManager,
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

    fun snapshot(): LsmStateSnapshot {
        return stateManager.snapshot()
    }

    fun get(key: ComparableByteArray): ComparableByteArray? {
        return get(TimestampedKey(key))
    }

    fun get(key: TimestampedKey): ComparableByteArray? {
        val snapshot = stateManager.snapshot(timestamp = 0L) // TODO: set timestamp accordingly

        snapshot.getFromMemTable(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from immutable memTables
                }
            }
        }

        snapshot.getFromImmutableMemTables(key).let {
            when {
                it.isValid() -> return it!!.value
                it.isDeleted() -> return null
                else -> {
                    // find from l0 sstables
                }
            }
        }

        return snapshot.getFromSstables(key)
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        stateManager.put(key, MemtableValue(value))
    }

    fun delete(key: ComparableByteArray) {
        stateManager.put(key, MemtableValue(ComparableByteArray.new()))
    }

    fun forceFreezeMemTable() {
        val newMemTableId = stateManager.forceFreezeMemTable()
        manifest?.addRecord(NewMemTable(newMemTableId))
    }

    /**
     * Force flush the earliest created immutable memTable to disk
     */
    fun forceFlushNextImmMemTable() {
        val previousMemtableId = stateManager.forceFlushNextImmutableMemTable()
        manifest?.addRecord(Flush(previousMemtableId))
    }

    /**
     * Trigger flush operation when number of immutable memTables exceeds limit.
     */
    fun triggerFlush() {
        if (stateManager.shouldFlush()) {
            forceFlushNextImmMemTable()
        }
    }

    fun scan(lower: Bound, upper: Bound): FusedIterator {
        // memTable iterators
        val snapshot = stateManager.snapshot()
        val memTableIters = listOf(snapshot.getMemTableIterator(lower, upper)) +
                snapshot.getImmutableMemTablesIterator(lower, upper)
        val memTableMergeIter = MergeIterator(memTableIters)
        val l0MergeIter = MergeIterator(snapshot.getL0SstablesIterator(lower, upper))
        val levelIters = snapshot.getLevelIterators(lower, upper)
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
