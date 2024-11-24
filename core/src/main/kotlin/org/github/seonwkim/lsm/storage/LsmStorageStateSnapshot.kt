package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.iterator.*
import org.github.seonwkim.lsm.memtable.MemtableValue
import org.github.seonwkim.lsm.memtable.isDeleted
import org.github.seonwkim.lsm.memtable.isValid
import org.github.seonwkim.lsm.sstable.Sstable

/**
 * Represents a snapshot of the LSM storage state at a specific point in time.
 *
 * @property state The current state of the LSM storage.
 * @property timestamp The timestamp when the snapshot was taken.
 */
class LsmStorageStateSnapshot(
    val state: LsmStorageState,
    val timestamp: Long
) {
    /**
     * Retrieves a value from the current memTable using the provided key.
     *
     * @param key The key to search for in the memTable.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromMemTable(key: TimestampedKey): MemtableValue? {
        return state.memTable.get().get(key)
    }

    fun getMemTableIterator(lower: Bound, upper: Bound): MemTableIterator {
        return state.memTable.get().iterator(lower, upper)
    }

    /**
     * Retrieves a value from the immutable memTables using the provided key.
     *
     * @param key The key to search for in the immutable memTables.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromImmutableMemTables(key: TimestampedKey): MemtableValue? {
        val immutableMemTables = state.immutableMemTables
        for (immMemTable in immutableMemTables) {
            val result = immMemTable.get(key)
            if (result.isValid() || result.isDeleted()) {
                return result
            }
        }

        return null
    }

    fun getImmutableMemTablesIterator(lower: Bound, upper: Bound): List<MemTableIterator> {
        return state.immutableMemTables.map { it.iterator(lower, upper) }
    }

    fun getImmutableMemTablesSize(): Int {
        return state.immutableMemTables.size
    }

    fun getImmutableMemTableApproximateSize(idx: Int): Int {
        return state.immutableMemTables.get(idx).approximateSize()
    }

    /**
     * Retrieves a value from the SSTables using the provided key.
     *
     * @param key The key to search for in the SSTables.
     * @return The value associated with the key, or null if not found.
     */
    fun getFromSstables(key: TimestampedKey): ComparableByteArray? {
        val l0Iter = MergeIterator(getL0SsTableIterators(key))
        val levelIters = MergeIterator(getSstConcatIterator(key))
        val totalMergedIter = TwoMergeIterator.create(
            first = l0Iter,
            second = levelIters
        )

        if (totalMergedIter.isValid() &&
            totalMergedIter.key() == key.bytes && // TODO: timestamp comparison between keys
            !totalMergedIter.value().isEmpty()
        ) {
            return totalMergedIter.value()
        }

        return null
    }

    private fun getL0SsTableIterators(key: TimestampedKey): List<SsTableIterator> {
        return state.l0Sstables.mapNotNull { l0SstableIdx ->
            val table = state.sstables[l0SstableIdx]!!
            if (table.firstKey <= key && key <= table.lastKey) {
                SsTableIterator.createAndSeekToKey(table, key)
            } else {
                null
            }
        }
    }

    private fun getSstConcatIterator(key: TimestampedKey): List<SstConcatIterator> {
        return state.levels.map { level ->
            val validLevelSsts = mutableListOf<Sstable>()
            level.sstIds.forEach { levelSstId ->
                val table = state.sstables[levelSstId]!!
                if (keepTable(key, table)) {
                    validLevelSsts.add(table)
                }
            }
            SstConcatIterator.createAndSeekToKey(validLevelSsts, key)
        }
    }

    private fun keepTable(key: TimestampedKey, table: Sstable): Boolean {
        if (table.firstKey <= key && key <= table.lastKey) {
            if (table.bloom?.mayContain(farmHashFingerPrintU32(key.bytes)) == true) {
                return true
            }
        } else {
            return true
        }

        return false
    }

    fun getL0SstablesSize(): Int {
        return state.sstables.size
    }

    fun getL0SstablesIterator(lower: Bound, upper: Bound): List<SsTableIterator> {
        return state.l0Sstables.mapNotNull { idx ->
            val table = state.sstables[idx]!!
            if (rangeOverlap(lower, upper, table.firstKey, table.lastKey)) {
                when (lower) {
                    is Included -> SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.key))
                    is Excluded -> {
                        val iter = SsTableIterator.createAndSeekToKey(table, TimestampedKey(lower.key))
                        if (iter.isValid() && iter.key() == lower.key) {
                            iter.next()
                        }
                        iter
                    }

                    is Unbounded -> SsTableIterator.createAndSeekToFirst(table)
                }
            } else null
        }
    }

    private fun rangeOverlap(
        userBegin: Bound,
        userEnd: Bound,
        tableBegin: TimestampedKey,
        tableEnd: TimestampedKey
    ): Boolean {
        when (userEnd) {
            is Excluded -> if (userEnd.key <= tableBegin.bytes) {
                return false
            }

            is Included -> if (userEnd.key < tableBegin.bytes) {
                return false
            }

            else -> {}
        }

        when (userBegin) {
            is Excluded -> if (userBegin.key >= tableEnd.bytes) {
                return false
            }

            is Included -> if (userBegin.key > tableEnd.bytes) {
                return false
            }

            else -> {}
        }

        return true
    }

    fun getLevelIterators(lower: Bound, upper: Bound): List<SstConcatIterator> {
        return state.levels.map { level ->
            val levelSsts = mutableListOf<Sstable>()
            level.sstIds.forEach {
                val sstable = state.sstables[it]!!
                if (rangeOverlap(
                        userBegin = lower,
                        userEnd = upper,
                        tableBegin = sstable.firstKey,
                        tableEnd = sstable.lastKey
                    )
                ) {
                    levelSsts.add(sstable)
                }
            }

            when (lower) {
                is Included -> {
                    SstConcatIterator.createAndSeekToKey(
                        sstables = levelSsts,
                        key = TimestampedKey(lower.key),
                    )
                }

                is Excluded -> {
                    val iter = SstConcatIterator.createAndSeekToKey(
                        sstables = levelSsts,
                        key = TimestampedKey(lower.key),
                    )
                    while (iter.isValid() && iter.key() == lower.key) {
                        iter.next()
                    }
                    iter
                }

                is Unbounded -> {
                    SstConcatIterator.createAndSeekToFirst(levelSsts)
                }
            }
        }
    }
}
