package org.github.seonwkim.lsm.storage

import com.google.common.annotations.VisibleForTesting
import org.github.seonwkim.common.lock.PriorityAwareLock
import org.github.seonwkim.common.lock.RwLock
import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.sstable.Sstable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * Represents the state of the LSM storage.
 *
 * @property memTable The current memTable.
 * @property immutableMemTables The immutable memTables from latest to earliest.
 * @property l0Sstables The L0 SSTs, from latest to earliest.
 * @property levels Sstables sorted by key range; L1 ~ L_max for leveled compaction, or tiers for tiered compaction.
 * @property sstables The map of SSTable IDs to SSTable instances.
 */
class LsmStorageState private constructor(
    var memTable: RwLock<MemTable>,
    var immutableMemTables: RwLock<LinkedList<MemTable>>,
    val l0Sstables: RwLock<LinkedList<Int>>,
    val levels: RwLock<LinkedList<SstLevel>>,
    val sstables: ConcurrentHashMap<Int, Sstable>
) {
    companion object {
        fun create(options: LsmStorageOptions): LsmStorageState {
            val levels = when (options.compactionOptions) {
                is Leveled -> {
                    (1..options.compactionOptions.options.maxLevel)
                        .map { SstLevel(level = it, sstIds = mutableListOf()) }
                }

                is Simple -> {
                    (1..options.compactionOptions.options.maxLevels)
                        .map { SstLevel(level = it, sstIds = mutableListOf()) }
                }

                is Tiered -> {
                    mutableListOf()
                }

                is NoCompaction -> {
                    mutableListOf(SstLevel(level = 1, sstIds = mutableListOf()))
                }
            }.toCollection(LinkedList())

            return LsmStorageState(
                memTable = PriorityAwareLock(value = MemTable.create(0), priority = 10),
                immutableMemTables = PriorityAwareLock(value = LinkedList(), priority = 20),
                l0Sstables = PriorityAwareLock(value = LinkedList(), priority = 30),
                levels = PriorityAwareLock(value = levels, priority = 40),
                sstables = ConcurrentHashMap()
            )
        }

        fun create(
            memTable: MemTable,
            immutableMemTables: LinkedList<MemTable>,
            l0Sstables: LinkedList<Int>,
            levels: LinkedList<SstLevel>,
            sstables: ConcurrentHashMap<Int, Sstable>
        ): LsmStorageState {
            return LsmStorageState(
                memTable = PriorityAwareLock(value = memTable, priority = 10),
                immutableMemTables = PriorityAwareLock(value = immutableMemTables, priority = 20),
                l0Sstables = PriorityAwareLock(value = l0Sstables, priority = 30),
                levels = PriorityAwareLock(value = levels, priority = 40),
                sstables = sstables
            )
        }

        @VisibleForTesting
        fun createWithCustomLock(
            memTable: RwLock<MemTable>,
            immutableMemTables: RwLock<LinkedList<MemTable>>,
            l0Sstables: RwLock<LinkedList<Int>>,
            levels: RwLock<LinkedList<SstLevel>>,
            sstables: ConcurrentHashMap<Int, Sstable>
        ): LsmStorageState {
            return LsmStorageState(
                memTable = memTable,
                immutableMemTables = immutableMemTables,
                l0Sstables = l0Sstables,
                levels = levels,
                sstables = sstables
            )
        }
    }

    fun setMemTable(memTable: MemTable) {
        this.memTable = PriorityAwareLock(value = memTable, priority = 10)
    }
}
