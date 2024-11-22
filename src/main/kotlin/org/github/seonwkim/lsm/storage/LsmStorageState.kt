package org.github.seonwkim.lsm.storage

import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.sstable.SsTable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class LsmStorageState(
    // current memTable
    var memTable: MemTable,
    // immutable memTables from latest to earliest earliest
    var immutableMemTables: LinkedList<MemTable> = LinkedList(),
    // L0 SSTs, from latest to earliest
    val l0SsTables: LinkedList<Int> = LinkedList(),
    val levels: List<SstLevel> = LinkedList(),
    // sst id, SSTable
    val ssTables: ConcurrentHashMap<Int, SsTable> = ConcurrentHashMap()
) {


    companion object {
        fun create(options: LsmStorageOptions): LsmStorageState {
            val levels = when (options.compactionOptions) {
                is CompactionOptions.Leveled -> {
                    (1..options.compactionOptions.options.maxLevel)
                        .map { SstLevel(level = it, tables = mutableListOf()) }
                }

                is CompactionOptions.Simple -> {
                    (1..options.compactionOptions.options.maxLevels)
                        .map { SstLevel(level = it, tables = mutableListOf()) }
                }

                is CompactionOptions.Tiered -> {
                    mutableListOf()
                }

                is CompactionOptions.NoCompaction -> {
                    mutableListOf(SstLevel(level = 1, tables = mutableListOf()))
                }
            }
            return LsmStorageState(
                memTable = MemTable.create(0),
                levels = levels
            )
        }
    }
}

/**
 * SsTables sorted by key range.
 * L1 ~ L_max for leveled compaction, or tiers for tiered compaction.
 */
data class SstLevel(
    val level: Int,
    val tables: MutableList<Int>
)
