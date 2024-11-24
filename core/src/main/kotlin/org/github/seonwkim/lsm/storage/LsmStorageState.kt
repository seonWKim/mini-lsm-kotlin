package org.github.seonwkim.lsm.storage

import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.sstable.Sstable
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Represents the state of the LSM storage.
 *
 * @property memTable The current memTable.
 * @property immutableMemTables The immutable memTables from latest to earliest.
 * @property l0Sstables The L0 SSTs, from latest to earliest.
 * @property levels The levels of SSTables.
 * @property sstables The map of SSTable IDs to SSTable instances.
 */
class LsmStorageState(
    var memTable: AtomicReference<MemTable>,
    var immutableMemTables: LinkedList<MemTable> = LinkedList(),
    val l0Sstables: LinkedList<Int> = LinkedList(),
    val levels: LinkedList<SstLevel> = LinkedList(),
    val sstables: ConcurrentHashMap<Int, Sstable> = ConcurrentHashMap()
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
                memTable = AtomicReference(MemTable.create(0)),
                levels = levels
            )
        }
    }
}
