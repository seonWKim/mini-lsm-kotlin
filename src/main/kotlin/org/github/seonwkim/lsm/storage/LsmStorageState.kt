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
    val ssTables: ConcurrentHashMap<Int, SsTable> = ConcurrentHashMap()
) {


    companion object {
        fun create(options: LsmStorageOptions): LsmStorageState {
            // TODO: apply options when creating LsmStorageState
            return LsmStorageState(
                memTable = MemTable.create(0),
            )
        }
    }
}
