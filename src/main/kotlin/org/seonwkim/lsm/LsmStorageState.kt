package org.seonwkim.lsm

import org.seonwkim.lsm.memtable.MemTable
import org.seonwkim.lsm.sstable.SsTable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class LsmStorageState(
    memTable: MemTable,
) {
    // current memTable
    var memTable: MemTable = memTable
        private set

    // immutable memtables from latest to earliest earliest
    var immutableMemtables: LinkedList<MemTable> = LinkedList()

    // L0 SSTs, from latest to earliest
    val l0SsTables: MutableList<Int> = mutableListOf()

    val ssTables: ConcurrentHashMap<Int, SsTable> = ConcurrentHashMap()

    fun freezeMemtable() {
        val prevId = memTable.id
        val newMemtable = MemTable.create(prevId + 1)
        immutableMemtables.addFirst(memTable)
        this.memTable = newMemtable
    }
}
