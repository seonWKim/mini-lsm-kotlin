package org.github.seonwkim.lsm

import org.github.seonwkim.lsm.memtable.MemTable
import org.github.seonwkim.lsm.sstable.SsTable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class LsmStorageState(
    memTable: MemTable,
) {
    // current memTable
    var memTable: MemTable = memTable
        private set

    // immutable memTables from latest to earliest earliest
    var immutableMemTables: LinkedList<MemTable> = LinkedList()

    // L0 SSTs, from latest to earliest
    val l0SsTables: MutableList<Int> = mutableListOf()

    val ssTables: ConcurrentHashMap<Int, SsTable> = ConcurrentHashMap()

    fun freezeMemtable() {
        val prevId = memTable.id
        val newMemtable = MemTable.create(prevId + 1)
        immutableMemTables.addFirst(memTable)
        this.memTable = newMemtable
    }
}
