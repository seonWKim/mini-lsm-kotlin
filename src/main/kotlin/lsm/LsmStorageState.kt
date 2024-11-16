package org.example.lsm

import org.example.lsm.memtable.MemTable
import java.util.*

class LsmStorageState {
    // current memTable
    var memTable: MemTable
        private set

    // immutable memtables from latest to earliest earliest
    var immutableMemtables: LinkedList<MemTable>

    constructor(memTable: MemTable) {
        this.memTable = memTable
        this.immutableMemtables = LinkedList()
    }

    fun freezeMemtable() {
        val prevId = memTable.id
        val newMemtable = MemTable.create(prevId + 1)
        immutableMemtables.addFirst(memTable)
        this.memTable = newMemtable
    }
}
