package org.example

import java.util.*

class LsmStorageState {
    // current memtable
    var memtable: MemTable
        private set

    // immutable memtables from latest to earliest earliest
    var immutableMemtables: LinkedList<MemTable>

    constructor(memtable: MemTable) {
        this.memtable = memtable
        this.immutableMemtables = LinkedList()
    }

    fun freezeMemtable() {
        val prevId = memtable.id
        val newMemtable = MemTable.create(prevId + 1)
        immutableMemtables.addFirst(memtable)
        this.memtable = newMemtable
    }
}
