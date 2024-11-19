package org.seonwkim.lsm

import org.seonwkim.lsm.memtable.MemTable
import java.util.*

class LsmStorageState(memTable: MemTable) {
    // current memTable
    var memTable: MemTable = memTable
        private set

    // immutable memtables from latest to earliest earliest
    var immutableMemtables: LinkedList<MemTable> = LinkedList()

    fun freezeMemtable() {
        val prevId = memTable.id
        val newMemtable = MemTable.create(prevId + 1)
        immutableMemtables.addFirst(memTable)
        this.memTable = newMemtable
    }
}
