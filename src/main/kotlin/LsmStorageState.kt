package org.example

class LsmStorageState(
    val memtable: MemTable, // current memtable
    val immutableMemtables: List<MemTable> = emptyList() // immutable memtables from latest to earliest ¬ˆ
)
