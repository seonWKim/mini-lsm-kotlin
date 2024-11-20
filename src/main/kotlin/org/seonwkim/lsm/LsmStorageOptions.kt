package org.seonwkim.lsm

data class LsmStorageOptions(
    // block size in bytes
    val blockSize: Int,

    // SST size in bytes, the approximate memtable capacity limit
    val targetSstSize: Int,

    // maximum number of memtables in memory, flush to L0 when exceeding this limit
    val numMemTableLimit: Int
)
