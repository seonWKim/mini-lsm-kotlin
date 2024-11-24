package org.github.seonwkim.lsm.storage

data class LsmStorageOptions(
    // block size in bytes
    val blockSize: Int,

    // SST size in bytes, the approximate memTable capacity limit
    val targetSstSize: Int,

    // maximum number of memtables in memory, flush to L0 when exceeding this limit
    val numMemTableLimit: Int,

    // whether wal is enabled
    val enableWal: Boolean = false,

    val compactionOptions: CompactionOptions = NoCompaction,

    // flush delay in millisecond
    val flushDelayMillis: Long = 50,

    // flush interval in millisecond
    val flushIntervalMillis: Long = 50,

    // TODO: add explanation
    val serializable: Boolean = false
)
