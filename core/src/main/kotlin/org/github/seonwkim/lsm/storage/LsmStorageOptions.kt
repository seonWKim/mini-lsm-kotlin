package org.github.seonwkim.lsm.storage

import org.github.seonwkim.lsm.storage.compaction.option.CompactionOptions
import org.github.seonwkim.lsm.storage.compaction.option.NoCompaction

data class LsmStorageOptions(
    // block size in bytes
    val blockSize: Int,

    // used to determine the size of sst
    val targetSstSize: Int,

    // maximum number of memtables in memory, flush to L0 when exceeding this limit
    val numMemTableLimit: Int,

    // whether to enable flush, if set to false, l0 flush will never occur
    val enableFlush: Boolean = true,

    // flush delay in millisecond
    val flushDelayMillis: Long = 50,

    // flush interval in millisecond
    val flushIntervalMillis: Long = 50,

    // whether wal is enabled
    val enableWal: Boolean = false,

    // whether to enable compaction
    val enableCompaction: Boolean = true,

    val compactionOptions: CompactionOptions = NoCompaction,

    // compaction interval in milliseconds
    val compactionIntervalMillis: Long = 50,

    // TODO: add explanation
    val serializable: Boolean = false,
)
