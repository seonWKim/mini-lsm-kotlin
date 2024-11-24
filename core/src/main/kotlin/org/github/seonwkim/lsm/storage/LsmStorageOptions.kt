package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.DefaultRwLock
import org.github.seonwkim.common.RwLock

data class LsmStorageOptions(
    // block size in bytes
    val blockSize: Int,

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
    val serializable: Boolean = false,
    val customizableMemTableLock: RwLock<Unit> = DefaultRwLock(Unit),
) {
    fun toStateOptions() = LsmStorageStateOptions(
        blockSize = blockSize,
        targetSstSize = targetSstSize,
        numMemTableLimit = numMemTableLimit,
        enableWal = enableWal,
        serializable = serializable,
        customizableMemTableLock = customizableMemTableLock,
    )

    fun toMiniLsmOptions() = MiniLsmOptions(
        flushDelayMillis = flushDelayMillis,
        flushIntervalMillis = flushIntervalMillis,
        compactionOptions = compactionOptions
    )
}

data class MiniLsmOptions(
    // flush delay in millisecond
    val flushDelayMillis: Long = 50,

    // flush interval in millisecond
    val flushIntervalMillis: Long = 50,

    val compactionOptions: CompactionOptions = NoCompaction,
)

data class LsmStorageStateOptions(
    // block size in bytes
    val blockSize: Int,

    val targetSstSize: Int,

    // maximum number of memtables in memory, flush to L0 when exceeding this limit
    val numMemTableLimit: Int,

    val enableWal: Boolean,

    // TODO: add explanation
    val serializable: Boolean,

    val customizableMemTableLock: RwLock<Unit>,
)
