package org.seonwkim.lsm

data class LsmStorageOptions(
    val blockSize: Int,
    val targetSstSize: Int,
    val numMemTableLimit: Int
)
