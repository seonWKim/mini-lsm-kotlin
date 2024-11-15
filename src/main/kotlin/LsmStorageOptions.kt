package org.example

class LsmStorageOptions(
    private var blockSize: Int,
    private var targetSstSize: Int,
    private var numMemtableLimit: Int,
) {
    companion object {
        fun defaultForWeek1Test(): LsmStorageOptions {
            return LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemtableLimit = 10
            )
        }
    }

    fun setBlockSize(blockSize: Int) {
        this.blockSize = blockSize
    }

    fun setTargetSstSize(targetSstSize: Int) {
        this.targetSstSize = targetSstSize
    }

    fun setNumMemtableLimit(numMemtableLimit: Int) {
        this.numMemtableLimit = numMemtableLimit
    }
}
