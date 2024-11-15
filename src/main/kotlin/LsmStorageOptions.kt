package org.example

class LsmStorageOptions {
    var blockSize: Int
        private set
    var targetSstSize: Int
        private set
    var numMemtableLimit: Int
        private set

    constructor(blockSize: Int, targetSstSize: Int, numMemtableLimit: Int) {
        this.blockSize = blockSize
        this.targetSstSize = targetSstSize
        this.numMemtableLimit = numMemtableLimit
    }

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
