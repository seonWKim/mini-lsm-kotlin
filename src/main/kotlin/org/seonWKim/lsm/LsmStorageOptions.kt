package org.seonWKim.lsm

class LsmStorageOptions(blockSize: Int, targetSstSize: Int, numMemTableLimit: Int) {
    var blockSize: Int = blockSize
        private set
    var targetSstSize: Int = targetSstSize
        private set
    var numMemtableLimit: Int = numMemTableLimit
        private set

    companion object {
        fun defaultForWeek1Test(): LsmStorageOptions {
            return LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 10
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
