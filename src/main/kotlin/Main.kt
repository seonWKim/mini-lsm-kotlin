package org.example

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

fun main() {
    println("Hello World!")
}

class MemTable(
    val map: ConcurrentSkipListMap<ByteBuffer, ByteBuffer>,
    val wal: Wal?,
    val id: Int,
) {
    companion object {
        fun create(id: Int): MemTable {
            TODO()
        }
    }

    fun forTestingPutSlice(key: ByteArray, value: ByteArray) {
        TODO()
    }

    fun forTestingGetSlice(key: ByteArray): ByteArray {
        TODO()
    }

    fun approximateSize(): Int {
        TODO()
    }
}

class Wal(

)

class LsmStorageInner(
    val state: LsmStorageState // TODO: we need to handle concurrent read/write on state
) {

    val stateLock: ReentrantReadWriteLock = ReentrantReadWriteLock()

    companion object {
        fun open(path: Path, options: LsmStorageOptions): LsmStorageInner {
            TODO()
        }
    }

    fun get(key: ByteArray): ByteArray? {
        TODO()
    }

    fun put(key: ByteArray, value: ByteArray) {
        TODO()
    }

    fun delete(key: ByteArray) {
        TODO()
    }

    fun forceFreezeMemtable() {
        TODO()
    }
}

class LsmStorageOptions(
    private var blockSize: Int,
    private var targetSstSize: Int,
    private var numMemtableLimit: Int,
) {
    companion object {
        fun default_for_week1_test(): LsmStorageOptions {
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

class LsmStorageState(
    val immutableMemtables: List<MemTable> = emptyList() // TODO
) {
}
