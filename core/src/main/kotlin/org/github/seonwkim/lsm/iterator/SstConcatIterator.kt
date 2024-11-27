package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.sstable.Sstable

class SstConcatIterator(
    private var current: SsTableIterator?,
    private var nextSstIdx: Int,
    private val sstables: List<Sstable>
) : StorageIterator {
    companion object {
        fun checkSstValid(sstables: List<Sstable>) {
            for (sst in sstables) {
                if (sst.firstKey > sst.lastKey) {
                    throw IllegalStateException("sst first key(${sst.firstKey} is larger than sst last key(${sst.lastKey})")
                }
            }

            if (sstables.isNotEmpty()) {
                for (i in 0..<sstables.lastIndex) {
                    if (sstables[i].lastKey >= sstables[i + 1].firstKey) {
                        throw IllegalStateException(
                            "previous sst table's lastKey(${sstables[i].lastKey}) is not smaller than next sst table's first key(${sstables[i + 1].firstKey})"
                        )
                    }
                }
            }
        }

        fun createAndSeekToFirst(sstables: List<Sstable>): SstConcatIterator {
            checkSstValid(sstables)
            if (sstables.isEmpty()) {
                return SstConcatIterator(
                    current = null,
                    nextSstIdx = 0,
                    sstables = sstables
                )
            }

            val iter = SstConcatIterator(
                current = SsTableIterator.createAndSeekToFirst(sstables[0]),
                nextSstIdx = 1,
                sstables
            )
            iter.moveUntilValid()
            return iter
        }

        fun createAndSeekToKey(sstables: List<Sstable>, key: TimestampedKey): SstConcatIterator {
            checkSstValid(sstables)
            val idx = findMinimumSstableIdxContainingKey(sstables, key)
            if (idx >= sstables.size) {
                return SstConcatIterator(
                    current = null,
                    nextSstIdx = sstables.size,
                    sstables = sstables
                )
            }

            val iter = SstConcatIterator(
                current = SsTableIterator.createAndSeekToKey(sstables[idx], key),
                nextSstIdx = idx + 1,
                sstables = sstables
            )
            iter.moveUntilValid()
            return iter
        }
    }

    private fun moveUntilValid() {
        while (current != null) {
            if (current!!.isValid()) {
                break
            }

            if (nextSstIdx >= sstables.size) {
                current = null
            } else {
                current = SsTableIterator.createAndSeekToFirst(sstables[nextSstIdx])
                nextSstIdx += 1
            }
        }
    }

    override fun key(): ComparableByteArray {
        if (current == null) {
            throw Error("Use isValid() function before calling this function")
        }
        return current!!.key()
    }

    override fun value(): ComparableByteArray {
        if (current == null) {
            throw Error("Use isValid() function before calling this function")
        }
        return current!!.value()
    }

    override fun isValid(): Boolean {
        return current?.isValid() == true
    }

    override fun next() {
        current?.next()
        moveUntilValid()
    }

    override fun copy(): StorageIterator {
        TODO("Not yet implemented")
    }

    override fun numActiveIterators(): Int {
        return 1
    }
}

fun findMinimumSstableIdxContainingKey(sstables: List<Sstable>, key: TimestampedKey): Int {
    return sstables.binarySearch { it.firstKey.compareTo(key) }
        .let { idx ->
            if (idx < 0) maxOf(-idx - 2, 0)
            else idx
        }
}
