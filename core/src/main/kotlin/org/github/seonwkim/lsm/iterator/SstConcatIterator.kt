package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.sstable.Sstable

/**
 * SstConcatIterator is an iterator for traversing entries across multiple SSTables.
 * It concatenates multiple SSTable iterators and supports iteration over key-value pairs.
 *
 * @property current the current SSTable iterator
 * @property nextSstIdx the index of the next SSTable to iterate over
 * @property sstables the list of SSTables to iterate over
 */
class SstConcatIterator(
    private var current: SsTableIterator?,
    private var nextSstIdx: Int,
    private val sstables: List<Sstable>
) : StorageIterator {

    companion object {
        /**
         * Creates an SstConcatIterator and seeks to the first entry.
         *
         * @param sstables the list of SSTables to iterate over
         * @return a new SstConcatIterator
         */
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

        /**
         * Creates an SstConcatIterator and seeks to the specified key.
         *
         * @param sstables the list of SSTables to iterate over
         * @param key the key to seek to
         * @return a new SstConcatIterator
         */
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

    /**
     * Moves the iterator until it points to a valid entry.
     */
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

/**
 * Finds the minimum SSTable index containing the specified key.
 *
 * @param sstables the list of SSTables to search
 * @param key the key to find
 * @return the index of the SSTable containing the key
 */
fun findMinimumSstableIdxContainingKey(sstables: List<Sstable>, key: TimestampedKey): Int {
    return sstables.binarySearch { it.firstKey.compareTo(key) }
        .let { idx ->
            if (idx < 0) maxOf(-idx - 2, 0)
            else idx
        }
}

/**
 * Checks if the SSTables are valid for concatenation.
 *
 * @param sstables the list of SSTables to check
 * @throws IllegalStateException if any SSTable is invalid
 */
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
