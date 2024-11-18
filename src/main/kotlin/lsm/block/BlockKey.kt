package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.toComparableByteArray

class BlockKey(
    key: ComparableByteArray,
    private var timestamp: Long = 0
): Comparable<BlockKey> {
    var key: ComparableByteArray = key
        private set

    fun isEmpty(): Boolean {
        return key.isEmpty()
    }

    fun size(): Int {
        return key.size()
    }

    fun slice(range: IntRange): List<Byte> {
        return key.array.slice(range)
    }

    fun timestamp(): Long {
        return timestamp
    }

    fun setTimestamp(timestamp: Long) {
        this.timestamp = timestamp
    }

    fun computeOverlap(other: BlockKey): Int {
        return key.computeOverlap(other.key)
    }

    fun clear() {
        key = ComparableByteArray.empty()
    }

    fun append(bytes: List<Byte>) {
        key.append(bytes)
    }

    override fun compareTo(other: BlockKey): Int {
        return this.key.compareTo(other.key)
    }
}

fun String.toBlockKey(): BlockKey {
    return BlockKey(this.toComparableByteArray())
}
