package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.toComparableByteArray

class BlockKey(
    val key: ComparableByteArray,
    private val timestamp: Long = 0
) {
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

    fun computeOverlap(other: BlockKey): Int {
        return key.computeOverlap(other.key)
    }
}

fun String.toBlockKey(): BlockKey {
    return BlockKey(this.toComparableByteArray())
}
