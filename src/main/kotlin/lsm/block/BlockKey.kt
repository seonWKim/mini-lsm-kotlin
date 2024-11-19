package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.toComparableByteArray

class BlockKey(
    key: ComparableByteArray,
    private var timestamp: Long = 0
) : Comparable<BlockKey> {
    var key: ComparableByteArray = key
        private set

    companion object {
        fun empty(): BlockKey {
            return BlockKey(key = ComparableByteArray.empty())
        }
    }

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

    fun setFromBlockKey(blockKey: BlockKey) {
        key = blockKey.key
    }

    override fun compareTo(other: BlockKey): Int {
        val keyComparison = this.key.compareTo(other.key)
        if (keyComparison != 0) {
            return keyComparison
        }
        return this.timestamp.compareTo(other.timestamp)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BlockKey

        return this.compareTo(other) == 0
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + key.hashCode()
        return result
    }
}

fun String.toBlockKey(): BlockKey {
    return BlockKey(this.toComparableByteArray())
}
