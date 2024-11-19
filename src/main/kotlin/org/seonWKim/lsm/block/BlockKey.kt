package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.toComparableByteArray

class BlockKey(
    bytes: ComparableByteArray,
    private var timestamp: Long = 0
) : Comparable<BlockKey> {
    var bytes: ComparableByteArray = bytes
        private set

    companion object {
        fun empty(): BlockKey {
            return BlockKey(bytes = ComparableByteArray.new())
        }
    }

    fun isEmpty(): Boolean {
        return bytes.isEmpty()
    }

    fun size(): Int {
        return bytes.size()
    }

    fun slice(range: IntRange): ComparableByteArray {
        return bytes.slice(range)
    }

    fun timestamp(): Long {
        return timestamp
    }

    fun setTimestamp(timestamp: Long) {
        this.timestamp = timestamp
    }

    fun computeOverlap(other: BlockKey): Int {
        return bytes.computeOverlap(other.bytes)
    }

    fun clear() {
        bytes = ComparableByteArray.new()
    }

    fun append(blockKey: BlockKey) {
        if (this.timestamp != blockKey.timestamp) {
            throw IllegalArgumentException("timestamp differs ${this.timestamp} != ${blockKey.timestamp}")
        }

        bytes = bytes + blockKey.bytes
    }

    fun append(bytes: ComparableByteArray) {
        this.bytes = this.bytes + bytes
    }

    fun setFromBlockKey(blockKey: BlockKey) {
        bytes = blockKey.bytes
    }

    override fun compareTo(other: BlockKey): Int {
        val keyComparison = this.bytes.compareTo(other.bytes)
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
        result = 31 * result + bytes.hashCode()
        return result
    }
}

fun String.toBlockKey(): BlockKey {
    return BlockKey(this.toComparableByteArray())
}
