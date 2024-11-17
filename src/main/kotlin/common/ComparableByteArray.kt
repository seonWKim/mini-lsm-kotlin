package org.seonWKim.common

import java.nio.charset.Charset

class ComparableByteArray(
    val array: ByteArray
) : Comparable<ComparableByteArray> {
    companion object {
        val EMPTY = ComparableByteArray(ByteArray(0))
    }

    fun size(): Int {
        return array.size
    }

    fun isEmpty(): Boolean {
        return array.isEmpty()
    }

    fun computeOverlap(other: ComparableByteArray): Int {
        var i = 0
        while (true) {
            if (i >= this.array.size || i >= other.array.size) {
                break
            }

            if (this.array[i] != other.array[i]) {
                break
            }

            i++
        }
        return i
    }

    override fun compareTo(other: ComparableByteArray): Int {
        if (this.array.size > other.array.size) {
            return other.compareTo(this)
        }

        for (idx in array.indices) {
            if (array[idx] == other.array[idx]) continue
            return array[idx] - other.array[idx]
        }

        return if (array.size == other.array.size) 0 else -1
    }

    override fun equals(other: Any?): Boolean {
        return other is ComparableByteArray && array.contentEquals(other.array)
    }

    override fun hashCode(): Int {
        return array.contentHashCode()
    }

    override fun toString(): String {
        return String(array, Charset.defaultCharset())
    }
}

fun String.toComparableByteArray(): ComparableByteArray {
    return ComparableByteArray(this.toByteArray())
}
