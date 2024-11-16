package org.example.common

import java.nio.charset.Charset

class ComparableByteArray(
    val array: ByteArray
) : Comparable<ComparableByteArray> {
    companion object {
        val EMPTY = ComparableByteArray("".toByteArray())
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

    fun size(): Int {
        return array.size
    }

    override fun toString(): String {
        return String(array, Charset.defaultCharset())
    }
}

fun String.toComparableByteArray(): ComparableByteArray {
    return ComparableByteArray(this.toByteArray())
}
