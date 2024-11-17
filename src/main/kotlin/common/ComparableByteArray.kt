package org.seonWKim.common

class ComparableByteArray(
    val array: List<Byte>
) : Comparable<ComparableByteArray> {
    companion object {
        val EMPTY = ComparableByteArray(emptyList())
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
        return other is ComparableByteArray && this.compareTo(other) == 0
    }

    override fun hashCode(): Int {
        return array.hashCode()
    }

    override fun toString(): String {
        val sb = StringBuilder(array.size)
        for (byte in array) {
            sb.append(byte.toInt().toChar())
        }
        return sb.toString()
    }
}

fun String.toComparableByteArray(): ComparableByteArray {
    val byteList = this.map { it.code.toByte() }
    return ComparableByteArray(byteList)
}
