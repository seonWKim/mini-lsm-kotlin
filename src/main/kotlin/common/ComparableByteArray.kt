package org.seonWKim.common

class ComparableByteArray(
    array: List<Byte>
) : Comparable<ComparableByteArray> {

    var array: MutableList<Byte> = array.toMutableList()
        private set

    companion object {
        fun empty(): ComparableByteArray {
            return ComparableByteArray(ArrayList())
        }
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

    fun append(bytes: List<Byte>) {
        array.addAll(bytes)
    }

    override fun compareTo(other: ComparableByteArray): Int {
        if (this.array.size > other.array.size) {
            return -other.compareTo(this)
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
    return ComparableByteArray(this.map { it.code.toByte() })
}
