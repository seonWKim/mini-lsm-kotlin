package org.seonWKim.common

class ComparableByteArray(
    array: List<Byte>
) : Comparable<ComparableByteArray> {
    var array: MutableList<Byte> = array.toMutableList()
        private set

    companion object {
        fun new(): ComparableByteArray {
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

    fun getByteArray(): ByteArray {
        return array.toByteArray()
    }

    fun slice(range: IntRange): ComparableByteArray {
        return ComparableByteArray(array.slice(range))
    }

    /**
     * Return a slice including [startIdx] and excluding [endIdx]
     */
    fun slice(startIdx: Int, endIdx: Int): ComparableByteArray {
        return slice(startIdx..<endIdx)
    }

    operator fun plus(other: ComparableByteArray): ComparableByteArray {
        return ComparableByteArray(this.array + other.array)
    }

    operator fun plusAssign(other: ComparableByteArray): Unit {
        this.array.addAll(other.array)
    }

    operator fun get(idx: Int): Byte {
        return array[idx]
    }

    operator fun set(idx: Int, value: Byte): Byte {
        return array.set(idx, value)
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
