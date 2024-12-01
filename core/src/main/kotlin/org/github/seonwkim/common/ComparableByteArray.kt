package org.github.seonwkim.common

/**
 * A class representing a byte array that can be compared to other byte arrays.
 *
 * @property array the list of bytes in the array
 */
class ComparableByteArray(
    array: List<Byte>
) : Comparable<ComparableByteArray> {
    var array: MutableList<Byte> = array.toMutableList()
        private set

    companion object {
        /**
         * Creates a new empty ComparableByteArray.
         *
         * @return a new instance of ComparableByteArray
         */
        fun new(): ComparableByteArray {
            return ComparableByteArray(ArrayList())
        }
    }

    /**
     * Returns the size of the byte array.
     *
     * @return the size of the byte array
     */
    fun size(): Int {
        return array.size
    }

    /**
     * Checks if the byte array is empty.
     *
     * @return true if the byte array is empty, false otherwise
     */
    fun isEmpty(): Boolean {
        return array.isEmpty()
    }

    /**
     * Computes the overlap between this byte array and another byte array.
     *
     * @param other the other byte array to compare with
     * @return the number of overlapping bytes
     */
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

    /**
     * Appends another byte array to this byte array.
     *
     * @param bytes the byte array to append
     */
    fun append(bytes: ComparableByteArray) {
        array.addAll(bytes.array)
    }

    /**
     * Appends a list of bytes to this byte array.
     *
     * @param bytes the list of bytes to append
     */
    fun append(bytes: List<Byte>) {
        array.addAll(bytes)
    }

    /**
     * Returns the byte array as a ByteArray.
     *
     * @return the byte array as a ByteArray
     */
    fun getByteArray(): ByteArray {
        return array.toByteArray()
    }

    /**
     * Returns a slice of the byte array.
     *
     * @param range the range of indices to include in the slice
     * @return a new ComparableByteArray containing the specified range
     */
    fun slice(range: IntRange): ComparableByteArray {
        return ComparableByteArray(array.slice(range))
    }

    /**
     * Return a slice including [startIdx] and excluding [endIdx]
     */
    fun slice(startIdx: Int, endIdx: Int): ComparableByteArray {
        return slice(startIdx..<endIdx)
    }

    /**
     * Concatenates this byte array with another byte array.
     *
     * @param other the other byte array to concatenate
     * @return a new ComparableByteArray containing the concatenated result
     */
    operator fun plus(other: ComparableByteArray): ComparableByteArray {
        return ComparableByteArray(this.array + other.array)
    }

    /**
     * Appends another byte array to this byte array.
     *
     * @param other the other byte array to append
     */
    operator fun plusAssign(other: ComparableByteArray) {
        this.array.addAll(other.array)
    }

    /**
     * Returns the byte at the specified index.
     *
     * @param idx the index of the byte to return
     * @return the byte at the specified index
     */
    operator fun get(idx: Int): Byte {
        return array[idx]
    }

    /**
     * Sets the byte at the specified index to the specified value.
     *
     * @param idx the index of the byte to set
     * @param value the value to set the byte to
     * @return the previous value of the byte at the specified index
     */
    operator fun set(idx: Int, value: Byte): Byte {
        return array.set(idx, value)
    }

    /**
     * Compares this byte array to another byte array.
     *
     * @param other the other byte array to compare with
     * @return a negative integer, zero, or a positive integer as this byte array is less than, equal to, or greater than the specified byte array
     */
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


/**
 * Converts a string to a ComparableByteArray.
 *
 * @return a new ComparableByteArray containing the bytes of the string
 */
fun String.toComparableByteArray(): ComparableByteArray {
    return ComparableByteArray(this.map { it.code.toByte() })
}
