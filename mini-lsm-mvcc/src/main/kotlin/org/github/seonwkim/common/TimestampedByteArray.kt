package org.github.seonwkim.common

/**
 * A class representing a byte array with a timestamp.
 * Originally ComparableByteArray in mini-lsm-starter module.
 *
 * @property bytes the list of bytes in the array
 */
class TimestampedByteArray(bytes: List<Byte>, timestamp: Long = 0L) : Comparable<TimestampedByteArray> {
    private var bytes: MutableList<Byte> = bytes.toMutableList()
    var timestamp: Long = timestamp
        private set


    companion object {
        /**
         * Creates a new empty ComparableByteArray.
         *
         * @return a new instance of ComparableByteArray
         */
        fun new(): TimestampedByteArray {
            return TimestampedByteArray(ArrayList())
        }
    }

    /**
     * Returns the size of the byte array.
     *
     * @return the size of the byte array
     */
    fun size(): Int {
        return bytes.size
    }

    /**
     * Checks if the byte array is empty.
     *
     * @return true if the byte array is empty, false otherwise
     */
    fun isEmpty(): Boolean {
        return bytes.isEmpty()
    }

    /**
     * Computes the overlap between this byte array and another byte array.
     *
     * @param other the other byte array to compare with
     * @return the number of overlapping bytes
     */
    fun computeOverlap(other: TimestampedByteArray): Int {
        var i = 0
        while (true) {
            if (i >= this.bytes.size || i >= other.bytes.size) {
                break
            }

            if (this.bytes[i] != other.bytes[i]) {
                break
            }

            i++
        }
        return i
    }

    fun setFromByteArray(bytes: TimestampedByteArray) {
        clear()
        append(bytes.bytes)
    }

    /**
     * Appends another byte array to this byte array.
     *
     * @param bytes the byte array to append
     */
    fun append(bytes: TimestampedByteArray) {
        this.bytes.addAll(bytes.bytes)
    }

    /**
     * Appends a list of bytes to this byte array.
     *
     * @param bytes the list of bytes to append
     */
    fun append(bytes: List<Byte>) {
        this.bytes.addAll(bytes)
    }

    /**
     * Returns the byte array as a ByteArray.
     *
     * @return the byte array as a ByteArray
     */
    fun getByteArray(): ByteArray {
        return bytes.toByteArray()
    }

    /**
     * Return a slice including [startIdx] and excluding [endIdx]
     */
    fun slice(startIdx: Int, endIdx: Int): TimestampedByteArray {
        return slice(startIdx..<endIdx)
    }

    /**
     * Returns a slice of the byte array.
     *
     * @param range the range of indices to include in the slice
     * @return a new ComparableByteArray containing the specified range
     */
    fun slice(range: IntRange): TimestampedByteArray {
        return TimestampedByteArray(bytes.slice(range))
    }

    /**
     * Clears the byte array.
     */
    fun clear() {
        this.bytes.clear()
    }

    /**
     * Creates a copy of this `TimestampedByteArray`.
     *
     * @return a new `TimestampedByteArray` with the same bytes and timestamp
     */
    fun copy(): TimestampedByteArray {
        return TimestampedByteArray(this.bytes.toList(), timestamp)
    }

    /**
     * Returns a copy of the bytes in this `TimestampedByteArray`.
     *
     * @return a list of bytes
     */
    fun copyBytes(): List<Byte> {
        return this.bytes.toList()
    }

    /**
     * Sets the timestamp for this `TimestampedByteArray`.
     *
     * @param timestamp the new timestamp to set
     */
    fun setTimestamp(timestamp: Long) {
        this.timestamp = timestamp
    }

    /**
     * Concatenates this byte array with another byte array.
     *
     * @param other the other byte array to concatenate
     * @return a new ComparableByteArray containing the concatenated result
     */
    operator fun plus(other: TimestampedByteArray): TimestampedByteArray {
        return TimestampedByteArray(this.bytes + other.bytes)
    }

    /**
     * Appends another byte array to this byte array.
     *
     * @param other the other byte array to append
     */
    operator fun plusAssign(other: TimestampedByteArray) {
        this.bytes.addAll(other.bytes)
    }

    /**
     * Returns the byte at the specified index.
     *
     * @param idx the index of the byte to return
     * @return the byte at the specified index
     */
    operator fun get(idx: Int): Byte {
        return bytes[idx]
    }

    /**
     * Sets the byte at the specified index to the specified value.
     *
     * @param idx the index of the byte to set
     * @param value the value to set the byte to
     * @return the previous value of the byte at the specified index
     */
    operator fun set(idx: Int, value: Byte): Byte {
        return bytes.set(idx, value)
    }

    /**
     * Compares this byte array to another byte array.
     *
     * @param other the other byte array to compare with
     * @return a negative integer, zero, or a positive integer as this byte array is less than, equal to, or greater than the specified byte array
     */
    override fun compareTo(other: TimestampedByteArray): Int {
        if (this.bytes.size > other.bytes.size) {
            return -other.compareTo(this)
        }

        for (idx in bytes.indices) {
            if (bytes[idx] == other.bytes[idx]) continue
            return bytes[idx] - other.bytes[idx]
        }

        return if (bytes.size == other.bytes.size) 0 else -1
    }

    override fun equals(other: Any?): Boolean {
        return other is TimestampedByteArray && this.compareTo(other) == 0
    }

    override fun hashCode(): Int {
        return bytes.hashCode()
    }

    override fun toString(): String {
        val sb = StringBuilder(bytes.size)
        for (byte in bytes) {
            sb.append(byte.toInt().toChar())
        }
        return sb.toString()
    }
}


/**
 * Converts a string into [TimestampedByteArray].
 *
 * @return a new [TimestampedByteArray] containing the bytes of the string
 */
fun String.toTimestampedByteArray(timestamp: Long = 0L): TimestampedByteArray {
    return TimestampedByteArray(this.map { it.code.toByte() }, timestamp)
}

/**
 * Converts a ByteArray into  [TimestampedByteArray].
 *
 * @return a new [TimestampedByteArray] containing the bytes of the string
 */
fun ByteArray.toTimestampedByteArray(timestamp: Long = 0L): TimestampedByteArray {
    return TimestampedByteArray(this.toList(), timestamp)
}

fun TimestampedByteArray?.isValid(): Boolean {
    return this != null && !this.isEmpty()
}

fun TimestampedByteArray?.isDeleted(): Boolean {
    return this != null && this.isEmpty()
}
