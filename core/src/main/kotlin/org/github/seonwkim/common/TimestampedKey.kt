package org.github.seonwkim.common

import org.github.seonwkim.lsm.Configuration.TS_DEFAULT

/**
 * A class representing a key with an associated timestamp.
 *
 * @property bytes the byte array representing the key
 * @property timestamp the timestamp associated with the key
 */
class TimestampedKey(
    bytes: ComparableByteArray,
    private var timestamp: Long = TS_DEFAULT
) : Comparable<TimestampedKey> {
    var bytes: ComparableByteArray = bytes
        private set

    companion object {
        /**
         * Creates an empty `TimestampedKey`.
         *
         * @return a new instance of `TimestampedKey` with an empty byte array
         */
        fun empty(): TimestampedKey {
            return TimestampedKey(bytes = ComparableByteArray.new())
        }
    }

    /**
     * Checks if the key is empty.
     *
     * @return true if the key is empty, false otherwise
     */
    fun isEmpty(): Boolean {
        return bytes.isEmpty()
    }

    /**
     * Returns the size of the key.
     *
     * @return the size of the key
     */
    fun size(): Int {
        return bytes.size()
    }

    /**
     * Returns a slice of the key's byte array.
     *
     * @param range the range of indices to include in the slice
     * @return a new [ComparableByteArray] containing the specified range
     */
    fun slice(range: IntRange): ComparableByteArray {
        return bytes.slice(range)
    }

    /**
     * Returns the timestamp associated with the key.
     *
     * @return the timestamp
     */
    fun timestamp(): Long {
        return timestamp
    }

    /**
     * Sets the timestamp associated with the key.
     *
     * @param timestamp the new timestamp
     */
    fun setTimestamp(timestamp: Long) {
        this.timestamp = timestamp
    }

    /**
     * Computes the overlap between this key and another key.
     *
     * @param other the other key to compare with
     * @return the number of overlapping bytes
     */
    fun computeOverlap(other: TimestampedKey): Int {
        return bytes.computeOverlap(other.bytes)
    }

    /**
     * Clears the key, setting its byte array to an empty array.
     */
    fun clear() {
        bytes = ComparableByteArray.new()
    }

    /**
     * Appends another `TimestampedKey` to this key.
     *
     * @param timestampedKey the key to append
     * @throws IllegalArgumentException if the timestamps of the keys differ
     */
    fun append(timestampedKey: TimestampedKey) {
        if (this.timestamp != timestampedKey.timestamp) {
            throw IllegalArgumentException("timestamp differs ${this.timestamp} != ${timestampedKey.timestamp}")
        }

        bytes = bytes + timestampedKey.bytes
    }

    /**
     * Appends a byte array to this key.
     *
     * @param bytes the byte array to append
     */
    fun append(bytes: ComparableByteArray) {
        this.bytes = this.bytes + bytes
    }

    /**
     * Sets the key's byte array from another `TimestampedKey`.
     *
     * @param timestampedKey the key to copy from
     */
    fun setFromBlockKey(timestampedKey: TimestampedKey) {
        bytes = timestampedKey.bytes
    }

    /**
     * Compares this key to another key.
     *
     * @param other the other key to compare with
     * @return a negative integer, zero, or a positive integer as this key is less than, equal to, or greater than the specified key
     */
    override fun compareTo(other: TimestampedKey): Int {
        val keyComparison = this.bytes.compareTo(other.bytes)
        if (keyComparison != 0) {
            return keyComparison
        }
        return this.timestamp.compareTo(other.timestamp)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TimestampedKey

        return this.compareTo(other) == 0
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + bytes.hashCode()
        return result
    }

    override fun toString(): String {
        return "$bytes@$timestamp"
    }
}

/**
 * Converts a string to a `TimestampedKey`.
 *
 * @return a new `TimestampedKey` containing the bytes of the string
 */
fun String.toTimestampedKey(): TimestampedKey {
    return TimestampedKey(this.toComparableByteArray())
}
