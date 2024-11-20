package org.github.seonwkim.common

// TODO: rename the class appropriately
class TimestampedKey(
    bytes: ComparableByteArray,
    private var timestamp: Long = TS_DEFAULT
) : Comparable<TimestampedKey> {
    var bytes: ComparableByteArray = bytes
        private set

    companion object {
        fun empty(): TimestampedKey {
            return TimestampedKey(bytes = ComparableByteArray.new())
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

    fun computeOverlap(other: TimestampedKey): Int {
        return bytes.computeOverlap(other.bytes)
    }

    fun clear() {
        bytes = ComparableByteArray.new()
    }

    fun append(timestampedKey: TimestampedKey) {
        if (this.timestamp != timestampedKey.timestamp) {
            throw IllegalArgumentException("timestamp differs ${this.timestamp} != ${timestampedKey.timestamp}")
        }

        bytes = bytes + timestampedKey.bytes
    }

    fun append(bytes: ComparableByteArray) {
        this.bytes = this.bytes + bytes
    }

    fun setFromBlockKey(timestampedKey: TimestampedKey) {
        bytes = timestampedKey.bytes
    }

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

fun String.toBlockKey(): TimestampedKey {
    return TimestampedKey(this.toComparableByteArray())
}
