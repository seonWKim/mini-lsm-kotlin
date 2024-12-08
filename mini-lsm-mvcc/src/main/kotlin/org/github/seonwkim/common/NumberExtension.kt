package org.github.seonwkim.common

/**
 * Clamps this integer to the specified range.
 *
 * @param min the minimum value to clamp to
 * @param max the maximum value to clamp to
 * @return the clamped value
 */
fun Int.clamp(min: Int, max: Int): Int {
    return when {
        this < min -> min
        this > max -> max
        else -> this
    }
}

/**
 * Converts this integer to a [TimestampedByteArray] representing an unsigned 8-bit integer.
 *
 * @return a [TimestampedByteArray] containing the byte representation of this integer
 */
fun Int.toU8ByteArray(): TimestampedByteArray {
    return TimestampedByteArray(listOf(this.toByte()))
}

/**
 * Converts this integer to a [TimestampedByteArray] representing an unsigned 16-bit integer.
 *
 * @return a [TimestampedByteArray] containing the byte representation of this integer
 */
fun Int.toU16ByteArray(): TimestampedByteArray {
    val highByte = (this shr 8).toByte()
    val lowByte = this.toByte()
    return TimestampedByteArray(listOf(highByte, lowByte))
}

/**
 * Converts this integer to a [TimestampedByteArray] representing an unsigned 32-bit integer.
 *
 * @return a [TimestampedByteArray] containing the byte representation of this integer
 */
fun Int.toU32ByteArray(): TimestampedByteArray {
    return TimestampedByteArray(
        listOf(
            (this shr 24).toByte(),
            (this shr 16).toByte(),
            (this shr 8).toByte(),
            this.toByte()
        )
    )
}

/**
 * Converts this long to a [TimestampedByteArray] representing an unsigned 64-bit integer.
 *
 * @return a [TimestampedByteArray] containing the byte representation of this long
 */
fun Long.toU64ByteArray(): TimestampedByteArray {
    return TimestampedByteArray(
        listOf(
            (this shr 56).toByte(),
            (this shr 48).toByte(),
            (this shr 40).toByte(),
            (this shr 32).toByte(),
            (this shr 24).toByte(),
            (this shr 16).toByte(),
            (this shr 8).toByte(),
            this.toByte()
        )
    )
}
