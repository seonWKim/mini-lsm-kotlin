package org.github.seonwkim.common

import kotlin.experimental.and
import kotlin.experimental.or

/**
 * Sets or clears a specific bit in the byte array.
 *
 * @param bitIds the index of the bit to be set or cleared
 * @param setBit if true, the bit is set; if false, the bit is cleared
 * @throws IndexOutOfBoundsException if the index is out of range
 */
fun TimestampedByteArray.setBit(bitIds: UInt, setBit: Boolean) {
    val pos = (bitIds / 8u).toInt()
    val offset = (bitIds % 8u).toInt()
    if (setBit) {
        this[pos] = this[pos] or ((1 shl offset).toByte())
    } else {
        this[pos] = this[pos] and (1 shl offset).inv().toByte()
    }
}

/**
 * Checks if a specific bit is set in the byte array.
 *
 * @param bitIdx the index of the bit to be checked
 * @return true if the bit is set, false otherwise
 * @throws IndexOutOfBoundsException if the index is out of range
 */
fun TimestampedByteArray.isBitSet(bitIdx: UInt): Boolean {
    val pos = (bitIdx / 8u).toInt()
    val offset = (bitIdx % 8u).toInt()

    return (this[pos] and (1 shl offset).toByte()) != 0.toByte()
}

/**
 * Converts a 2-byte array to an unsigned 16-bit integer.
 *
 * @return the unsigned 16-bit integer value
 * @throws IllegalArgumentException if the byte array does not contain exactly 2 bytes
 */
fun TimestampedByteArray.toU16Int(): Int {
    require(this.size() == 2) { "List must contain exactly 2 bytes" }
    val highByte = this[0].toInt() and 0xFF
    val lowByte = this[1].toInt() and 0xFF
    return (highByte shl 8) or lowByte
}

/**
 * Converts a 4-byte array to an unsigned 32-bit integer.
 *
 * @return the unsigned 32-bit integer value
 * @throws IllegalArgumentException if the byte array does not contain exactly 4 bytes
 */
fun TimestampedByteArray.toU32Int(): Int {
    require(this.size() == 4) { "List must contain exactly 4 bytes" }
    return (this[0].toInt() and 0xFF shl 24) or
            (this[1].toInt() and 0xFF shl 16) or
            (this[2].toInt() and 0xFF shl 8) or
            (this[3].toInt() and 0xFF)
}

/**
 * Converts an 8-byte array to an unsigned 64-bit long.
 *
 * @return the unsigned 64-bit long value
 * @throws IllegalArgumentException if the byte array does not contain exactly 8 bytes
 */
fun TimestampedByteArray.toU64Long(): Long {
    require(this.size() == 8) { "List must contain exactly 8 bytes" }
    return (this[0].toLong() and 0xFF shl 56) or
            (this[1].toLong() and 0xFF shl 48) or
            (this[2].toLong() and 0xFF shl 40) or
            (this[3].toLong() and 0xFF shl 32) or
            (this[4].toLong() and 0xFF shl 24) or
            (this[5].toLong() and 0xFF shl 16) or
            (this[6].toLong() and 0xFF shl 8) or
            (this[7].toLong() and 0xFF)
}

/**
 * Sets or clears a specific bit in the byte array.
 *
 * @param bitIds the index of the bit to be set or cleared
 * @param setBit if true, the bit is set; if false, the bit is cleared
 * @throws IndexOutOfBoundsException if the index is out of range
 */
fun ComparableByteArray.setBit(bitIds: UInt, setBit: Boolean) {
    val pos = (bitIds / 8u).toInt()
    val offset = (bitIds % 8u).toInt()
    if (setBit) {
        this[pos] = this[pos] or ((1 shl offset).toByte())
    } else {
        this[pos] = this[pos] and (1 shl offset).inv().toByte()
    }
}

/**
 * Checks if a specific bit is set in the byte array.
 *
 * @param bitIdx the index of the bit to be checked
 * @return true if the bit is set, false otherwise
 * @throws IndexOutOfBoundsException if the index is out of range
 */
fun ComparableByteArray.isBitSet(bitIdx: UInt): Boolean {
    val pos = (bitIdx / 8u).toInt()
    val offset = (bitIdx % 8u).toInt()

    return (this[pos] and (1 shl offset).toByte()) != 0.toByte()
}

/**
 * Converts a 2-byte array to an unsigned 16-bit integer.
 *
 * @return the unsigned 16-bit integer value
 * @throws IllegalArgumentException if the byte array does not contain exactly 2 bytes
 */
fun ComparableByteArray.toU16Int(): Int {
    require(this.size() == 2) { "List must contain exactly 2 bytes" }
    val highByte = this[0].toInt() and 0xFF
    val lowByte = this[1].toInt() and 0xFF
    return (highByte shl 8) or lowByte
}

/**
 * Converts a 4-byte array to an unsigned 32-bit integer.
 *
 * @return the unsigned 32-bit integer value
 * @throws IllegalArgumentException if the byte array does not contain exactly 4 bytes
 */
fun ComparableByteArray.toU32Int(): Int {
    require(this.size() == 4) { "List must contain exactly 4 bytes" }
    return (this[0].toInt() and 0xFF shl 24) or
            (this[1].toInt() and 0xFF shl 16) or
            (this[2].toInt() and 0xFF shl 8) or
            (this[3].toInt() and 0xFF)
}

/**
 * Converts an 8-byte array to an unsigned 64-bit long.
 *
 * @return the unsigned 64-bit long value
 * @throws IllegalArgumentException if the byte array does not contain exactly 8 bytes
 */
fun ComparableByteArray.toU64Long(): Long {
    require(this.size() == 8) { "List must contain exactly 8 bytes" }
    return (this[0].toLong() and 0xFF shl 56) or
            (this[1].toLong() and 0xFF shl 48) or
            (this[2].toLong() and 0xFF shl 40) or
            (this[3].toLong() and 0xFF shl 32) or
            (this[4].toLong() and 0xFF shl 24) or
            (this[5].toLong() and 0xFF shl 16) or
            (this[6].toLong() and 0xFF shl 8) or
            (this[7].toLong() and 0xFF)
}
