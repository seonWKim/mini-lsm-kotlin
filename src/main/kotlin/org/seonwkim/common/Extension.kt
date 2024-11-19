package org.seonwkim.common

import com.google.common.hash.Hashing
import java.util.zip.CRC32

fun Int.toU16ByteArray(): ComparableByteArray {
    val highByte = (this shr 8).toByte()
    val lowByte = this.toByte()
    return ComparableByteArray(listOf(highByte, lowByte))
}

fun Int.toU32ByteArray(): ComparableByteArray {
    return ComparableByteArray(
        listOf(
            (this shr 24).toByte(),
            (this shr 16).toByte(),
            (this shr 8).toByte(),
            this.toByte()
        )
    )
}

fun Long.toU64ByteArray(): ComparableByteArray {
    return ComparableByteArray(
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

fun ComparableByteArray.toU16Int(): Int {
    require(this.size() == 2) { "List must contain exactly 2 bytes" }
    val highByte = this[0].toInt() and 0xFF
    val lowByte = this[1].toInt() and 0xFF
    return (highByte shl 8) or lowByte
}

fun ComparableByteArray.toU32Int(): Int {
    require(this.size() == 4) { "List must contain exactly 4 bytes" }
    return (this[0].toInt() and 0xFF shl 24) or
            (this[1].toInt() and 0xFF shl 16) or
            (this[2].toInt() and 0xFF shl 8) or
            (this[3].toInt() and 0xFF)
}

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

fun crcHash(buf: ComparableByteArray): Int {
    return crcHash(buf.getByteArray())
}

fun crcHash(buf: ByteArray): Int {
    val crc = CRC32().run {
        this.update(buf)
    }
    return crc.hashCode()
}

fun murmurHash(buf: ComparableByteArray): Int {
    val hasher = Hashing.murmur3_32_fixed().newHasher()
    hasher.putBytes(buf.getByteArray())
    return hasher.hash().asInt()
}
