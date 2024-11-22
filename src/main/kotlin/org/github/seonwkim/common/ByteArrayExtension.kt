package org.github.seonwkim.common

import kotlin.experimental.and
import kotlin.experimental.or

fun ComparableByteArray.setBit(idx: UInt, setBit: Boolean) {
    val pos = (idx / 8u).toInt()
    val offset = (idx % 8u).toInt()
    if (setBit) {
        this[pos] = this[pos] or ((1 shl offset).toByte())
    } else {
        this[pos] = this[pos] and (1 shl offset).inv().toByte()
    }
}

fun ComparableByteArray.isBitSet(idx: UInt): Boolean {
    val pos = (idx / 8u).toInt()
    val offset = (idx % 8u).toInt()

    return (this[pos] and (1 shl offset).toByte()) != 0.toByte()
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
