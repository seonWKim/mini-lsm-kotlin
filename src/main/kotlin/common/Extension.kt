package org.seonWKim.common

fun Int.toU16ByteArray(): List<Byte> {
    val highByte = (this shr 8).toByte()
    val lowByte = this.toByte()
    return listOf(highByte, lowByte)
}

fun Long.toU64ByteArray(): List<Byte> {
    return listOf(
        (this shr 56).toByte(),
        (this shr 48).toByte(),
        (this shr 40).toByte(),
        (this shr 32).toByte(),
        (this shr 24).toByte(),
        (this shr 16).toByte(),
        (this shr 8).toByte(),
        this.toByte()
    )
}

fun List<Byte>.toUInt(): Int {
    val highByte = this[0].toInt() and 0xFF
    val lowByte = this[1].toInt() and 0xFF
    return (highByte shl 8) or lowByte
}

fun List<Byte>.toU64Long(): Long {
    require(this.size == 8) { "List must contain exactly 8 bytes" }
    return (this[0].toLong() and 0xFF shl 56) or
            (this[1].toLong() and 0xFF shl 48) or
            (this[2].toLong() and 0xFF shl 40) or
            (this[3].toLong() and 0xFF shl 32) or
            (this[4].toLong() and 0xFF shl 24) or
            (this[5].toLong() and 0xFF shl 16) or
            (this[6].toLong() and 0xFF shl 8) or
            (this[7].toLong() and 0xFF)
}
