package org.example.common

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
