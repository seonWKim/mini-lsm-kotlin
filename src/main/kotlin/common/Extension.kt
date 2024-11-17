package org.example.common

fun Int.toU16(): List<Byte> {
    val highByte = (this shr 8).toByte()
    val lowByte = this.toByte()
    return listOf(highByte, lowByte)
}

fun List<UByte>.toInt(): Int {
    val highByte = this[0].toInt()
    val lowByte = this[1].toInt()
    return (highByte shl 8) or lowByte
}
