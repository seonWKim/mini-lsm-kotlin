package org.example.common

fun Int.toU16(): List<Byte> {
    val highByte = (this shr 8).toByte()
    val lowByte = this.toByte()
    return listOf(highByte, lowByte)
}
