package org.github.seonwkim.common

fun Int.clamp(min: Int, max: Int): Int {
    return when {
        this < min -> min
        this > max -> max
        else -> this
    }
}

fun Int.toU8ByteArray(): ComparableByteArray {
    return ComparableByteArray(listOf(this.toByte()))
}

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
