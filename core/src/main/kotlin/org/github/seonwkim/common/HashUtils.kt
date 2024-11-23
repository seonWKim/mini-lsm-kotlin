package org.github.seonwkim.common

import com.google.common.hash.Hashing

fun crcHash(key: ComparableByteArray): Int {
    return crcHash(key.getByteArray())
}

fun crcHash(key: ByteArray): Int {
    return Hashing.crc32().hashBytes(key).asInt()
}

fun murmurHash(key: ComparableByteArray): UInt {
    return Hashing.murmur3_32_fixed().newHasher()
        .putBytes(key.getByteArray())
        .hash().asInt().toUInt()
}

fun farmHashFingerPrintU32(key: ComparableByteArray): UInt {
    return farmHashFingerPrintU32(key.getByteArray())
}

fun farmHashFingerPrintU32(key: ByteArray): UInt {
    return Hashing.farmHashFingerprint64().hashBytes(key).asInt().toUInt()
}
