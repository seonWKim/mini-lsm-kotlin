package org.github.seonwkim.common

import com.google.common.hash.Hashing

fun crcHash(key: ComparableByteArray): Int {
    return crcHash(key.getByteArray())
}

fun crcHash(key: ByteArray): Int {
    return Hashing.crc32().hashBytes(key).asInt()
}

fun murmurHash(key: ComparableByteArray): Int {
    return Hashing.murmur3_32_fixed().newHasher().putBytes(key.getByteArray()).hash().asInt()
}

fun farmHashFingerPrint64(key: ComparableByteArray): Int {
    return farmHashFingerPrint64(key.getByteArray())
}

fun farmHashFingerPrint64(key: ByteArray): Int {
    return Hashing.farmHashFingerprint64().hashBytes(key).hashCode()
}
