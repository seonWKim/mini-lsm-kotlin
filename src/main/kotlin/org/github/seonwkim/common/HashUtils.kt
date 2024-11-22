package org.github.seonwkim.common

import com.google.common.hash.Hashing

fun crcHash(buf: ComparableByteArray): Int {
    return crcHash(buf.getByteArray())
}

fun crcHash(buf: ByteArray): Int {
    return Hashing.crc32().hashBytes(buf).asInt()
}

fun murmurHash(buf: ComparableByteArray): Int {
    return Hashing.murmur3_32_fixed().newHasher().putBytes(buf.getByteArray()).hash().asInt()
}
