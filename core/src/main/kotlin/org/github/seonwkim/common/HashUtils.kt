package org.github.seonwkim.common

import com.google.common.hash.Hashing

/**
 * Computes the CRC32 hash of the given [ComparableByteArray].
 *
 * @param key the [ComparableByteArray] to hash
 * @return the CRC32 hash as an integer
 */
fun crcHash(key: ComparableByteArray): Int {
    return crcHash(key.getByteArray())
}

/**
 * Computes the CRC32 hash of the given byte array.
 *
 * @param key the byte array to hash
 * @return the CRC32 hash as an integer
 */
fun crcHash(key: ByteArray): Int {
    return Hashing.crc32().hashBytes(key).asInt()
}

/**
 * Computes the FarmHash fingerprint (32-bit) of the given [ComparableByteArray].
 *
 * @param key the [ComparableByteArray] to hash
 * @return the FarmHash fingerprint as an unsigned integer
 */
fun farmHashFingerPrintU32(key: ComparableByteArray): UInt {
    return farmHashFingerPrintU32(key.getByteArray())
}

/**
 * Computes the FarmHash fingerprint (32-bit) of the given byte array.
 *
 * @param key the byte array to hash
 * @return the FarmHash fingerprint as an unsigned integer
 */
fun farmHashFingerPrintU32(key: ByteArray): UInt {
    return Hashing.farmHashFingerprint64().hashBytes(key).asInt().toUInt()
}
