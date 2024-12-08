package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.clamp
import org.github.seonwkim.common.isBitSet
import org.github.seonwkim.common.setBit

/**
 * Bloom filter implementation.
 *
 * @property filter the data of the filter in bits
 * @property hashFunctionsCount the number of hash functions
 * @property nbits the number of bits in the filter (8 * filter.size)
 */
class Bloom private constructor(
    val filter: ComparableByteArray,
    val hashFunctionsCount: Int,
    private val nbits: UInt
) {

    companion object {
        private const val ROTATE_BIT_COUNT = 15

        fun create(filter: ComparableByteArray, hashFunctionsCount: Int): Bloom {
            return Bloom(
                filter = filter,
                hashFunctionsCount = hashFunctionsCount,
                nbits = filter.size().toUInt() * 8u
            )
        }

        /**
         * Creates a Bloom filter from a list of key hashes.
         *
         * @param keys the list of key hashes
         * @param bitsPerKey the number of bits per key
         * @return a new Bloom filter
         */
        fun fromKeyHashes(keys: List<UInt>, bitsPerKey: Int): Bloom {
            val hashFunctionsCount = (bitsPerKey * 0.69).toInt().clamp(1, 30)
            var nbits = maxOf(keys.size.toUInt() * bitsPerKey.toUInt(), 64u)
            val nbytes = (nbits + 7u) / 8u
            nbits = nbytes * 8u

            val filter = ComparableByteArray(List(nbytes.toInt()) { 0 })
            for (key in keys) {
                var hashKey: UInt = key
                val delta: UInt = hashKey.rotateLeft(ROTATE_BIT_COUNT)
                repeat(hashFunctionsCount) {
                    val bitPosition = hashKey % nbits
                    filter.setBit(bitPosition, true)
                    hashKey += delta
                }
            }

            return Bloom(
                filter = filter,
                hashFunctionsCount = hashFunctionsCount,
                nbits = nbits
            )
        }
    }

    /**
     * Checks if the Bloom filter may contain the given key.
     *
     * @param key the key to check
     * @return true if the Bloom filter may contain the key, false otherwise
     */
    fun mayContain(key: UInt): Boolean {
        var hashKey = key
        val delta = hashKey.rotateLeft(ROTATE_BIT_COUNT)
        repeat(hashFunctionsCount) {
            val bitPosition = hashKey % nbits
            if (!filter.isBitSet(bitPosition)) {
                return false
            }
            hashKey += delta
        }

        return true
    }
}
