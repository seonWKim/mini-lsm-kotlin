package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.clamp
import org.github.seonwkim.common.isBitSet
import org.github.seonwkim.common.setBit

/**
 * Bloom filter implementation.
 */
class Bloom(
    // data of filter in bits
    val filter: ComparableByteArray,

    // number of hash functions
    val hashFunctionsCount: Int,

    // Number of bites in filters which is 8 * filter.size
    private val nbits: UInt
) {

    companion object {
        private const val ROTATE_BIT_COUNT = 15

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
