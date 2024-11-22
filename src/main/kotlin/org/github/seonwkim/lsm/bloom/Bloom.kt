package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.*

import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

/**
 * Bloom filter implementation.
 */
class Bloom private constructor(
    // data of filter in bits
    val filter: ComparableByteArray,

    // number of hash functions
    val hashFunctionsCount: Int,

    // Number of bites in filters which is 8 * filter.size
    private val nbits: UInt
) {

    companion object {
        private val LN_2_POW_2 = ln(2.0).pow(2)
        private const val ROTATE_BIT_COUNT = 15

        /**
         * Get bloom filter bits per key from [numberOfEntries] and [falsePositiveRate].
         */
        fun bloomBitsPerKey(numberOfEntries: Int, falsePositiveRate: Double): Int {
            val entriesDouble = numberOfEntries.toDouble()
            val size = -1.0 * entriesDouble * ln(falsePositiveRate) / LN_2_POW_2
            val locs = ceil((size / entriesDouble))
            return locs.toInt()
        }

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
