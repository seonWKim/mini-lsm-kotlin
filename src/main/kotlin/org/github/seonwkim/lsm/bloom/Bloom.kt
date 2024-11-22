package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.clamp

import org.github.seonwkim.common.setBit
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

/**
 * Bloom filter implementation.
 */
class Bloom private constructor(
    // data of filter in bits
    val filter: List<Byte>,

    // number of hash functions
    val k: Int
) {

    companion object {
        private val LN_2_POW_2 = ln(2.0).pow(2)

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
            val k = (bitsPerKey * 0.69).toInt().clamp(1, 30)
            var nbits = maxOf(keys.size.toUInt() * bitsPerKey.toUInt(), 64u)
            val nbytes = (nbits + 7u) / 8u
            nbits = nbytes * 8u

            val filter = ByteArray(nbytes.toInt()) { 0 }
            for (key in keys) {
                var h = key
                val delta = h.rotateLeft(15)
                repeat(k) {
                    val bitPosition = h % nbits
                    filter.setBit(bitPosition, true)
                    h += delta
                }
            }

            return Bloom(filter.toList(), k)
        }
    }
}
