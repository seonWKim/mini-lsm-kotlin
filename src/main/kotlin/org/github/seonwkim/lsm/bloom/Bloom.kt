package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.clamp
import org.github.seonwkim.common.rotateLeft

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
        fun fromKeyHashes(keys: List<Int>, bitsPerKey: Int): Bloom {
            val k = (bitsPerKey * 0.69).toInt().clamp(1, 30)
            var nbits = maxOf(keys.size * bitsPerKey, 64)
            val nbytes = (nbits + 7) / 8
            nbits = nbytes * 8

            val filter = ByteArray(nbytes) { 0 }
            for (key in keys) {
                var h = key
                val delta = h.rotateLeft(15)
                repeat(k) {
                    val bitPosition = (h % nbits).toInt()
                    filter[bitPosition] = 1
                    h += delta
                }
            }
            return Bloom(filter.toList(), k)
        }
    }
}
