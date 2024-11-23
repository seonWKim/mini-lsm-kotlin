package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.*
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

object BloomUtil {
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

    fun encode(buf: ComparableByteArray, bloom: Bloom) {
        val offset = buf.size()
        buf.append(bloom.filter)
        buf.append(bloom.hashFunctionsCount.toU8ByteArray())
        val checksum = crcHash(buf.slice(offset..<buf.size()))
        buf.append(checksum.toU32ByteArray())
    }

    fun decode(buf: ComparableByteArray): Bloom {
        val bufSize = buf.size()
        val checksum = buf.slice(bufSize - 4..<bufSize).toU32Int()
        if (checksum != crcHash(buf.slice(0..<bufSize - 4))) {
            throw Error("Bloom filter checksum doesn't match")
        }

        val hashFunctionsCount = buf[bufSize - 5].toInt()
        val filter = buf.slice(0, bufSize - 5)
        return Bloom(
            filter = filter,
            hashFunctionsCount = hashFunctionsCount,
            nbits = filter.size().toUInt() * 8u
        )
    }
}
