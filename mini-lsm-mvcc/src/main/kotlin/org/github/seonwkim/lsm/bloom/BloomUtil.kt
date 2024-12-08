package org.github.seonwkim.lsm.bloom

import org.github.seonwkim.common.*
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.pow

/**
 * Utility object for encoding, decoding, and calculating parameters for Bloom filters.
 */
object BloomUtil {
    private val LN_2_POW_2 = ln(2.0).pow(2)

    /**
     * Calculates the number of bits per key for a Bloom filter based on the number of entries and the desired false positive rate.
     *
     * @param numberOfEntries the number of entries expected in the Bloom filter
     * @param falsePositiveRate the desired false positive rate
     * @return the number of bits per key
     */
    fun bloomBitsPerKey(numberOfEntries: Int, falsePositiveRate: Double): Int {
        val entriesDouble = numberOfEntries.toDouble()
        val size = -1.0 * entriesDouble * ln(falsePositiveRate) / LN_2_POW_2
        val locs = ceil((size / entriesDouble))
        return locs.toInt()
    }

    /**
     * Encodes a Bloom filter into a buffer.
     *
     * @param buf the buffer to encode the Bloom filter into
     * @param bloom the Bloom filter to encode
     */
    fun encode(buf: TimestampedByteArray, bloom: Bloom) {
        val offset = buf.size()
        buf.append(bloom.filter)
        buf.append(bloom.hashFunctionsCount.toU8ByteArray())
        val checksum = crcHash(buf.slice(offset..<buf.size()))
        buf.append(checksum.toU32ByteArray())
    }

    /**
     * Decodes a Bloom filter from a buffer.
     *
     * @param buf the buffer containing the encoded Bloom filter
     * @return the decoded Bloom filter
     * @throws Error if the checksum does not match
     */
    fun decode(buf: TimestampedByteArray): Bloom {
        val bufSize = buf.size()
        val checksum = buf.slice(bufSize - 4..<bufSize).toU32Int()
        if (checksum != crcHash(buf.slice(0..<bufSize - 4))) {
            throw Error("Bloom filter checksum doesn't match")
        }

        val hashFunctionsCount = buf[bufSize - 5].toInt()
        val filter = buf.slice(0, bufSize - 5)
        return Bloom.create(
            filter = filter,
            hashFunctionsCount = hashFunctionsCount,
        )
    }
}
