package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

class BlockBuilder(
    // expected block size
    private val blockSize: Int,
) {
    // offsets of each key-value entries
    private val offset: ComparableByteArray = ComparableByteArray.new()

    // all serialized key-value pairs in the block
    private val data: ComparableByteArray = ComparableByteArray.new()

    // first key in the block
    private var firstKey: TimestampedKey? = null

    companion object {
        private const val OFFSET_KEY_VALUE_COUNT = 3
    }

    /**
     * Adds a key-value pair to the block. Returns false when the block is full.
     */
    fun add(key: TimestampedKey, value: ComparableByteArray): Boolean {
        if (key.isEmpty()) {
            throw IllegalArgumentException("key should not be empty")
        }

        if (!additionAllowed(key, value)) {
            return false
        }

        // add offset of the data into the offset array
        offset += data.size().toU16ByteArray()

        // encode key overlap
        val overlap = firstKey?.computeOverlap(key) ?: 0
        data += overlap.toU16ByteArray()

        // encode key length
        data += (key.size() - overlap).toU16ByteArray()

        // encode key content
        data += key.slice(overlap..<key.size())

        // encode key ts
        data += key.timestamp().toU64ByteArray()

        // encode value length
        data += value.size().toU16ByteArray()

        // encode value content
        data += value

        if (firstKey == null) {
            firstKey = key
        }

        return true
    }

    private fun additionAllowed(key: TimestampedKey, value: ComparableByteArray): Boolean {
        if (isEmpty()) return true

        val nextEstimatedSize =
            estimatedSize() + key.size() + value.size() + SIZE_OF_U16_IN_BYTE * OFFSET_KEY_VALUE_COUNT
        // println("estimated size: ${nextEstimatedSize}")
        return nextEstimatedSize <= blockSize
    }

    /**
     * 1. number of key-value pairs in the block
     * 2. offsets
     * 3. key-value pairs
     */
    private fun estimatedSize(): Int {
        // println("offset: ${offset.size}, data: ${data.size}")
        return SIZE_OF_U16_IN_BYTE + offset.size() * SIZE_OF_U16_IN_BYTE + data.size()
    }

    /**
     * Check if there are no key-value paris in this block
     */
    private fun isEmpty(): Boolean {
        return offset.isEmpty()
    }

    /**
     * Finalize the block
     */
    fun build(): Block {
        if (isEmpty()) {
            throw Error("Block should not be empty")
        }

        return Block(
            data = data,
            offsets = offset
        )
    }
}
