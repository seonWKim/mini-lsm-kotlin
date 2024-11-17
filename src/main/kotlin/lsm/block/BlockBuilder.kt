package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.SIZE_OF_U16_IN_BYTE
import org.seonWKim.common.toU16ByteArray
import org.seonWKim.common.toU64ByteArray

class BlockBuilder(
    // expected block size
    private val blockSize: Int,
) {
    // offsets of each key-value entries
    private val offset: MutableList<Byte> = mutableListOf()

    // all serialized key-value pairs in the block
    private val data: MutableList<Byte> = mutableListOf()

    // first key in the block
    private var firstKey: BlockKey? = null

    companion object {
        private const val OFFSET_KEY_VALUE_COUNT = 3
    }

    /**
     * Adds a key-value pair to the block. Returns false when the block is full.
     */
    fun add(key: BlockKey, value: ComparableByteArray): Boolean {
        if (key.isEmpty()) {
            throw IllegalArgumentException("key should not be empty")
        }

        if (!additionAllowed(key, value)) {
            return false
        }

        // add offset of the data into the offset array
        offset.addAll(data.size.toU16ByteArray())

        // encode key overlap
        val overlap = firstKey?.computeOverlap(key) ?: 0
        data.addAll(overlap.toU16ByteArray())

        // encode key length
        data.addAll((key.size() - overlap).toU16ByteArray())

        // encode key content
        data.addAll(key.slice(overlap..<key.size()))

        // encode key ts
        data.addAll(key.timestamp().toU64ByteArray())

        // encode value length
        data.addAll(value.size().toU16ByteArray())

        // encode value content
        data.addAll(value.array)

        if (firstKey == null) {
            firstKey = key
        }

        return true
    }

    private fun additionAllowed(key: BlockKey, value: ComparableByteArray): Boolean {
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
        return SIZE_OF_U16_IN_BYTE + offset.size * SIZE_OF_U16_IN_BYTE + data.size
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
