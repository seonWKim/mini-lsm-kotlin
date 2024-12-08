package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.*

/**
 * A builder class for constructing blocks of key-value pairs.
 *
 * @property blockSize the expected size of the block
 */
class BlockBuilder(
    private val blockSize: Int,
) {
    // Offsets of each key-value entry
    private val offset: ComparableByteArray = ComparableByteArray.new()

    // All serialized key-value pairs in the block
    private val data: ComparableByteArray = ComparableByteArray.new()

    // First key in the block
    private var firstKey: ComparableByteArray? = null

    companion object {
        private const val OFFSET_KEY_VALUE_COUNT = 3
    }

    /**
     * Adds a key-value pair to the block.
     *
     * @param key the key to add
     * @param value the value to add
     * @return false if the block is full, true otherwise
     * @throws IllegalArgumentException if the key is empty
     */
    fun add(key: ComparableByteArray, value: ComparableByteArray): Boolean {
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
        // data += key.timestamp().toU64ByteArray()

        // encode value length
        data += value.size().toU16ByteArray()

        // encode value content
        data += value

        if (firstKey == null) {
            firstKey = key.copy()
        }

        return true
    }

    /**
     * Checks if adding a key-value pair is allowed based on the estimated size.
     *
     * @param key the key to add
     * @param value the value to add
     * @return true if the addition is allowed, false otherwise
     */
    private fun additionAllowed(key: ComparableByteArray, value: ComparableByteArray): Boolean {
        if (isEmpty()) return true

        val nextEstimatedSize =
            estimatedSize() + key.size() + value.size() + SIZE_OF_U16_IN_BYTE * OFFSET_KEY_VALUE_COUNT
        return nextEstimatedSize <= blockSize
    }

    /**
     * Estimates the size of the block. Sum of following:
     *
     * 1. number of key-value pairs in the block
     * 2. offsets
     * 3. key-value pairs
     */
    private fun estimatedSize(): Int {
        return SIZE_OF_U16_IN_BYTE + offset.size() * SIZE_OF_U16_IN_BYTE + data.size()
    }

    /**
     * Checks if there are no key-value pairs in this block.
     *
     * @return true if the block is empty, false otherwise
     */
    private fun isEmpty(): Boolean {
        return offset.isEmpty()
    }

    /**
     * Finalizes the block and returns it.
     *
     * @return the constructed block
     * @throws Error if the block is empty
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
