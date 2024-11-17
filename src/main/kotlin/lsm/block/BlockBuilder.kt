package org.example.lsm.block

import org.example.common.ComparableByteArray
import org.example.common.toU16

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
        private const val SIZEOF_U16: Int = Short.SIZE_BYTES // Size of a `Short` which is 2 bytes in JVM
        private const val OFFSET_KEY_VALUE_COUNT = 3
    }

    /**
     * 1. number of key-value pairs in the block
     * 2. offsets
     * 3. key-value pairs
     */
    fun estimatedSize(): Int {
        return SIZEOF_U16 + offset.size * SIZEOF_U16 + data.size
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
        offset.addAll(data.size.toU16())

        // encode key overlap
        val overlap = firstKey?.computeOverlap(key) ?: 0
        data.addAll(overlap.toU16())

        // encode key length
        data.addAll((key.size() - overlap).toU16())

        // encode key content
        data.addAll(key.slice(overlap..<key.size()))

        // encode value length
        data.addAll(value.size().toU16())

        // encode value content
        data.addAll(value.array.toList())

        if (firstKey == null) {
            firstKey = key
        }

        return true
    }

    private fun additionAllowed(key: BlockKey, value: ComparableByteArray): Boolean {
        if (isEmpty()) return true

        val nextEstimatedSize =
            estimatedSize() + key.size() + value.size() + SIZEOF_U16 * OFFSET_KEY_VALUE_COUNT
        println("estimated size: ${nextEstimatedSize}")
        return nextEstimatedSize <= blockSize
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
            offset = offset
        )
    }
}
