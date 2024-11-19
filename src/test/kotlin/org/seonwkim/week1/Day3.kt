package org.seonwkim.week1

import org.seonwkim.common.toComparableByteArray
import org.seonwkim.lsm.block.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Day3 {

    @Test
    fun `test block build single key`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("233".toBlockKey(), "233333".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build full`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "11".toComparableByteArray()) }
        assertFalse { builder.add("22".toBlockKey(), "22".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 1`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 2`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "1".toComparableByteArray()) }
        assertFalse { builder.add("11".toBlockKey(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build all`() {
        generateBlock()
    }

    @Test
    fun `test block encode`() {
        val block = generateBlock()
        block.encode()
    }

    @Test
    fun `test block decode`() {
        val block = generateBlock()
        val encoded = block.encode()
        val decoded = Block.decode(encoded)
        assertEquals(block.offsets, decoded.offsets)
        assertEquals(block.data, decoded.data)
    }

    @Test
    fun `test block iterator`() {
        val block = generateBlock()
        val iter = BlockIterator.createAndSeekToFirst(block)
        for (unused in 0..<5) {
            for (i in 0..<100) {
                val key = iter.key().bytes
                val expectedKey = createKey(i).toComparableByteArray()
                assertTrue("expected: $expectedKey, actual: $key") {
                    key.compareTo(createKey(i).toComparableByteArray()) == 0
                }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") {
                    value.compareTo(expectedValue) == 0
                }
                iter.next()
            }
            iter.seekToFirst()
        }
    }

    @Test
    fun `test block seek key`() {
        val block = generateBlock()
        val iter = BlockIterator.createAndSeekToKey(block, createKey(0).toBlockKey())
        for (offset in 1..5) {
            for (i in 0..<100) {
                val key = iter.key().bytes
                val expectedKey = createKey(i).toComparableByteArray()
                assertTrue("expected: $expectedKey, actual: $key") {
                    key.compareTo(createKey(i).toComparableByteArray()) == 0
                }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") {
                    value.compareTo(expectedValue) == 0
                }
                iter.seekToKey(BlockKey("key_%03d".format(i * 5 + offset).toComparableByteArray()))
            }
            iter.seekToKey(BlockKey("k".toComparableByteArray()))
        }
    }

    private fun generateBlock(): Block {
        val builder = BlockBuilder(10000)
        for (idx in 0..<100) {
            val key = createKey(idx).toBlockKey()
            val value = createValue(idx).toComparableByteArray()
            assertTrue { builder.add(key, value) }
        }
        return builder.build()
    }

    private fun createKey(i: Int): String {
        return "key_%03d".format(i * 5)
    }

    private fun createValue(i: Int): String {
        return "value_%010d".format(i)
    }
}
