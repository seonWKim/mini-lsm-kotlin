package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.common.toTimestampedByteArrayWithoutTs
import org.github.seonwkim.lsm.block.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Day3 {

    @Test
    fun `test block build single key`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("233".toTimestampedByteArrayWithoutTs(), "233333".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build full`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toTimestampedByteArrayWithoutTs(), "11".toComparableByteArray()) }
        assertFalse { builder.add("22".toTimestampedByteArrayWithoutTs(), "22".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 1`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toTimestampedByteArrayWithoutTs(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 2`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toTimestampedByteArrayWithoutTs(), "1".toComparableByteArray()) }
        assertFalse { builder.add("11".toTimestampedByteArrayWithoutTs(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build all`() {
        generateBlock()
    }

    @Test
    fun `test block encode`() {
        val block = generateBlock()
        BlockUtil.encode(block)
    }

    @Test
    fun `test block decode`() {
        val block = generateBlock()
        val encoded = BlockUtil.encode(block)
        val decoded = BlockUtil.decode(encoded)
        assertEquals(block.offsets, decoded.offsets)
        assertEquals(block.data, decoded.data)
    }

    @Test
    fun `test block iterator`() {
        val block = generateBlock()
        val iter = BlockIterator.createAndSeekToFirst(block)
        for (unused in 0..<5) {
            for (i in 0..<100) {
                val key = iter.key()
                val expectedKey = createKey(i).toTimestampedByteArrayWithoutTs()
                assertTrue("expected: $expectedKey, actual: $key") {
                    key.compareTo(expectedKey) == 0
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
        val iter = BlockIterator.createAndSeekToKey(block, createKey(0).toTimestampedByteArrayWithoutTs())
        for (offset in 1..5) {
            for (i in 0..<100) {
                val key = iter.key()
                val expectedKey = createKey(i).toTimestampedByteArrayWithoutTs()
                assertTrue("expected: $expectedKey, actual: $key") {
                    key.compareTo(expectedKey) == 0
                }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") {
                    value.compareTo(expectedValue) == 0
                }
                iter.seekToKey("key_%03d".format(i * 5 + offset).toTimestampedByteArrayWithoutTs())
            }
            iter.seekToKey("k".toTimestampedByteArrayWithoutTs())
        }
    }

    private fun generateBlock(): Block {
        val builder = BlockBuilder(10000)
        for (idx in 0..<100) {
            val key = createKey(idx).toTimestampedByteArrayWithoutTs()
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
