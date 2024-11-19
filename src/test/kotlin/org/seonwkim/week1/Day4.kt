package org.seonwkim.week1

import org.junit.jupiter.api.Test
import org.seonwkim.common.toComparableByteArray
import org.seonwkim.lsm.block.BlockKey
import org.seonwkim.lsm.block.toBlockKey
import org.seonwkim.lsm.iterator.SsTableIterator
import org.seonwkim.lsm.sstable.SsTable
import org.seonwkim.lsm.sstable.SsTableBuilder
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day4 {

    companion object {
        private val NUMBER_OF_KEYS = 100
    }

    @Test
    fun `test sst build single key`() {
        val builder = SsTableBuilder(16)
        builder.add("233".toBlockKey(), "233333".toComparableByteArray())
        val dir = createTempDirectory("test_Sst_build_single_key")
        builder.buildForTest(dir.resolve("1.sst"))
    }

    @Test
    fun `test sst build two blocks`() {
        val builder = SsTableBuilder(16)
        builder.add("11".toBlockKey(), "11".toComparableByteArray())
        builder.add("22".toBlockKey(), "22".toComparableByteArray())
        builder.add("33".toBlockKey(), "11".toComparableByteArray())
        builder.add("44".toBlockKey(), "22".toComparableByteArray())
        builder.add("55".toBlockKey(), "11".toComparableByteArray())
        builder.add("66".toBlockKey(), "22".toComparableByteArray())
        assertTrue { builder.meta.size >= 2 }
        val dir = createTempDirectory("test_set_build_two_blocks")
        builder.buildForTest(dir.resolve("1.sst"))
    }

    @Test
    fun `test sst build all`() {
        val (_, sst) = generateSst()
        assertBlockKeyEqualsContent(sst.firstKey, BlockKey(createKey(0).toComparableByteArray()))
        assertBlockKeyEqualsContent(
            sst.lastKey,
            BlockKey(createKey(NUMBER_OF_KEYS - 1).toComparableByteArray())
        )
    }

    @Test
    fun `test sst decode`() {
        val (_, sst) = generateSst()
        val meta = sst.blockMeta
        val newSst = SsTable.openForTest(sst.file)
        assertEquals(newSst.blockMeta, meta)
        assertBlockKeyEqualsContent(newSst.firstKey, BlockKey(createKey(0).toComparableByteArray()))
        assertBlockKeyEqualsContent(
            newSst.lastKey,
            BlockKey(createKey(NUMBER_OF_KEYS - 1).toComparableByteArray())
        )
    }

    @Test
    fun `test sst iterator`() {
        val (_, sst) = generateSst()
        val iter = SsTableIterator.createAndSeekToFirst(sst)
        for (unused in 0 until 5) {
            for (i in 0 until NUMBER_OF_KEYS) {
                val key = iter.key()
                val expectedKey = createKey(i).toComparableByteArray()
                assertTrue("expected: $expectedKey, actual: $key") { key.compareTo(expectedKey) == 0 }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") { value.compareTo(expectedValue) == 0 }
                iter.next()
            }

            iter.seekToFirst()
        }
    }

    @Test
    fun `test sst seek key`() {
        val (_, sst) = generateSst()
        val iter = SsTableIterator.createAndSeekToKey(sst, createKey(0).toBlockKey())
        for (offset in 1..5) {
            for (i in 0 until NUMBER_OF_KEYS) {
                val key = iter.key()
                val expectedKey = createKey(i).toComparableByteArray()
                assertTrue("expected: $expectedKey, actual: $key") { key.compareTo(expectedKey) == 0 }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") { value.compareTo(expectedValue) == 0 }

                iter.seekToKey("key_%03d".format(i * 5 + offset).toBlockKey())
            }

            iter.seekToKey("k".toBlockKey())
        }
    }

    private fun generateSst(): Pair<Path, SsTable> {
        val builder = SsTableBuilder(128)
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            val value = createValue(idx)
            builder.add(BlockKey(key.toComparableByteArray()), value.toComparableByteArray())
        }
        val dir = createTempDirectory("temp")
        val path = dir.resolve("1.sst")
        return Pair(dir, builder.buildForTest(path))
    }

    private fun createKey(i: Int): String {
        return "key_%03d".format(i * 5)
    }

    private fun createValue(i: Int): String {
        return "value_%010d".format(i)
    }

    private fun assertBlockKeyEqualsContent(key1: BlockKey, key2: BlockKey): Boolean {
        return key1.bytes.array == key2.bytes.array
    }
}
