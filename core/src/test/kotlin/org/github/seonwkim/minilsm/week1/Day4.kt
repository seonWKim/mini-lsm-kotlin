package org.github.seonwkim.minilsm.week1

import org.junit.jupiter.api.Test
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.common.toTimestampedKey
import org.github.seonwkim.lsm.iterator.SsTableIterator
import org.github.seonwkim.lsm.sstable.Sstable
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day4 {

    companion object {
        private const val NUMBER_OF_KEYS = 100
    }

    @Test
    fun `test sst build single key`() {
        val builder = SsTableBuilder(16)
        builder.add("233".toTimestampedKey(), "233333".toComparableByteArray())
        val dir = createTempDirectory()
        builder.buildForTest(dir.resolve("1.sst"))
    }

    @Test
    fun `test sst build two blocks`() {
        val builder = SsTableBuilder(16)
        builder.add("11".toTimestampedKey(), "11".toComparableByteArray())
        builder.add("22".toTimestampedKey(), "22".toComparableByteArray())
        builder.add("33".toTimestampedKey(), "11".toComparableByteArray())
        builder.add("44".toTimestampedKey(), "22".toComparableByteArray())
        builder.add("55".toTimestampedKey(), "11".toComparableByteArray())
        builder.add("66".toTimestampedKey(), "22".toComparableByteArray())
        assertTrue { builder.meta.size >= 2 }
        val dir = createTempDirectory()
        builder.buildForTest(dir.resolve("1.sst"))
    }

    @Test
    fun `test sst build all`() {
        val (_, sst) = generateSst()
        assertBlockKeyEqualsContent(sst.firstKey, TimestampedKey(createKey(0).toComparableByteArray()))
        assertBlockKeyEqualsContent(
            sst.lastKey,
            TimestampedKey(createKey(NUMBER_OF_KEYS - 1).toComparableByteArray())
        )
    }

    @Test
    fun `test sst decode`() {
        val (_, sst) = generateSst()
        val meta = sst.blockMeta
        val newSst = Sstable.openForTest(sst.file)
        assertEquals(newSst.blockMeta, meta)
        assertBlockKeyEqualsContent(newSst.firstKey, TimestampedKey(createKey(0).toComparableByteArray()))
        assertBlockKeyEqualsContent(
            newSst.lastKey,
            TimestampedKey(createKey(NUMBER_OF_KEYS - 1).toComparableByteArray())
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
        val iter = SsTableIterator.createAndSeekToKey(sst, createKey(0).toTimestampedKey())
        for (offset in 1..5) {
            for (i in 0 until NUMBER_OF_KEYS) {
                val key = iter.key()
                val expectedKey = createKey(i).toComparableByteArray()
                assertTrue("expected: $expectedKey, actual: $key") { key.compareTo(expectedKey) == 0 }

                val value = iter.value()
                val expectedValue = createValue(i).toComparableByteArray()
                assertTrue("expected: $expectedValue, actual: $value") { value.compareTo(expectedValue) == 0 }

                iter.seekToKey("key_%03d".format(i * 5 + offset).toTimestampedKey())
            }

            iter.seekToKey("k".toTimestampedKey())
        }
    }

    private fun generateSst(): Pair<Path, Sstable> {
        val builder = SsTableBuilder(128)
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            val value = createValue(idx)
            builder.add(TimestampedKey(key.toComparableByteArray()), value.toComparableByteArray())
        }
        val dir = createTempDirectory()
        val path = dir.resolve("1.sst")
        return Pair(dir, builder.buildForTest(path))
    }

    private fun createKey(i: Int): String {
        return "key_%03d".format(i * 5)
    }

    private fun createValue(i: Int): String {
        return "value_%010d".format(i)
    }

    private fun assertBlockKeyEqualsContent(key1: TimestampedKey, key2: TimestampedKey): Boolean {
        return key1.bytes.array == key2.bytes.array
    }
}
