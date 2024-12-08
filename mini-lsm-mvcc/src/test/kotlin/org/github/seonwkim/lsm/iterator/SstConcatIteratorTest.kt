package org.github.seonwkim.lsm.iterator

import org.github.seonwkim.common.toTimestampedByteArray
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.Sstable
import org.junit.jupiter.api.Assertions.assertEquals
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class SstConcatIteratorTest {

    @Test
    fun `test find nearest sstable idx key exists`() {
        val dir = createTempDirectory()
        val sstables = listOf(
            generateConcatSst(0, 10, dir, 1),
            generateConcatSst(10, 20, dir, 2),
            generateConcatSst(20, 30, dir, 3)
        )
        val key = "00010".toTimestampedByteArray()
        val idx = findMinimumSstableIdxContainingKey(sstables, key)
        assertEquals(1, idx)
    }

    @Test
    fun `test find nearest sstable idx key between elements`() {
        val dir = createTempDirectory()
        val sstables = listOf(
            generateConcatSst(0, 10, dir, 1),
            generateConcatSst(10, 20, dir, 2),
            generateConcatSst(20, 30, dir, 3)
        )
        val key = "00015".toTimestampedByteArray()
        val idx = findMinimumSstableIdxContainingKey(sstables, key)
        assertEquals(1, idx)
    }

    @Test
    fun `test find nearest sstable idx key smaller than first element`() {
        val dir = createTempDirectory()
        val sstables = listOf(
            generateConcatSst(10, 20, dir, 1),
            generateConcatSst(20, 30, dir, 2),
            generateConcatSst(30, 40, dir, 3)
        )
        val key = "00000".toTimestampedByteArray()
        val idx = findMinimumSstableIdxContainingKey(sstables, key)
        assertEquals(0, idx)
    }

    @Test
    fun `test find nearest sstable idx key larger than last element`() {
        val dir = createTempDirectory()
        val sstables = listOf(
            generateConcatSst(0, 10, dir, 1),
            generateConcatSst(10, 20, dir, 2),
            generateConcatSst(20, 30, dir, 3)
        )
        val key = "00030".toTimestampedByteArray()
        val idx = findMinimumSstableIdxContainingKey(sstables, key)
        assertEquals(2, idx)
    }

    private fun generateConcatSst(
        startKey: Int,
        endKey: Int,
        dir: Path,
        id: Int
    ): Sstable {
        val builder = SsTableBuilder(128)
        for (idx in startKey until endKey) {
            val key = "%05d".format(idx)
            builder.add(key.toTimestampedByteArray(), "test".toTimestampedByteArray())
        }
        val path = dir.resolve("${id}.sst")
        return builder.buildForTest(path)
    }
}
