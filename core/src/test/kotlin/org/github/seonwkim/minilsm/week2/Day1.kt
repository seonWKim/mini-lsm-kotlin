package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.iterator.SstConcatIterator
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.Sstable
import org.github.seonwkim.lsm.storage.LsmStorageInner
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.compaction.option.NoCompaction
import org.github.seonwkim.minilsm.week2.Utils.checkIterator
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day1 {

    @Test
    fun `test task1 full compaction`() {
        val dir = createTempDirectory("test_task1_full_compaction")
        val storage = LsmStorageInner.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                compactionOptions = NoCompaction,
                enableWal = false,
                numMemTableLimit = 50,
                serializable = false
            )
        )

        storage.put("0".toComparableByteArray(), "v1".toComparableByteArray())
        sync(storage)

        storage.put("0".toComparableByteArray(), "v2".toComparableByteArray())
        storage.put("1".toComparableByteArray(), "v2".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "v2".toComparableByteArray())
        sync(storage)

        storage.delete("0".toComparableByteArray())
        storage.delete("2".toComparableByteArray())
        sync(storage)

        assertEquals(storage.getL0SstablesSize(), 3)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v1".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                    )
                )
            }
        }

        storage.forceFullCompaction()
        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v1".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                    )
                )
            }
        }

        storage.put("0".toComparableByteArray(), "v3".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "v3".toComparableByteArray())
        sync(storage)
        storage.delete("1".toComparableByteArray())
        sync(storage)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v1".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v3".toComparableByteArray()),
                    )
                )
            }
        }

        storage.forceFullCompaction()
        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("0".toComparableByteArray(), "v1".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("1".toComparableByteArray(), "v2".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toComparableByteArray(), "v3".toComparableByteArray()),
                        Pair("2".toComparableByteArray(), "v3".toComparableByteArray()),
                    )
                )
            }
        }
    }

    @Test
    fun `test task2 concat iterator`() {
        val dir = createTempDirectory("test_task2_concat_iterator")
        val sstables = mutableListOf<Sstable>()
        for (id in 1..10) {
            sstables.add(
                generateConcatSst(
                    startKey = id * 10,
                    endKey = (id + 1) * 10,
                    dir = dir,
                    id = id
                )
            )
        }
        for (key in 0 until 120) {
            val iter = SstConcatIterator.createAndSeekToKey(
                sstables = sstables,
                key = TimestampedKey("%05d".format(key).toComparableByteArray())
            )
            if (key < 10) {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "00010".toComparableByteArray())
            } else if (key >= 110) {
                assertTrue { !iter.isValid() }
            } else {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "%05d".format(key).toComparableByteArray())
            }
        }

        val iter = SstConcatIterator.createAndSeekToFirst(sstables)
        assertTrue { iter.isValid() }
        assertEquals(iter.key(), "00010".toComparableByteArray())
    }

    @Test
    fun `test task3 integration`() {
        val dir = createTempDirectory("test_task3_integration")
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("0".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("00".toComparableByteArray(), "2333333".toComparableByteArray())
        storage.put("4".toComparableByteArray(), "23".toComparableByteArray())
        sync(storage)

        storage.delete("4".toComparableByteArray())
        sync(storage)

        storage.forceFullCompaction()
        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        storage.put("1".toComparableByteArray(), "233".toComparableByteArray())
        storage.put("2".toComparableByteArray(), "2333".toComparableByteArray())
        sync(storage)

        storage.put("00".toComparableByteArray(), "2333".toComparableByteArray())
        storage.put("3".toComparableByteArray(), "23333".toComparableByteArray())
        storage.delete("1".toComparableByteArray())
        sync(storage)
        storage.forceFullCompaction()

        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        checkIterator(
            actual = storage.scan(Unbounded, Unbounded),
            expected = listOf(
                Pair("0".toComparableByteArray(), "2333333".toComparableByteArray()),
                Pair("00".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("2".toComparableByteArray(), "2333".toComparableByteArray()),
                Pair("3".toComparableByteArray(), "23333".toComparableByteArray()),
            )
        )

        assertEquals(storage.get("0".toComparableByteArray()), "2333333".toComparableByteArray())
        assertEquals(storage.get("00".toComparableByteArray()), "2333".toComparableByteArray())
        assertEquals(storage.get("2".toComparableByteArray()), "2333".toComparableByteArray())
        assertEquals(storage.get("3".toComparableByteArray()), "23333".toComparableByteArray())
        assertEquals(storage.get("4".toComparableByteArray()), null)
        assertEquals(storage.get("--".toComparableByteArray()), null)
        assertEquals(storage.get("555".toComparableByteArray()), null)
    }

    private fun sync(storage: LsmStorageInner) {
        storage.forceFreezeMemTable()
        storage.forceFlushNextImmMemTable()
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
            builder.add(TimestampedKey(key.toComparableByteArray()), "test".toComparableByteArray())
        }
        val path = dir.resolve("${id}.sst")
        return builder.buildForTest(path)
    }

    private fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
        blockSize = 4096,
        targetSstSize = 2 shl 20,
        numMemTableLimit = 50,
        enableWal = false,
        compactionOptions = NoCompaction,
        serializable = false
    )
}
