package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.Unbounded
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.common.toTimestampedByteArrayWithoutTs
import org.github.seonwkim.lsm.Configuration
import org.github.seonwkim.lsm.LsmStorageInner
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.compaction.option.NoCompaction
import org.github.seonwkim.lsm.iterator.SstConcatIterator
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.Sstable
import org.github.seonwkim.minilsm.week2.Utils.checkIterator
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day1 {

    @Test
    fun `test task1 full compaction`() {
        val dir = createTempDirectory()
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

        storage.put("0".toTimestampedByteArrayWithoutTs(), "v1".toComparableByteArray())
        sync(storage)

        storage.put("0".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray())
        storage.put("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray())
        sync(storage)

        storage.delete("0".toTimestampedByteArrayWithoutTs())
        storage.delete("2".toTimestampedByteArrayWithoutTs())
        sync(storage)

        assertEquals(storage.getL0SstablesSize(), 3)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v1".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
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
                        Pair("0".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v1".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                    )
                )
            }
        }

        storage.put("0".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray())
        sync(storage)
        storage.delete("1".toTimestampedByteArrayWithoutTs())
        sync(storage)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v1".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
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
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v1".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("1".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v2".toComparableByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                        Pair("2".toTimestampedByteArrayWithoutTs(), "v3".toComparableByteArray()),
                    )
                )
            }
        }
    }

    @Test
    fun `test task2 concat iterator`() {
        val dir = createTempDirectory()
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
                key = "%05d".format(key).toTimestampedByteArrayWithoutTs()
            )
            if (key < 10) {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "00010".toTimestampedByteArrayWithoutTs())
            } else if (key >= 110) {
                assertTrue { !iter.isValid() }
            } else {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "%05d".format(key).toTimestampedByteArrayWithoutTs())
            }
        }

        val iter = SstConcatIterator.createAndSeekToFirst(sstables)
        assertTrue { iter.isValid() }
        assertEquals(iter.key(), "00010".toTimestampedByteArrayWithoutTs())
    }

    @Test
    fun `test task3 integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray())
        storage.put("00".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray())
        storage.put("4".toTimestampedByteArrayWithoutTs(), "23".toComparableByteArray())
        sync(storage)

        storage.delete("4".toTimestampedByteArrayWithoutTs())
        sync(storage)

        storage.forceFullCompaction()
        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        storage.put("1".toTimestampedByteArrayWithoutTs(), "233".toComparableByteArray())
        storage.put("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        sync(storage)

        storage.put("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray())
        storage.put("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray())
        storage.delete("1".toTimestampedByteArrayWithoutTs())
        sync(storage)
        storage.forceFullCompaction()

        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        checkIterator(
            actual = storage.scan(Unbounded, Unbounded),
            expected = listOf(
                Pair("0".toTimestampedByteArrayWithoutTs(), "2333333".toComparableByteArray()),
                Pair("00".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray()),
                Pair("2".toTimestampedByteArrayWithoutTs(), "2333".toComparableByteArray()),
                Pair("3".toTimestampedByteArrayWithoutTs(), "23333".toComparableByteArray()),
            )
        )

        assertEquals(storage.get("0".toTimestampedByteArrayWithoutTs()), "2333333".toComparableByteArray())
        assertEquals(storage.get("00".toTimestampedByteArrayWithoutTs()), "2333".toComparableByteArray())
        assertEquals(storage.get("2".toTimestampedByteArrayWithoutTs()), "2333".toComparableByteArray())
        assertEquals(storage.get("3".toTimestampedByteArrayWithoutTs()), "23333".toComparableByteArray())
        assertEquals(storage.get("4".toTimestampedByteArrayWithoutTs()), null)
        assertEquals(storage.get("--".toTimestampedByteArrayWithoutTs()), null)
        assertEquals(storage.get("555".toTimestampedByteArrayWithoutTs()), null)
    }

    private fun sync(storage: LsmStorageInner) {
        storage.forceFreezeMemTableWithLock()
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
            builder.add(key.toTimestampedByteArrayWithoutTs(), "test".toComparableByteArray())
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
