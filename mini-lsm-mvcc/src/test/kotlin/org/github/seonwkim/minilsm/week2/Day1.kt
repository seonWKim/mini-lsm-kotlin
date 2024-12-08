package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.Unbounded
import org.github.seonwkim.common.toTimestampedByteArray
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

        storage.put("0".toTimestampedByteArray(), "v1".toTimestampedByteArray())
        sync(storage)

        storage.put("0".toTimestampedByteArray(), "v2".toTimestampedByteArray())
        storage.put("1".toTimestampedByteArray(), "v2".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "v2".toTimestampedByteArray())
        sync(storage)

        storage.delete("0".toTimestampedByteArray())
        storage.delete("2".toTimestampedByteArray())
        sync(storage)

        assertEquals(storage.getL0SstablesSize(), 3)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v1".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "".toTimestampedByteArray()),
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
                        Pair("0".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v1".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                    )
                )
            }
        }

        storage.put("0".toTimestampedByteArray(), "v3".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "v3".toTimestampedByteArray())
        sync(storage)
        storage.delete("1".toTimestampedByteArray())
        sync(storage)
        storage.constructMergeIterator().let { iter ->
            if (Configuration.TS_ENABLED) {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v1".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
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
                        Pair("0".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("0".toTimestampedByteArray(), "v1".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("1".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v2".toTimestampedByteArray()),
                    )
                )
            } else {
                checkIterator(
                    actual = iter,
                    expected = listOf(
                        Pair("0".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
                        Pair("2".toTimestampedByteArray(), "v3".toTimestampedByteArray()),
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
                key = "%05d".format(key).toTimestampedByteArray()
            )
            if (key < 10) {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "00010".toTimestampedByteArray())
            } else if (key >= 110) {
                assertTrue { !iter.isValid() }
            } else {
                assertTrue { iter.isValid() }
                assertEquals(iter.key(), "%05d".format(key).toTimestampedByteArray())
            }
        }

        val iter = SstConcatIterator.createAndSeekToFirst(sstables)
        assertTrue { iter.isValid() }
        assertEquals(iter.key(), "00010".toTimestampedByteArray())
    }

    @Test
    fun `test task3 integration`() {
        val dir = createTempDirectory()
        val storage = LsmStorageInner.open(
            path = dir,
            options = lsmStorageOptionForTest()
        )
        storage.put("0".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("00".toTimestampedByteArray(), "2333333".toTimestampedByteArray())
        storage.put("4".toTimestampedByteArray(), "23".toTimestampedByteArray())
        sync(storage)

        storage.delete("4".toTimestampedByteArray())
        sync(storage)

        storage.forceFullCompaction()
        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        storage.put("1".toTimestampedByteArray(), "233".toTimestampedByteArray())
        storage.put("2".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        sync(storage)

        storage.put("00".toTimestampedByteArray(), "2333".toTimestampedByteArray())
        storage.put("3".toTimestampedByteArray(), "23333".toTimestampedByteArray())
        storage.delete("1".toTimestampedByteArray())
        sync(storage)
        storage.forceFullCompaction()

        assertTrue { storage.state.l0Sstables.readValue().isEmpty() }
        assertTrue { storage.state.levels.readValue()[0].sstIds.isNotEmpty() }

        checkIterator(
            actual = storage.scan(Unbounded, Unbounded),
            expected = listOf(
                Pair("0".toTimestampedByteArray(), "2333333".toTimestampedByteArray()),
                Pair("00".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
                Pair("2".toTimestampedByteArray(), "2333".toTimestampedByteArray()),
                Pair("3".toTimestampedByteArray(), "23333".toTimestampedByteArray()),
            )
        )

        assertEquals(storage.get("0".toTimestampedByteArray()), "2333333".toTimestampedByteArray())
        assertEquals(storage.get("00".toTimestampedByteArray()), "2333".toTimestampedByteArray())
        assertEquals(storage.get("2".toTimestampedByteArray()), "2333".toTimestampedByteArray())
        assertEquals(storage.get("3".toTimestampedByteArray()), "23333".toTimestampedByteArray())
        assertEquals(storage.get("4".toTimestampedByteArray()), null)
        assertEquals(storage.get("--".toTimestampedByteArray()), null)
        assertEquals(storage.get("555".toTimestampedByteArray()), null)
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
            builder.add(key.toTimestampedByteArray(), "test".toTimestampedByteArray())
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
