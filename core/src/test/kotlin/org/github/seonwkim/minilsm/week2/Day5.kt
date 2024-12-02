package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.toU32ByteArray
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import org.github.seonwkim.lsm.compaction.option.CompactionOptions
import org.github.seonwkim.lsm.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.TieredCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.dumpFilesInDir
import org.github.seonwkim.minilsm.week2.Utils.waitUntilCompactionEnds
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day5 {

    @Test
    fun `simple leveled compaction integration test`() {
        integrationTest(
            compactionOptions = SimpleLeveledCompactionOptions(
                sizeRatioPercent = 200,
                level0FileNumCompactionTrigger = 2,
                maxLevels = 3
            )
        )
    }

    @Test
    fun `leveled compaction integration test`() {
        integrationTest(
            compactionOptions = LeveledCompactionOptions(
                levelSizeMultiplier = 2,
                level0FileNumCompactionTrigger = 2,
                maxLevel = 3,
                baseLevelSizeMB = 1
            )
        )
    }

    @Test
    fun `tiered compaction integration test`() {
        integrationTest(
            compactionOptions = TieredCompactionOptions(
                minNumTiers = 3,
                maxSizeAmplificationPercent = 200,
                maxSizeRatio = 1,
                minMergeWidth = 3,
                maxMergeWidth = null
            )
        )
    }

    @Test
    fun `test multiple compacted sst leveled`() {
        val compactionOptions = LeveledCompactionOptions(
            levelSizeMultiplier = 4,
            level0FileNumCompactionTrigger = 2,
            maxLevel = 2,
            baseLevelSizeMB = 2
        )

        val lsmStorageOptions = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 2 shl 20,
            compactionOptions = compactionOptions,
            enableWal = false,
            numMemTableLimit = 2,
            serializable = false
        )

        val dir = createTempDirectory()
        val storage = MiniLsm.open(path = dir, options = lsmStorageOptions)

        // Insert approximately 10MB of data to ensure that at least one compaction is triggered by priority
        // Insert 500 key-value pairs where each pair is 2KB
        for (i in 0 until 500) {
            val (key, value) = keyValueWithTargetSize(i, 20 * 1024)
            storage.put(key, value)
        }

        waitUntilCompactionEnds(storage)

        storage.close()
        assertTrue { storage.inner.state.memTable.readValue().isEmpty() }
        assertTrue { storage.inner.state.immutableMemTables.readValue().isEmpty() }

        storage.dumpStructure()
        dumpFilesInDir(dir)

        MiniLsm.open(
            path = dir,
            options = lsmStorageOptions
        ).let {
            for (i in 0 until 500) {
                val (key, value) = keyValueWithTargetSize(i, 20 * 1024)
                assertEquals(it.get(key), value)
            }
        }
    }

    private fun integrationTest(compactionOptions: CompactionOptions) {
        val dir = createTempDirectory()
        val options = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 1 shl 20,
            compactionOptions = compactionOptions,
            enableWal = false,
            numMemTableLimit = 2,
            serializable = false
        )
        val storage = MiniLsm.open(
            path = dir,
            options = options
        )

        for (i in 0..20) {
            storage.put("0", "v$i")
            if (i % 2 == 0) {
                storage.put("1", "v$i")
            } else {
                storage.delete("1")
            }

            if (i % 2 == 1) {
                storage.put("2", "v$i")
            } else {
                storage.delete("2")
            }

            storage.inner.forceFreezeMemTableWithLock()
        }

        storage.close()

        assertTrue { storage.inner.state.memTable.readValue().entriesSize() == 0 }
        assertTrue { storage.inner.state.immutableMemTables.readValue().isEmpty() }

        storage.dumpStructure()
        dumpFilesInDir(dir)

        MiniLsm.open(
            path = dir,
            options = options
        ).let {
            assertEquals(it.get("0"), "v20")
            assertEquals(it.get("1"), "v20")
            assertEquals(it.get("2"), null)

            Thread.sleep(1000)
        }
    }

    private fun keyValueWithTargetSize(
        seed: Int,
        targetSizeByte: Int
    ): Pair<ComparableByteArray, ComparableByteArray> {
        val key = seed.toU32ByteArray()
        key += ComparableByteArray(List(targetSizeByte - 4) { 0 })

        val value = seed.toU32ByteArray()
        value += ComparableByteArray(List(targetSizeByte - 4) { 0 })

        return Pair(key, value)
    }
}
