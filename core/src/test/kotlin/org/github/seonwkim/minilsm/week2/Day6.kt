package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import org.github.seonwkim.lsm.compaction.option.CompactionOptions
import org.github.seonwkim.lsm.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.option.TieredCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.dumpFilesInDir
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day6 {
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

    private fun integrationTest(compactionOptions: CompactionOptions) {
        val dir = createTempDirectory()
        val options = LsmStorageOptions(
            blockSize = 4096,
            targetSstSize = 1 shl 20,
            compactionOptions = compactionOptions,
            enableWal = true,
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

        // ensure some SSTs are not flushed
        assertTrue {
            !storage.inner.state.memTable.readValue().isEmpty() ||
                    !storage.inner.state.immutableMemTables.readValue().isEmpty()
        }

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
}
