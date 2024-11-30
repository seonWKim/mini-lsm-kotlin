package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.TieredCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.compactionBench
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class Day3 {

    @Test
    fun `tiered compaction integration test`() {
        val dir = createTempDirectory("tiered_compaction_integration_test")
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = TieredCompactionOptions(
                    numTiers = 3,
                    maxSizeAmplificationPercent = 200,
                    sizeRatio = 1,
                    minMergeWidth = 2,
                    maxMergeWidth = null
                ),
                enableWal = false,
                numMemTableLimit = 2,
                serializable = false
            )
        )

        compactionBench(storage)
        checkCompactionRatio(storage)
    }
}
