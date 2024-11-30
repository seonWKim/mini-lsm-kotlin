package org.github.seonwkim.minilsm.week2

import mu.KotlinLogging
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.minilsm.week2.Utils.checkCompactionRatio
import org.github.seonwkim.minilsm.week2.Utils.compactionBench
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class Day2 {

    private val log = KotlinLogging.logger { }

    @Test
    fun `simple leveled compaction integration test`() {
        val dir = createTempDirectory("simple_leveled_compaction_integration_test")
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = SimpleLeveledCompactionOptions(
                    sizeRatioPercent = 200,
                    level0FileNumCompactionTrigger = 2,
                    maxLevels = 3
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
