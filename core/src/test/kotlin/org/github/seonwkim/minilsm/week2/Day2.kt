package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.storage.MiniLsm
import org.github.seonwkim.lsm.storage.compaction.Simple
import org.github.seonwkim.lsm.storage.compaction.SimpleLeveledCompactionOptions
import kotlin.io.path.createTempDirectory
import kotlin.test.Test

class Day2 {

    @Test
    fun `test integration`() {
        val dir = createTempDirectory("test_integration")
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = Simple(
                    options = SimpleLeveledCompactionOptions(
                        sizeRatioPercent = 200,
                        level0FileNumCompactionTrigger = 2,
                        maxLevels = 3
                    )
                ),
                enableWal = false,
                numMemTableLimit = 2,
                serializable = false
            )
        )

        // TODO
    }
}
