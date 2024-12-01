package org.github.seonwkim.minilsm.week2

import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import org.github.seonwkim.lsm.compaction.option.CompactionOptions
import kotlin.io.path.createTempDirectory

class Day5 {


    fun integrationTest(dirName: String, compactionOptions: CompactionOptions) {
        val dir = createTempDirectory(dirName)
        val storage = MiniLsm.open(
            path = dir,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 1 shl 20,
                compactionOptions = compactionOptions,
                enableWal = false,
                numMemTableLimit = 2,
                serializable = false
            )
        )

    }
}
