package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.option.LeveledCompactionOptions

class LeveledCompactionController(options: LeveledCompactionOptions) : CompactionController {
    override fun generateCompactionTask(state: LsmStorageState) {
        TODO()
    }

    override fun flushTol0(): Boolean {
        return true
    }
}
