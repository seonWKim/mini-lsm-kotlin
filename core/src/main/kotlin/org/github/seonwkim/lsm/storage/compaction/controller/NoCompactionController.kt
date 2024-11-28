package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

data object NoCompactionController : CompactionController {
    override fun generateCompactionTask(state: LsmStorageState): CompactionTask? {
        throw Error("generateCompactionTask should not be called on NoCompactionController")
    }

    override fun flushTol0(): Boolean {
        return false
    }
}
