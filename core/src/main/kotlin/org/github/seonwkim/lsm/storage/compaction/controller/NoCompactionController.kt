package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState

data object NoCompactionController : CompactionController {
    override fun generateCompactionTask(state: LsmStorageState) {
        throw Error("generateCompactionTask should not be called on NoCompactionController")
    }

    override fun flushTol0(): Boolean {
        return false
    }
}
