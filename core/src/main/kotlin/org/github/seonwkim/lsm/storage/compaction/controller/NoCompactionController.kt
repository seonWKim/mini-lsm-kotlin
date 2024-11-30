package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

data object NoCompactionController : CompactionController {
    override fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask? {
        throw Error("generateCompactionTask should not be called on NoCompactionController")
    }

    override fun applyCompactionResult(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        throw Error("applyCompactionResult should not be called on NoCompactionController")
    }

    override fun flushToL0(): Boolean {
        return true
    }
}
