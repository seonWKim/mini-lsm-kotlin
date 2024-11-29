package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageStateDiskSnapshot
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

data object NoCompactionController : CompactionController {
    override fun generateCompactionTask(snapshot: LsmStorageStateDiskSnapshot): CompactionTask? {
        throw Error("generateCompactionTask should not be called on NoCompactionController")
    }

    override fun applyCompactionResult(
        snapshot: LsmStorageStateDiskSnapshot,
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
