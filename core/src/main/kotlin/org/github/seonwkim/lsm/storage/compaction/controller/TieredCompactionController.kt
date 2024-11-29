package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageStateDiskSnapshot
import org.github.seonwkim.lsm.storage.compaction.option.TieredCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

class TieredCompactionController(options: TieredCompactionOptions) : CompactionController {
    override fun generateCompactionTask(snapshot: LsmStorageStateDiskSnapshot): CompactionTask? {
        TODO()
    }

    override fun applyCompactionResult(
        snapshot: LsmStorageStateDiskSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        TODO("Not yet implemented")
    }

    override fun flushToL0(): Boolean {
        return false
    }
}
