package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.storage.SstLevel
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import java.util.*

data object NoCompactionController : CompactionController {
    override fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask? {
        throw Error("generateCompactionTask should not be called on NoCompactionController")
    }

    override fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        throw Error("applyCompactionResult should not be called on NoCompactionController")
    }

    override fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    ) {
        // do nothing  
    }

    override fun flushToL0(): Boolean {
        return true
    }
}
