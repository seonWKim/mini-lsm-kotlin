package org.github.seonwkim.lsm.compaction.controller

import org.github.seonwkim.lsm.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.SstLevel
import org.github.seonwkim.lsm.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.compaction.task.CompactionTask
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
