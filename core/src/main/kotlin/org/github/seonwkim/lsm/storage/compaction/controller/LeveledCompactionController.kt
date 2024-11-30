package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.storage.SstLevel
import org.github.seonwkim.lsm.storage.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import java.util.*

class LeveledCompactionController(options: LeveledCompactionOptions) : CompactionController {
    override fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask? {
        TODO()
    }

    override fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        TODO("Not yet implemented")
    }

    override fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    ) {
        TODO("Not yet implemented")
    }

    override fun flushToL0(): Boolean {
        return true
    }
}
