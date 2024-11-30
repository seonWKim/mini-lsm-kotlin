package org.github.seonwkim.lsm.compaction.controller

import org.github.seonwkim.lsm.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.SstLevel
import org.github.seonwkim.lsm.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.task.CompactionTask
import java.util.*

class LeveledCompactionController(
    private val options: LeveledCompactionOptions
) :
    CompactionController {
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
