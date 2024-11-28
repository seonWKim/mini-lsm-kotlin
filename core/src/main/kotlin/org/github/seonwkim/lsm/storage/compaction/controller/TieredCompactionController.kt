package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.option.TieredCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

class TieredCompactionController(options: TieredCompactionOptions) : CompactionController {
    override fun generateCompactionTask(state: LsmStorageState): CompactionTask? {
        TODO()
    }

    override fun flushTol0(): Boolean {
        return true
    }
}
