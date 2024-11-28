package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.option.*
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

sealed interface CompactionController {
    companion object {
        fun createCompactionController(option: CompactionOptions): CompactionController {
            return when (option) {
                is NoCompaction -> NoCompactionController
                is SimpleLeveledCompactionOptions -> SimpleLeveledCompactionController(option)
                is LeveledCompactionOptions -> LeveledCompactionController(option)
                is TieredCompactionOptions -> TieredCompactionController(option)
            }
        }
    }

    fun generateCompactionTask(state: LsmStorageState): CompactionTask

    fun flushTol0(): Boolean
}
