package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.option.*
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import org.github.seonwkim.lsm.storage.compaction.task.SimpleLeveledCompactionTask

/**
 * All compaction controllers will run on a single threaded environment, so we shouldn't have to consider concurrency
 * for now. But if we need to introduce concurrency, implementations should be updated.
 */
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

    fun generateCompactionTask(state: LsmStorageState): CompactionTask?

    fun applyCompactionResult(
        state: LsmStorageState,
        task: CompactionTask,
        output: List<Int>,
        inRecovery: Boolean
    ): List<Int>

    fun flushTol0(): Boolean
}
