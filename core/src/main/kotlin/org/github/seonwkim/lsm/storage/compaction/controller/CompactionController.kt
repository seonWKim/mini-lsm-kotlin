package org.github.seonwkim.lsm.storage.compaction.controller

import org.github.seonwkim.lsm.storage.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.storage.SstLevel
import org.github.seonwkim.lsm.storage.compaction.option.*
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import java.util.*

/**
 * Interface for compaction controllers in the LSM storage system.
 *
 * All compaction controllers will run in a single-threaded environment, so concurrency considerations are not necessary
 * for now. If concurrency is introduced in the future, implementations should be updated accordingly.
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

    /**
     * Generates a compaction task based on the provided snapshot.
     *
     * This method analyzes the current state of the LSM storage system as represented by the snapshot
     * and determines if a compaction task is necessary. If a compaction task is needed, it returns
     * a `CompactionTask` object that describes the task to be performed. If no compaction is needed,
     * it returns `null`.
     *
     * @param snapshot The current state of the LSM storage system.
     * @return A `CompactionTask` if compaction is needed, or `null` if no compaction is necessary.
     */
    fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask?

    /**
     * Applies the result of a compaction task to the provided snapshot of the LSM storage system and return a new snapshot.
     *
     * This method updates the LSM storage state based on the provided compaction task and the new SSTable IDs
     * generated during the compaction. It removes the SSTable IDs that were compacted and adds the new SSTable IDs
     * to the appropriate levels. If the system is in recovery mode, additional steps may be taken to ensure consistency.
     * The method does not directly modify the state of the [snapshot]. Instead, it returns a new snapshot
     * representing the state of the LSM storage system after applying the compaction result.
     *
     * @param snapshot The current state of the LSM storage system before applying the compaction result.
     * @param task The compaction task that was performed.
     * @param newSstIds The list of new SSTable IDs generated during the compaction.
     * @param inRecovery Indicates whether the system is in recovery mode.
     * @return An [LsmCompactionResult] object representing the state of the LSM storage system after applying the compaction result.
     * @throws Error if the task is not of the expected type or if there is a mismatch in SSTable IDs.
     */
    fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult

    /**
     * Updates the levels in the LSM storage system based on the provided snapshot and task.
     *
     * This method removes invalid levels and updates them with the levels from the provided snapshot.
     * It is used to reflect the changes made after the compaction process.
     *
     * @param levels The current levels in the LSM storage system.
     * @param task The compaction task that was performed.
     * @param snapshot The snapshot of the LSM storage system after the compaction. Might not reflect the exact current state if there are concurrent modifications to the l0sstables or levels.
     */
    fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    )

    /**
     * Determines whether to flush the current data to level 0 (L0).
     *
     * This method checks if the current conditions require a flush operation to level 0 (L0) of the LSM storage system.
     *
     * @return `true` if a flush to L0 is required, `false` otherwise.
     */
    fun flushToL0(): Boolean
}
