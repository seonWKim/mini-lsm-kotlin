package org.github.seonwkim.lsm.compaction.controller

import mu.KotlinLogging
import org.github.seonwkim.lsm.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.SstLevel
import org.github.seonwkim.lsm.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.task.CompactionTask
import org.github.seonwkim.lsm.compaction.task.SimpleLeveledCompactionTask
import java.util.*

/**
 * Controller for simple leveled compaction in the LSM storage system.
 *
 * This controller handles the compaction process for a leveled compaction strategy, where SSTables are organized
 * into multiple levels. Compaction is triggered based on the size ratio between levels and other criteria defined
 * in the provided options.
 *
 * @property options The options for configuring the simple leveled compaction strategy.
 */
class SimpleLeveledCompactionController(
    private val options: SimpleLeveledCompactionOptions
) : CompactionController {

    private val log = KotlinLogging.logger { }

    private val sizeRatioLimit = options.sizeRatioPercent.toDouble() / 100.0

    override fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask? {
        val l0Sstables = snapshot.l0Sstables
        val l0SstablesSize = l0Sstables.size
        val levels = snapshot.levels
        val levelSizes = listOf(l0SstablesSize) + levels.map { it.sstIds.size }

        for (upperLevel in 0 until options.maxLevels) {
            if (upperLevel == 0 && l0SstablesSize < options.level0FileNumCompactionTrigger) {
                continue
            }

            // We don't have to compact when we don't have any sstables to compact
            if (levelSizes[upperLevel] == 0) {
                continue
            }

            val lowerLevel = upperLevel + 1
            val sizeRatio = levelSizes[lowerLevel].toDouble() / levelSizes[upperLevel].toDouble()
            // no need for compaction as we already have enough lower level sstables
            if (sizeRatio >= sizeRatioLimit) {
                continue
            }

            log.debug { "Compaction triggered at level $upperLevel and $lowerLevel with size ratio $sizeRatio" }
            return SimpleLeveledCompactionTask(
                upperLevel = if (upperLevel == 0) null else upperLevel,
                upperLevelSstIds = if (upperLevel == 0) {
                    l0Sstables.toList()
                } else {
                    levels[upperLevel - 1].sstIds.toList()
                },
                lowerLevel = lowerLevel,
                lowerLevelSstIds = levels[lowerLevel - 1].sstIds.toList(),
                lowerLevelBottomLevel = lowerLevel == options.maxLevels
            )
        }

        return null
    }

    override fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        if (task !is SimpleLeveledCompactionTask) {
            throw Error("task($task) should be of type SimpleLeveledCompactionTask")
        }

        val sstIdsToRemove = hashSetOf<Int>()

        val l0Sstables = snapshot.l0Sstables.toMutableList()
        val levels = snapshot.levels.toMutableList()
        log.debug { "Simple Leveled compaction: l0($l0Sstables), levels($levels)" }
        val upperLevel = task.upperLevel
        if (task.l0Compaction()) {
            sstIdsToRemove.addAll(task.upperLevelSstIds)
            val l0SstsCompacted = task.upperLevelSstIds.toHashSet()
            val newL0Sstables = l0Sstables.filter { !l0SstsCompacted.remove(it) }
            if (l0SstsCompacted.isNotEmpty()) {
                throw Error("l0SstsCompacted should be empty")
            }
            l0Sstables.clear()
            l0Sstables.addAll(newL0Sstables)
        } else {
            require(upperLevel != null)
            if (task.upperLevelSstIds != levels[upperLevel - 1].sstIds) {
                throw Error("sst mismatched: ${task.upperLevelSstIds} != ${levels[upperLevel - 1].sstIds}")
            }

            sstIdsToRemove.addAll(levels[upperLevel - 1].sstIds)
            levels[upperLevel - 1].sstIds.clear()
        }

        sstIdsToRemove.addAll(levels[task.lowerLevel - 1].sstIds)
        levels[task.lowerLevel - 1].sstIds.clear()
        levels[task.lowerLevel - 1].sstIds.addAll(newSstIds)

        return LsmCompactionResult(
            snapshot = LsmStorageSstableSnapshot(
                l0Sstables = l0Sstables,
                levels = levels,
            ),
            sstIdsToRemove = sstIdsToRemove,
            l0Compacted = upperLevel == null
        )
    }

    /**
     * Updates the levels in the LSM storage system based on the provided snapshot.
     *
     * In SimpleLeveledCompaction, no SSTable is added to a level while the compaction process is being processed.
     * Therefore, this method can simply clear all the existing levels and adds new levels from the provided snapshot.
     */
    override fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    ) {
        levels.clear()
        levels.addAll(snapshot.levels)
    }

    override fun flushToL0(): Boolean {
        return true
    }
}
