package org.github.seonwkim.lsm.storage.compaction.controller

import mu.KotlinLogging
import org.github.seonwkim.lsm.storage.LsmCompactionResult
import org.github.seonwkim.lsm.storage.LsmStorageStateDiskSnapshot
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import org.github.seonwkim.lsm.storage.compaction.task.SimpleLeveledCompactionTask

class SimpleLeveledCompactionController(
    private val options: SimpleLeveledCompactionOptions
) : CompactionController {

    private val log = KotlinLogging.logger { }

    private val sizeRatioLimit = options.sizeRatioPercent.toDouble() / 100.0

    override fun generateCompactionTask(snapshot: LsmStorageStateDiskSnapshot): CompactionTask? {
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

            log.info { "Compaction triggered at level $upperLevel and $lowerLevel with size ratio $sizeRatio" }
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

    override fun applyCompactionResult(
        snapshot: LsmStorageStateDiskSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        if (task !is SimpleLeveledCompactionTask) {
            throw Error("task should be of type SimpleLeveledCompactionTask")
        }

        val sstIdsToRemove = hashSetOf<Int>()

        val l0Sstables = snapshot.l0Sstables.toMutableList()
        val levels = snapshot.levels.toMutableList()
        log.info { "Simple Leveled compaction: l0($l0Sstables), levels($levels)" }
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
            snapshot = LsmStorageStateDiskSnapshot(
                l0Sstables = l0Sstables,
                levels = levels,
                sstables = snapshot.sstables
            ),
            sstIdsToRemove = sstIdsToRemove,
            l0Compacted = upperLevel == null
        )
    }

    override fun flushToL0(): Boolean {
        return true
    }
}
