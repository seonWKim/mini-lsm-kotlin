package org.github.seonwkim.lsm.storage.compaction.controller

import mu.KotlinLogging
import org.github.seonwkim.lsm.storage.LsmStorageState
import org.github.seonwkim.lsm.storage.compaction.option.SimpleLeveledCompactionOptions
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask
import org.github.seonwkim.lsm.storage.compaction.task.SimpleLeveledCompactionTask

class SimpleLeveledCompactionController(
    private val options: SimpleLeveledCompactionOptions
) : CompactionController {

    private val log = KotlinLogging.logger { }

    private val sizeRatioLimit = options.sizeRatioPercent.toDouble() / 100.0

    /**
     * Generates a compaction task.
     *
     * Returns [null] if no compaction needs to be executed.
     */
    override fun generateCompactionTask(state: LsmStorageState): CompactionTask? {
        val l0Sstables = state.l0Sstables.withReadLock { it }.toList()
        val levels = state.levels.withReadLock { it }.toList()
        val levelSizes = mutableListOf<Int>()
        val l0SstablesSize = levels.size
        levelSizes.add(l0SstablesSize)
        for ((_, files) in levels) {
            levelSizes.add(files.size)
        }

        for (i in 0 until options.maxLevels) {
            if (i == 0 && l0SstablesSize < options.level0FileNumCompactionTrigger) {
                continue
            }

            val lowerLevel = i + 1
            val sizeRatio = levelSizes[lowerLevel].toDouble() / levelSizes[i].toDouble()
            if (sizeRatio >= sizeRatioLimit) {
                // no need for compaction as we already flushed enough sstables to the lower level
                continue
            }

            log.info { "Compaction triggered at level $i and $lowerLevel with size ratio $sizeRatio" }
            return SimpleLeveledCompactionTask(
                upperLevel = if (i == 0) null else i,
                upperLevelSstIds = if (i == 0) {
                    l0Sstables.toList()
                } else {
                    levels[i - 1].sstIds.toList()
                },
                lowerLevel = lowerLevel,
                lowerLevelSstIds = levels[lowerLevel - 1].sstIds.toList(),
                lowerLevelBottomLevel = lowerLevel == options.maxLevels
            )
        }

        return null
    }

    override fun flushTol0(): Boolean {
        return true
    }
}
