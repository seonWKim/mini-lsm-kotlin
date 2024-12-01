package org.github.seonwkim.lsm.compaction.controller

import mu.KotlinLogging
import org.github.seonwkim.lsm.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.SstLevel
import org.github.seonwkim.lsm.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.compaction.option.TieredCompactionOptions
import org.github.seonwkim.lsm.compaction.task.CompactionTask
import org.github.seonwkim.lsm.compaction.task.Tier
import org.github.seonwkim.lsm.compaction.task.TieredCompactionTask
import java.util.*

class TieredCompactionController(
    private val options: TieredCompactionOptions
) : CompactionController {

    private val log = KotlinLogging.logger { }
    private val sizeRatioLimit = (100.0 + options.sizeRatio.toDouble()) / 100.0

    override fun generateCompactionTask(snapshot: LsmStorageSstableSnapshot): CompactionTask? {
        if (snapshot.l0Sstables.isNotEmpty()) {
            throw Error("L0 sstables should be empty in tiered compaction: ${snapshot.l0Sstables}")
        }

        if (snapshot.levels.size < options.numTiers) {
            return null
        }

        if (spaceAmplificationExceeded(snapshot)) {
            return TieredCompactionTask(
                tiers = snapshot.levels.map { it.deepCopy().toTier() },
                bottomTierIncluded = true
            )
        }

        // compaction triggered by size ratio
        var sumPreviousTiersSize = 0
        for (id in 0 until snapshot.levels.size - 1) {
            val tiersCount = id + 1
            // We only run size ratio triggered compaction when number of tiers exceeds minMergeWidth
            if (tiersCount < options.minMergeWidth) {
                continue
            }

            sumPreviousTiersSize += snapshot.levels[id].sstIds.size
            val currentTierSize = snapshot.levels[id + 1].sstIds.size
            val sizeRatio = currentTierSize.toDouble() / sumPreviousTiersSize.toDouble()
            if (sizeRatio > sizeRatioLimit) {
                log.debug { "Compaction triggered by size ratio: ${sizeRatio * 100.0}% > ${sizeRatioLimit * 100.0}%" }
                return TieredCompactionTask(
                    tiers = snapshot.levels.take(tiersCount).map { it.deepCopy().toTier() },
                    bottomTierIncluded = tiersCount >= snapshot.levels.size
                )
            }
        }

        val numTiersToTake = minOf(snapshot.levels.size, options.maxMergeWidth ?: Int.MAX_VALUE)
        log.debug { "Compaction triggered by reducing sorted runs" }
        return TieredCompactionTask(
            tiers = snapshot.levels.take(numTiersToTake).map { it.deepCopy().toTier() },
            bottomTierIncluded = snapshot.levels.size >= numTiersToTake
        )
    }

    private fun spaceAmplificationExceeded(snapshot: LsmStorageSstableSnapshot): Boolean {
        val size = (0 until snapshot.levels.lastIndex).sumOf { snapshot.levels[it].sstIds.size }
        val spaceAmpRatio = (size.toDouble() / snapshot.levels.last().sstIds.size.toDouble()) * 100.0
        val spaceAmplificationExceeded = spaceAmpRatio >= options.maxSizeAmplificationPercent
        if (spaceAmplificationExceeded) {
            log.debug { "Space amplification ratio exceeded: $spaceAmpRatio >= ${options.maxSizeAmplificationPercent}" }
        }
        return spaceAmplificationExceeded
    }

    override fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        if (task !is TieredCompactionTask) {
            throw Error("task($task) should be of type TieredCompactionTask")
        }

        if (snapshot.l0Sstables.isNotEmpty()) {
            throw Error("L0 sstables should be empty in tiered compaction: ${snapshot.l0Sstables}")
        }

        val removeTargetSstIdsByTierId = task.tiers.associate { it.id to it.sstIds }.toMutableMap()
        val tiers = mutableListOf<SstLevel>()
        var newTierAdded = false
        val sstIdsToRemove = hashSetOf<Int>()
        for (tier in snapshot.levels) {
            val (tierId, sstIds) = tier
            val removeTargetSstIds = removeTargetSstIdsByTierId.remove(tierId)
            if (removeTargetSstIds != null) {
                if (sstIds != removeTargetSstIds) {
                    throw Error("File changed after issuing tiered compaction task. $sstIds != $removeTargetSstIds")
                }
                sstIdsToRemove.addAll(sstIds)
            } else {
                tiers.add(tier)
            }

            if (removeTargetSstIdsByTierId.isEmpty() && !newTierAdded) {
                newTierAdded = true
                tiers.add(
                    SstLevel(
                        id = newSstIds[0],
                        sstIds = newSstIds.toMutableList()
                    )
                )
            }
        }

        if (removeTargetSstIdsByTierId.isNotEmpty()) {
            throw Error("Some tiers not found?")
        }

        return LsmCompactionResult(
            snapshot = LsmStorageSstableSnapshot(
                l0Sstables = emptyList(),
                levels = tiers,
            ),
            sstIdsToRemove = sstIdsToRemove
        )
    }

    /**
     * New tiers can be added to the levels while the compaction is being processed, so invalid levels must be
     * removed by traversing from the last element of [levels].
     */
    override fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    ) {
        if (task !is TieredCompactionTask) {
            throw Error("task($task) should be of type TieredCompactionTask")
        }
        val tierIdsToExclude = mutableSetOf<Int>()

        // tier ids included in task is targeted for compaction
        task.tiers.forEach { tierIdsToExclude.add(it.id) }

        // tier ids included in snapshot has 2 types
        // 1. newly created tier ids
        // 2. existing tier ids which didn't participate in the compaction process
        // because we are going to append tiers included in snapshot at the end of levels, we first exclude them all from current level
        snapshot.levels.forEach { tierIdsToExclude.add(it.id) }

        levels.removeIf { tierIdsToExclude.contains(it.id) }
        levels.addAll(snapshot.levels)
    }

    override fun flushToL0(): Boolean {
        return false
    }

    private fun SstLevel.toTier(): Tier {
        return Tier(
            id = this.id,
            sstIds = this.sstIds
        )
    }
}
