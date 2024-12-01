package org.github.seonwkim.lsm.compaction.controller

import mu.KotlinLogging
import org.github.seonwkim.lsm.LsmStorageSstableSnapshot
import org.github.seonwkim.lsm.SstLevel
import org.github.seonwkim.lsm.compaction.LsmCompactionResult
import org.github.seonwkim.lsm.compaction.option.LeveledCompactionOptions
import org.github.seonwkim.lsm.compaction.task.CompactionTask
import org.github.seonwkim.lsm.compaction.task.LeveledCompactionTask
import org.github.seonwkim.lsm.sstable.Sstable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class LeveledCompactionController(
    private val options: LeveledCompactionOptions
) : CompactionController {

    companion object {
        private const val MB_SIZE = 1024L
    }

    private val log = KotlinLogging.logger { }

    override fun generateCompactionTask(
        snapshot: LsmStorageSstableSnapshot,
        sstables: ConcurrentHashMap<Int, Sstable>
    ): CompactionTask? {
        val targetLevelSize = LongArray(options.maxLevel) { 0L }
        val realLevelSize = mutableListOf<Long>()
        var baseLevel = options.maxLevel
        for (i in 0 until options.maxLevel) {
            realLevelSize.add(snapshot.levels[i].sstIds.sumOf { id -> sstables[id]?.file?.size ?: 0 })
        }
        val baseLevelSizeBytes = options.baseLevelSizeMB * MB_SIZE * MB_SIZE

        // select base level and compute target level size
        targetLevelSize[options.maxLevel - 1] = maxOf(realLevelSize[options.maxLevel - 1], baseLevelSizeBytes)
        for (i in (0 until options.maxLevel - 1).reversed()) {
            val nextLevelSize = targetLevelSize[i + 1]
            val thisLevelSize = nextLevelSize / options.levelSizeMultiplier
            if (nextLevelSize > baseLevelSizeBytes) {
                targetLevelSize[i] = thisLevelSize
            }
            if (targetLevelSize[i] > 0) {
                baseLevel = i + 1
            }
        }

        // Flush L0 SST is the top priority
        if (snapshot.l0SstableIds.size >= options.level0FileNumCompactionTrigger) {
            log.debug { "Flush L0 SST to base level: $baseLevel" }
            return LeveledCompactionTask(
                upperLevel = null,
                upperLevelSstIds = snapshot.l0SstableIds,
                lowerLevel = baseLevel,
                lowerLevelSstIds = findOverlappingSsts(
                    levels = snapshot.levels,
                    sstIds = snapshot.l0SstableIds,
                    sstables = sstables,
                    baseLevel = baseLevel),
                lowerLevelBottomLevel = baseLevel == options.maxLevel
            )
        }

        val priorities = mutableListOf<PriorityAndLevel>()
        for (level in 0 until options.maxLevel) {
            val priority = realLevelSize[level].toDouble() / targetLevelSize[level].toDouble()
            if (priority > 1.0) {
                priorities.add(
                    PriorityAndLevel(
                        priority = priority,
                        level = level + 1
                    )
                )
            }
        }

        val sortedPriorities = priorities.sortedWith(
            compareByDescending<PriorityAndLevel> { it.priority }.thenByDescending { it.level }
        )
        val firstPriority = sortedPriorities.firstOrNull() ?: return null
        val (_, level) = firstPriority
        val targetLevelSizesFormatted =
            targetLevelSize.map { String.format("%.3fMB", it.toDouble() / 1024.0 / 1024.0) }
        val realLevelSizesFormatted =
            realLevelSize.map { String.format("%.3fMB", it.toDouble() / 1024.0 / 1024.0) }
        log.debug {
            "target level sizes: $targetLevelSizesFormatted, real level sizes: $realLevelSizesFormatted, base level: $baseLevel"
        }

        // select oldest sst  to compact
        val selectedSstIds = snapshot.levels[level - 1].sstIds.minOrNull()!!
        log.debug { "compaction triggered by priority: $level out of $priorities, select $selectedSstIds for compaction" }
        return LeveledCompactionTask(
            upperLevel = level,
            upperLevelSstIds = listOf(selectedSstIds),
            lowerLevel = level + 1,
            lowerLevelSstIds = findOverlappingSsts(
                levels = snapshot.levels,
                sstIds = listOf(selectedSstIds),
                sstables = sstables,
                baseLevel = level + 1,
            ),
            lowerLevelBottomLevel = level + 1 == options.maxLevel
        )
    }

    private fun findOverlappingSsts(
        levels: List<SstLevel>,
        sstIds: List<Int>,
        sstables: ConcurrentHashMap<Int, Sstable>,
        baseLevel: Int
    ): List<Int> {
        val beginKey = sstIds.mapNotNull { id -> sstables[id]?.firstKey }.minOrNull() ?: return emptyList()
        val endKey = sstIds.mapNotNull { id -> sstables[id]?.lastKey }.maxOrNull() ?: return emptyList()
        val overlapSsts = mutableListOf<Int>()
        for (sstId in levels[baseLevel - 1].sstIds) {
            val sst = sstables[sstId] ?: throw Error("sstable $sstId not found")
            val firstKey = sst.firstKey
            val lastKey = sst.lastKey
            if (!(lastKey < beginKey || firstKey > endKey)) {
                overlapSsts.add(sstId)
            }
        }

        return overlapSsts
    }

    override fun applyCompaction(
        snapshot: LsmStorageSstableSnapshot,
        sstables: ConcurrentHashMap<Int, Sstable>,
        task: CompactionTask,
        newSstIds: List<Int>,
        inRecovery: Boolean
    ): LsmCompactionResult {
        if (task !is LeveledCompactionTask) {
            throw Error("task($task) should be of type LeveledCompactionController")
        }

        val sstIdsToRemove = hashSetOf<Int>()
        val upperLevelSstIdsSet = task.upperLevelSstIds.toHashSet()
        val lowerLevelSstIdsSet = task.lowerLevelSstIds.toHashSet()
        val l0SstableIds = snapshot.l0SstableIds.toMutableList()
        val levels = snapshot.levels.map { it.deepCopy() }

        if (task.upperLevel != null) {
            val newUpperLevelSsts = levels[task.upperLevel - 1]
                .sstIds
                .filter { !upperLevelSstIdsSet.remove(it) }
            if (upperLevelSstIdsSet.isNotEmpty()) {
                throw Error("upperLevelSstIdsSet must be empty: $upperLevelSstIdsSet")
            }
            levels[task.upperLevel - 1].sstIds.clear()
            levels[task.upperLevel - 1].sstIds.addAll(newUpperLevelSsts)
        } else {
            val newL0Ssts = l0SstableIds
                .filter { !upperLevelSstIdsSet.remove(it) }

            l0SstableIds.clear()
            l0SstableIds.addAll(newL0Ssts)
        }

        sstIdsToRemove.addAll(task.upperLevelSstIds)
        sstIdsToRemove.addAll(task.lowerLevelSstIds)

        val newLowerLevelSsts = levels[task.lowerLevel - 1]
            .sstIds
            .filter { !lowerLevelSstIdsSet.remove(it) }
            .toMutableList()
        if (lowerLevelSstIdsSet.isNotEmpty()) {
            throw Error("lowerLevelSstIdsSet must be empty: $lowerLevelSstIdsSet")
        }
        newLowerLevelSsts.addAll(newSstIds)

        // Don't sort the SST IDs during recovery because actual SSTs are not loaded at that point
        if (!inRecovery) {
            newLowerLevelSsts.sortWith { s1, s2 ->
                val sst1 = sstables[s1] ?: throw Error("SST($s1) has been deleted unexpectedly")
                val sst2 = sstables[s2] ?: throw Error("SST($s2) has been deleted unexpectedly")
                sst1.firstKey.compareTo(sst2.firstKey)
            }
        }
        levels[task.lowerLevel - 1].sstIds.clear()
        levels[task.lowerLevel - 1].sstIds.addAll(newLowerLevelSsts)
        return LsmCompactionResult(
            snapshot = LsmStorageSstableSnapshot(
                l0SstableIds = l0SstableIds,
                levels = levels
            ),
            sstIdsToRemove = sstIdsToRemove,
            l0Compacted = task.upperLevel == null
        )
    }

    override fun updateLevels(
        levels: LinkedList<SstLevel>,
        task: CompactionTask,
        snapshot: LsmStorageSstableSnapshot
    ) {
        val levelsMap = snapshot.levels.associate { Pair(it.id, it.sstIds) }
        levels.forEach { (id, sstIds) ->
            // compaction processed applied on this level
            if (levelsMap.containsKey(id)) {
                sstIds.clear()
                sstIds.addAll(levelsMap[id]!!)
            }
        }
    }

    override fun flushToL0(): Boolean {
        return true
    }
}


data class PriorityAndLevel(
    val priority: Double,
    val level: Int
)
