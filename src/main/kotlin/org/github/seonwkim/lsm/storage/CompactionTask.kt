package org.github.seonwkim.lsm.storage

sealed interface CompactionTask {
    data class Leveled(private val task: LeveledCompactionTask) : CompactionTask {
        override fun compactToBottomLevel(): Boolean {
            return task.isLowerLevelBottomLevel
        }
    }

    data class Tiered(private val task: TieredCompactionTask) : CompactionTask {
        override fun compactToBottomLevel(): Boolean {
            return task.bottomTierIncluded
        }
    }

    data class Simple(private val task: SimpleLeveledCompactionTask) : CompactionTask {
        override fun compactToBottomLevel(): Boolean {
            return task.isLowerLevelBottomLevel
        }
    }

    data class ForceFullCompaction(
        private val l0SsTables: List<Int>,
        private val l1SsTables: List<Int>
    ) : CompactionTask {
        override fun compactToBottomLevel(): Boolean {
            return true
        }
    }

    fun compactToBottomLevel(): Boolean
}

data class LeveledCompactionTask(
    val upperLevel: Int,
    val upperLevelSstIds: List<Int>,
    val lowerLevel: Int,
    val lowerLevelSstIds: List<Int>,
    val isLowerLevelBottomLevel: Boolean
)

data class TieredCompactionTask(
    val tiers: List<Pair<Int, List<Int>>>,
    val bottomTierIncluded: Boolean
)

data class SimpleLeveledCompactionTask(
    val upperLevel: Int,
    val upperLevelSstIds: List<Int>,
    val lowerLevel: Int,
    val lowerLevelSstIds: List<Int>,
    val isLowerLevelBottomLevel: Boolean
)
