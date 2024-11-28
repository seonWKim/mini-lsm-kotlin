package org.github.seonwkim.lsm.storage.compaction.option

/**
 * Simple leveled compaction
 */
data class SimpleLeveledCompactionOptions(
    val sizeRatioPercent: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevels: Int
) : CompactionOptions
