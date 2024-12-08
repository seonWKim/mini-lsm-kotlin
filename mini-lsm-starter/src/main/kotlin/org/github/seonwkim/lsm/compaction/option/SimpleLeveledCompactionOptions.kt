package org.github.seonwkim.lsm.compaction.option

/**
 * Options for simple leveled compaction.
 *
 * @property sizeRatioPercent the minimum size ratio of lower / upper level in percentage
 * @property level0FileNumCompactionTrigger the number of files in level 0 to trigger compaction
 * @property maxLevels the maximum number of levels in the compaction
 */
data class SimpleLeveledCompactionOptions(
    val sizeRatioPercent: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevels: Int
) : CompactionOptions
