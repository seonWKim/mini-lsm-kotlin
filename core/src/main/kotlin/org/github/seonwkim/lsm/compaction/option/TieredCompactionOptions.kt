package org.github.seonwkim.lsm.compaction.option

/**
 * Tiered compaction
 * ref. RockDB's universal compaction
 */
data class TieredCompactionOptions(
    val numTiers: Int,
    val maxSizeAmplificationPercent: Int,
    val sizeRatio: Int,
    val minMergeWidth: Int,
    val maxMergeWidth: Int?
) : CompactionOptions
