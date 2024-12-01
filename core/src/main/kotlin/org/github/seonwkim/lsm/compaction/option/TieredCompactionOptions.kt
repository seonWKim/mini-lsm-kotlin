package org.github.seonwkim.lsm.compaction.option

/**
 * Options for tiered compaction.
 * Reference: RocksDB's universal compaction.
 *
 * @property minNumTiers the minimum number of tiers in the compaction to trigger compaction
 * @property maxSizeAmplificationPercent the maximum size amplification percentage
 * @property maxSizeRatio the maximum size ratio between tiers to trigger compaction
 * @property minMergeWidth the minimum number of files to merge at once. Compaction is triggered by size ratio only if the number of tiers exceeds this value.
 * @property maxMergeWidth the maximum number of files to merge at once (nullable). Limits the number of tiers to take for compaction.
 */
data class TieredCompactionOptions(
    val minNumTiers: Int,
    val maxSizeAmplificationPercent: Int,
    val maxSizeRatio: Int,
    val minMergeWidth: Int,
    val maxMergeWidth: Int?
) : CompactionOptions
