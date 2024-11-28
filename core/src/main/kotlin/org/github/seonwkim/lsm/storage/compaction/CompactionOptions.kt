package org.github.seonwkim.lsm.storage.compaction

sealed interface CompactionOptions

/**
 * No compaction
 */
data object NoCompaction : CompactionOptions

/**
 * Simple leveled compaction
 */
data class SimpleLeveledCompactionOptions(
    val sizeRatioPercent: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevels: Int
) : CompactionOptions

/**
 * Leveled compaction with partial compaction + dynamic level support
 * ref. RocksDB's Leveled Compaction
 */
data class LeveledCompactionOptions(
    val levelSizeMultiplier: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevel: Int,
    val baseLevelSizeMB: Int
) : CompactionOptions

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
