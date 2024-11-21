package org.github.seonwkim.lsm.storage

sealed interface CompactionOptions {
    data object NoCompaction : CompactionOptions
    data class Simple(val options: SimpleLeveledCompactionOptions) : CompactionOptions
    data class Leveled(val options: LeveledCompactionOptions) : CompactionOptions
    data class Tiered(val options: TieredCompactionOptions) : CompactionOptions
}

/**
 * Simple leveled compaction
 */
data class SimpleLeveledCompactionOptions(
    val sizeRatioPercent: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevels: Int
)

/**
 * Leveled compaction with partial compaction + dynamic level support
 * ref. RocksDB's Leveled Compaction
 */
data class LeveledCompactionOptions(
    val levelSizeMultiplier: Int,
    val level0FileNumCompactionTrigger: Int,
    val maxLevel: Int,
    val baseLevelSizeMB: Int
)

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
)
