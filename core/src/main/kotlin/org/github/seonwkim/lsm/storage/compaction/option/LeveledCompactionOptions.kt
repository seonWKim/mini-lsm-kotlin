package org.github.seonwkim.lsm.storage.compaction.option

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
