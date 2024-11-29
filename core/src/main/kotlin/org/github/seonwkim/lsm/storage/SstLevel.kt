package org.github.seonwkim.lsm.storage

import org.github.seonwkim.lsm.sstable.Sstable

/**
 * SsTables sorted by key range.
 * L1 ~ L_max for leveled compaction, or tiers for tiered compaction.
 */
data class SstLevel(
    val level: Int,
    val sstIds: MutableList<Int>
) {
    init {
        if (level < 1) {
            throw IllegalStateException("Level should be greater than or equal to 1")
        }
    }

    fun deepCopy(): SstLevel {
        return SstLevel(
            level = this.level,
            sstIds = this.sstIds.toMutableList()
        )
    }
}
