package org.github.seonwkim.lsm.storage

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
}
