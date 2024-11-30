package org.github.seonwkim.lsm.storage

/**
 * SsTables sorted by key range.
 * L1 ~ L_max for leveled compaction, or tiers for tiered compaction.
 */
data class SstLevel(
    // sst level or tier id
    val level: Int,

    // sst ids or tier ids
    val sstIds: MutableList<Int>
) {
    init {
        if (level < 0) {
            throw IllegalStateException("SST id $level < 0 or Tier id $level < 0")
        }
    }

    fun deepCopy(): SstLevel {
        return SstLevel(
            level = this.level,
            sstIds = this.sstIds.toMutableList()
        )
    }
}
