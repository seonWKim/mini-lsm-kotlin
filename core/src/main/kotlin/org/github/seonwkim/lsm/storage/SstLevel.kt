package org.github.seonwkim.lsm.storage

/**
 * SsTables sorted by key range.
 * L1 ~ L_max for leveled compaction, or tiers for tiered compaction.
 */
data class SstLevel(
    // sst level or tier id
    val id: Int,

    // sst ids or tier ids
    val sstIds: MutableList<Int>
) {
    init {
        if (id < 0) {
            throw IllegalStateException("SST id $id < 0 or Tier id $id < 0")
        }
    }

    fun deepCopy(): SstLevel {
        return SstLevel(
            id = this.id,
            sstIds = this.sstIds.toMutableList()
        )
    }
}
