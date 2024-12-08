package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Task for tiered compaction.
 * This task handles compaction across multiple tiers, where each tier consists of a list of SSTables.
 *
 * @property tiers the list of tiers involved in the compaction
 * @property bottomTierIncluded whether the bottom-most tier is included in the compaction
 */
data class TieredCompactionTask @JsonCreator constructor(
    @JsonProperty("tiers") val tiers: List<Tier>,
    @JsonProperty("bottomTierIncluded") val bottomTierIncluded: Boolean
) : CompactionTask {

    /**
     * Determines if the compaction process should include the bottom level.
     * For [TieredCompactionTask], this is determined by the `bottomTierIncluded` property.
     *
     * @return true if the compaction process includes the bottom-most level, false otherwise.
     */
    override fun compactToBottomLevel(): Boolean {
        return bottomTierIncluded
    }

    /**
     * Checks if level 0 compaction occurred.
     * For [TieredCompactionTask], this always returns false as it does not involve level 0 compaction.
     *
     * @return false as level 0 compaction does not occur in tiered compaction.
     */
    override fun l0Compaction(): Boolean {
        return false
    }
}

/**
 * Represents a tier in the tiered compaction task.
 *
 * @property id the unique identifier of the tier
 * @property sstIds the list of SSTable IDs in the tier
 */
data class Tier @JsonCreator constructor(
    @JsonProperty("id") val id: Int,
    @JsonProperty("sstIds") val sstIds: List<Int>
)
