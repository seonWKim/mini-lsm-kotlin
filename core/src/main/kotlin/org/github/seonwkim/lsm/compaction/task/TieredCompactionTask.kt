package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class TieredCompactionTask @JsonCreator constructor(
    @JsonProperty("tiers") val tiers: List<Tier>,
    @JsonProperty("bottomTierIncluded") val bottomTierIncluded: Boolean
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return bottomTierIncluded
    }

    override fun l0Compaction(): Boolean {
        TODO()
    }
}

data class Tier @JsonCreator constructor(
    @JsonProperty("id") val id: Int,
    @JsonProperty("sstIds") val sstIds: List<Int>
)
