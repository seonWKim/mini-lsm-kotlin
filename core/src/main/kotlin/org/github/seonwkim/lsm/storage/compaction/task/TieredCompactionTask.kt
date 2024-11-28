package org.github.seonwkim.lsm.storage.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class TieredCompactionTask @JsonCreator constructor(
    @JsonProperty("tiers") val tiers: List<Pair<Int, List<Int>>>,
    @JsonProperty("bottomTierIncluded") val bottomTierIncluded: Boolean
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return bottomTierIncluded
    }
}
