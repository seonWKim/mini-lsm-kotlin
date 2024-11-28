package org.github.seonwkim.lsm.storage.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class SimpleLeveledCompactionTask @JsonCreator constructor(
    @JsonProperty("upperLevel") val upperLevel: Int?,
    @JsonProperty("upperLevelSstIds") val upperLevelSstIds: List<Int>,
    @JsonProperty("lowerLevel") val lowerLevel: Int,
    @JsonProperty("lowerLevelSstIds") val lowerLevelSstIds: List<Int>,
    @JsonProperty("lowerLevelBottomLevel") val lowerLevelBottomLevel: Boolean
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return lowerLevelBottomLevel
    }
}
