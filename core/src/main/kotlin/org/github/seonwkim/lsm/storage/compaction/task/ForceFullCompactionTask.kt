package org.github.seonwkim.lsm.storage.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class ForceFullCompactionTask @JsonCreator constructor(
    @JsonProperty("l0Sstables") val l0Sstables: List<Int>,
    @JsonProperty("l1Sstables") val l1Sstables: List<Int>
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return true
    }
}
