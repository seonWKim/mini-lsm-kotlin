package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class ForceFullCompactionTask @JsonCreator constructor(
    @JsonProperty("l0Sstables") val l0Sstables: List<Int>,
    @JsonProperty("l1Sstables") val l1Sstables: List<Int>
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return true
    }

    /**
     * [ForceFullCompactionTask] always flushes l0 to l1.
     */
    override fun l0Compaction(): Boolean {
        return true
    }
}
