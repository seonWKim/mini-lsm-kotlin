package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Task for forcing a full compaction.
 * This task ensures that all SSTables from level 0 and level 1 are compacted.
 *
 * @property l0Sstables the list of SSTable IDs at level 0 to be compacted
 * @property l1Sstables the list of SSTable IDs at level 1 to be compacted
 */
data class ForceFullCompactionTask @JsonCreator constructor(
    @JsonProperty("l0Sstables") val l0Sstables: List<Int>,
    @JsonProperty("l1Sstables") val l1Sstables: List<Int>
) : CompactionTask {

    /**
     * Determines if the compaction process should include the bottom level.
     * For [ForceFullCompactionTask], this always returns true.
     *
     * @return true if the compaction process includes the bottom-most level.
     */
    override fun compactToBottomLevel(): Boolean {
        return true
    }

    /**
     * Checks if level 0 compaction occurred.
     * For [ForceFullCompactionTask], this always returns true as it always flushes l0 to l1.
     *
     * @return true if level 0 compaction occurred.
     */
    override fun l0Compaction(): Boolean {
        return true
    }
}
