package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Task for simple leveled compaction.
 * This task handles compaction between two levels, with an optional upper level.
 *
 * @property upperLevel the optional upper level for compaction (nullable)
 * @property upperLevelSstIds the list of SSTable IDs at the upper level to be compacted
 * @property lowerLevel the lower level for compaction
 * @property lowerLevelSstIds the list of SSTable IDs at the lower level to be compacted
 * @property lowerLevelBottomLevel whether the lower level is the bottom-most level
 */
data class SimpleLeveledCompactionTask @JsonCreator constructor(
    @JsonProperty("upperLevel") val upperLevel: Int?,
    @JsonProperty("upperLevelSstIds") val upperLevelSstIds: List<Int>,
    @JsonProperty("lowerLevel") val lowerLevel: Int,
    @JsonProperty("lowerLevelSstIds") val lowerLevelSstIds: List<Int>,
    @JsonProperty("lowerLevelBottomLevel") val lowerLevelBottomLevel: Boolean
) : CompactionTask {

    /**
     * Determines if the compaction process should include the bottom level.
     * For [SimpleLeveledCompactionTask], this is determined by the `lowerLevelBottomLevel` property.
     *
     * @return true if the compaction process includes the bottom-most level, false otherwise.
     */
    override fun compactToBottomLevel(): Boolean {
        return lowerLevelBottomLevel
    }

    /**
     * Checks if level 0 compaction occurred.
     * For [SimpleLeveledCompactionTask], this returns true if `upperLevel` is null.
     *
     * @return true if level 0 compaction occurred, false otherwise.
     */
    override fun l0Compaction(): Boolean {
        return upperLevel == null
    }
}
