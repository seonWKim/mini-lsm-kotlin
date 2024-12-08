package org.github.seonwkim.lsm.compaction.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * Interface for compaction tasks.
 * Implementations of this interface define specific compaction task strategies.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = LeveledCompactionTask::class, name = "LeveledTask"),
    JsonSubTypes.Type(value = TieredCompactionTask::class, name = "TieredTask"),
    JsonSubTypes.Type(value = SimpleLeveledCompactionTask::class, name = "SimpleTask"),
    JsonSubTypes.Type(value = ForceFullCompactionTask::class, name = "ForceFullCompaction")
)
sealed interface CompactionTask {

    /**
     * Determines if the compaction process should include the bottom level.
     *
     * @return true if the compaction process includes the bottom-most level, false otherwise.
     */
    fun compactToBottomLevel(): Boolean

    /**
     * Checks if level 0 compaction occurred.
     *
     * @return true if level 0 compaction occurred, false otherwise
     */
    fun l0Compaction(): Boolean
}
