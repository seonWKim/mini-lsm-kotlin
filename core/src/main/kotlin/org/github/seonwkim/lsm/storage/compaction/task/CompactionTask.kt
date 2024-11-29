package org.github.seonwkim.lsm.storage.compaction.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

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
    fun compactToBottomLevel(): Boolean

    // Whether l0 compaction occurred.
    fun l0Compaction(): Boolean
}
