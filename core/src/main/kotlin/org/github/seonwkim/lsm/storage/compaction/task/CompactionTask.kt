package org.github.seonwkim.lsm.storage.compaction.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = LeveledTask::class, name = "LeveledTask"),
    JsonSubTypes.Type(value = TieredTask::class, name = "TieredTask"),
    JsonSubTypes.Type(value = SimpleTask::class, name = "SimpleTask"),
    JsonSubTypes.Type(value = ForceFullCompactionTask::class, name = "ForceFullCompaction")
)
sealed interface CompactionTask {
    fun compactToBottomLevel(): Boolean
}
