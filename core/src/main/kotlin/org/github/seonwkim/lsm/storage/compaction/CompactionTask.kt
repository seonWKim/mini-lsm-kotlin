package org.github.seonwkim.lsm.storage.compaction

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
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
    JsonSubTypes.Type(value = ForceFullCompaction::class, name = "ForceFullCompaction")
)
sealed interface CompactionTask {
    fun compactToBottomLevel(): Boolean
}

data class LeveledTask @JsonCreator constructor(
    @JsonProperty("meta") val meta: LeveledCompactionTaskMeta
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return meta.lowerLevelBottomLevel
    }
}

data class TieredTask @JsonCreator constructor(
    @JsonProperty("meta") val meta: TieredCompactionTaskMeta
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return meta.bottomTierIncluded
    }
}

data class SimpleTask @JsonCreator constructor(
    @JsonProperty("meta") val meta: SimpleLeveledCompactionTaskMeta
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return meta.lowerLevelBottomLevel
    }
}

data class ForceFullCompaction @JsonCreator constructor(
    @JsonProperty("l0Sstables") val l0Sstables: List<Int>,
    @JsonProperty("l1Sstables") val l1Sstables: List<Int>
) : CompactionTask {
    override fun compactToBottomLevel(): Boolean {
        return true
    }
}

data class LeveledCompactionTaskMeta @JsonCreator constructor(
    @JsonProperty("upperLevel") val upperLevel: Int,
    @JsonProperty("upperLevelSstIds") val upperLevelSstIds: List<Int>,
    @JsonProperty("lowerLevel") val lowerLevel: Int,
    @JsonProperty("lowerLevelSstIds") val lowerLevelSstIds: List<Int>,
    @JsonProperty("lowerLevelBottomLevel") val lowerLevelBottomLevel: Boolean
)

data class TieredCompactionTaskMeta @JsonCreator constructor(
    @JsonProperty("tiers") val tiers: List<Pair<Int, List<Int>>>,
    @JsonProperty("bottomTierIncluded") val bottomTierIncluded: Boolean
)

data class SimpleLeveledCompactionTaskMeta @JsonCreator constructor(
    @JsonProperty("upperLevel") val upperLevel: Int,
    @JsonProperty("upperLevelSstIds") val upperLevelSstIds: List<Int>,
    @JsonProperty("lowerLevel") val lowerLevel: Int,
    @JsonProperty("lowerLevelSstIds") val lowerLevelSstIds: List<Int>,
    @JsonProperty("lowerLevelBottomLevel") val lowerLevelBottomLevel: Boolean
)
