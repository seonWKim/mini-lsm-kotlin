package org.github.seonwkim.lsm.manifest

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = FlushRecord::class, name = "Flush"),
    JsonSubTypes.Type(value = NewMemTableRecord::class, name = "NewMemTable"),
    JsonSubTypes.Type(value = CompactionRecord::class, name = "Compaction")
)
sealed interface ManifestRecord
