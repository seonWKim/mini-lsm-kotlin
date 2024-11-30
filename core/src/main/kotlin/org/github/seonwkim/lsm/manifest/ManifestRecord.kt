package org.github.seonwkim.lsm.manifest

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = Flush::class, name = "Flush"),
    JsonSubTypes.Type(value = NewMemTable::class, name = "NewMemTable"),
    JsonSubTypes.Type(value = Compaction::class, name = "Compaction")
)
sealed interface ManifestRecord
