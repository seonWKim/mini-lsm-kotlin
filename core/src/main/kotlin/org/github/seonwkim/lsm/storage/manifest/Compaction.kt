package org.github.seonwkim.lsm.storage.manifest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.github.seonwkim.lsm.storage.compaction.task.CompactionTask

data class Compaction @JsonCreator constructor(
    @JsonProperty("task") val task: CompactionTask,
    @JsonProperty("output") val output: List<Int>
) : ManifestRecord
