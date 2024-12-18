package org.github.seonwkim.lsm.manifest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class FlushRecord @JsonCreator constructor(
    @JsonProperty("sstId") val sstId: Int
) : ManifestRecord
