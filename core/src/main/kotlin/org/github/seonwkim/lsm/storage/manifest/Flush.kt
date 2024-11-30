package org.github.seonwkim.lsm.storage.manifest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class Flush @JsonCreator constructor(
    @JsonProperty("sstId") val sstId: Int
) : ManifestRecord
