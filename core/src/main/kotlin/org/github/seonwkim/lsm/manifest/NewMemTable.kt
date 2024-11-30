package org.github.seonwkim.lsm.manifest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class NewMemTable @JsonCreator constructor(
    @JsonProperty("memTableId") val memTableId: Int
) : ManifestRecord
