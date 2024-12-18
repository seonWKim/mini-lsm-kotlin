package org.github.seonwkim.lsm.manifest

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class NewMemTableRecord @JsonCreator constructor(
    @JsonProperty("memTableId") val memTableId: Int
) : ManifestRecord
