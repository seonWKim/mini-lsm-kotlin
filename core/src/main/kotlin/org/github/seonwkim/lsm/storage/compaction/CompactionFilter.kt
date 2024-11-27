package org.github.seonwkim.lsm.storage.compaction

import org.github.seonwkim.common.ComparableByteArray

sealed interface CompactionFilter

class Prefix(bytes: ComparableByteArray) : CompactionFilter
