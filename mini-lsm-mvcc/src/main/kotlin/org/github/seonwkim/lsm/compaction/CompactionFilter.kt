package org.github.seonwkim.lsm.compaction

import org.github.seonwkim.common.ComparableByteArray

sealed interface CompactionFilter

class Prefix(bytes: ComparableByteArray) : CompactionFilter
