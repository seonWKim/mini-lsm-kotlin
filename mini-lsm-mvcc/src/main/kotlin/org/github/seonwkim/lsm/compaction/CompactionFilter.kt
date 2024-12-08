package org.github.seonwkim.lsm.compaction

import org.github.seonwkim.common.TimestampedByteArray

sealed interface CompactionFilter

class Prefix(bytes: TimestampedByteArray) : CompactionFilter
