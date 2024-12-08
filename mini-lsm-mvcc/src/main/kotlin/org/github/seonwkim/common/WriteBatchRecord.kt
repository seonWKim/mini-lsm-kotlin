package org.github.seonwkim.common

sealed interface WriteBatchRecord

data class Put(val key: TimestampedByteArray, val value: ComparableByteArray) : WriteBatchRecord
data class Delete(val key: TimestampedByteArray) : WriteBatchRecord
