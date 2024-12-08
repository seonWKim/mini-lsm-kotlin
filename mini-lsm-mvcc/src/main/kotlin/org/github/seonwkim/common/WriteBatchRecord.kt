package org.github.seonwkim.common

sealed interface WriteBatchRecord

data class Put(val key: ComparableByteArray, val value: ComparableByteArray) : WriteBatchRecord
data class Delete(val key: ComparableByteArray) : WriteBatchRecord
