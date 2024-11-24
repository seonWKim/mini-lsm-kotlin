package org.github.seonwkim.common

sealed interface Bound
data object Unbounded : Bound
data class Included(val key: ComparableByteArray) : Bound
data class Excluded(val key: ComparableByteArray) : Bound
