package org.github.seonwkim.common

sealed interface Bound {
    data object Unbounded : Bound
    data class Included(val value: ComparableByteArray) : Bound
    data class Excluded(val value: ComparableByteArray) : Bound
}
