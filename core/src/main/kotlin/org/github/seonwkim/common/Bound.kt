package org.github.seonwkim.common

// TODO: extract Bound implementations to outside of Bound
sealed interface Bound {
    data object Unbounded : Bound
    data class Included(val key: ComparableByteArray) : Bound
    data class Excluded(val key: ComparableByteArray) : Bound
}
