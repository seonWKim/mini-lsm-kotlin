package org.github.seonwkim.common

sealed interface BoundV2 {
    data object Unbounded : BoundV2
    data class Included(val value: ComparableByteArray) : BoundV2
    data class Excluded(val value: ComparableByteArray) : BoundV2
}
