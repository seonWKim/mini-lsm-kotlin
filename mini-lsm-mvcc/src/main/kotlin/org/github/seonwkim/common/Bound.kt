package org.github.seonwkim.common

/**
 * Represents a boundary in a range.
 */
sealed interface Bound

/**
 * Represents an unbounded range.
 */
data object Unbounded : Bound

/**
 * Represents a range that includes the specified key.
 *
 * @property key the key that is included in the range
 */
data class Included(val key: TimestampedByteArray) : Bound

/**
 * Represents a range that excludes the specified key.
 *
 * @property key the key that is excluded from the range
 */
data class Excluded(val key: TimestampedByteArray) : Bound
