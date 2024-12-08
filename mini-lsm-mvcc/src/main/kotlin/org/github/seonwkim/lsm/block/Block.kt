package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.TimestampedByteArray

/**
 * A data class representing a block of key-value pairs.
 *
 * @property data the serialized key-value pairs in the block
 * @property offsets the offsets of each key-value entry
 */
data class Block(
    val data: TimestampedByteArray,
    val offsets: TimestampedByteArray
)
