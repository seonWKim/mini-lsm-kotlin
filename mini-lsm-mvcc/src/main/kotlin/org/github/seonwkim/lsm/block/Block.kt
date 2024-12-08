package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.ComparableByteArray

/**
 * A data class representing a block of key-value pairs.
 *
 * @property data the serialized key-value pairs in the block
 * @property offsets the offsets of each key-value entry
 */
data class Block(
    val data: ComparableByteArray,
    val offsets: ComparableByteArray
)
