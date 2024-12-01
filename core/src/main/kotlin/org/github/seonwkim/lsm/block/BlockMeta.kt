package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.TimestampedKey

/**
 * A data class representing metadata for a data block.
 *
 * @property offset the offset of this data block
 * @property firstKey the first key of the data block
 * @property lastKey the last key of the data block
 */
data class BlockMeta(
    val offset: Int,
    val firstKey: TimestampedKey,
    val lastKey: TimestampedKey
)
