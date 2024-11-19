package org.seonwkim.lsm.block

import org.seonwkim.common.ComparableByteArray

data class Block(
    val data: ComparableByteArray,
    val offsets: ComparableByteArray
)
