package org.github.seonwkim.lsm.block

import org.github.seonwkim.common.ComparableByteArray

data class Block(
    val data: ComparableByteArray,
    val offsets: ComparableByteArray
)
