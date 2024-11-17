package org.example.lsm.block

data class Block(
    val data: List<Byte>,
    val offset: List<Byte>
)
