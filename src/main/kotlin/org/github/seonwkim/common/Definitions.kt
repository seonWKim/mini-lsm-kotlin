package org.github.seonwkim.common

// Size of a `Short` which is 2 bytes in JVM
// TODO: Are there any situations where SIZEOF_U16 is not 2?
const val SIZE_OF_U16_IN_BYTE: Int = Short.SIZE_BYTES
const val SIZE_OF_U32_IN_BYTE: Int = Int.SIZE_BYTES
const val SIZE_OF_U64_IN_BYTE: Int = Long.SIZE_BYTES

const val TS_DEFAULT: Long = 0L
