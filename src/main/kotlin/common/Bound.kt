package org.example.common

class Bound(
    val value: ComparableByteArray,
    val flag: BoundFlag = BoundFlag.NOT_INCLUDED
) {
    companion object {
        fun unbounded(): Bound {
            return Bound(ComparableByteArray.EMPTY, BoundFlag.UNBOUNDED)
        }
    }
}

enum class BoundFlag {
    NOT_INCLUDED,
    INCLUDED,
    UNBOUNDED,
}
