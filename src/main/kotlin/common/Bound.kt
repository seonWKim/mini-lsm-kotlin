package org.seonWKim.common

class Bound(
    val value: ComparableByteArray,
    val flag: BoundFlag = BoundFlag.EXCLUDED
) {
    companion object {
        fun unbounded(): Bound {
            return Bound(ComparableByteArray.EMPTY, BoundFlag.UNBOUNDED)
        }
    }
}

enum class BoundFlag {
    EXCLUDED,
    INCLUDED,
    UNBOUNDED,
}
