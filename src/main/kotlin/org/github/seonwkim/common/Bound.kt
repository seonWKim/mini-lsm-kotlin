package org.github.seonwkim.common

class Bound(
    val value: ComparableByteArray,
    val flag: BoundFlag = BoundFlag.EXCLUDED
) {
    companion object {
        fun unbounded(): Bound {
            return Bound(ComparableByteArray.new(), BoundFlag.UNBOUNDED)
        }
    }
}

enum class BoundFlag {
    EXCLUDED,
    INCLUDED,
    UNBOUNDED,
}
