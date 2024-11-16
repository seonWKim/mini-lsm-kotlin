package org.example.common

class Bound(
    val value: ComparableByteArray
) {
    companion object {
        // unlimited bound(can be either direction)
        val UNBOUNDED = Bound(ComparableByteArray.EMPTY)
    }
}
