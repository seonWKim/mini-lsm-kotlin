package org.seonwkim.lsm.iterator

data class IteratorMeta(
    val flag: IteratorFlag = IteratorFlag.NORMAL
) {

}

enum class IteratorFlag {
    NORMAL,
    DELETED
}
