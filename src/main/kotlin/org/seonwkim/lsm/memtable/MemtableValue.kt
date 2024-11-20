package org.seonwkim.lsm.memtable

import org.seonwkim.common.ComparableByteArray

data class MemtableValue(
    val value: ComparableByteArray,
) {
    fun size(): Int {
        return value.size()
    }
}

fun MemtableValue?.isValid(): Boolean {
    return this != null && !this.value.isEmpty()
}

fun MemtableValue?.isDeleted(): Boolean {
    return this != null && this.value.isEmpty()
}
