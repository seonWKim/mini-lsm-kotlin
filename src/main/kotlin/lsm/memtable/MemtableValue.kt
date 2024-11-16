package org.example.lsm.memtable

import org.example.lsm.ComparableByteArray

data class MemtableValue(
    val value: ComparableByteArray,
    val flag: MemtableValueFlag
) {
    fun size(): Int {
        return value.size()
    }
}

fun MemtableValue?.isValid(): Boolean {
    return this != null && this.flag != MemtableValueFlag.DELETED
}

fun MemtableValue?.isDeleted(): Boolean {
    return this != null && this.flag == MemtableValueFlag.DELETED
}
