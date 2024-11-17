package org.example.lsm.memtable

import org.example.common.ComparableByteArray

data class MemtableValue(
    val value: ComparableByteArray,
    val flag: MemTableValueFlag = MemTableValueFlag.NORMAL
) {
    fun size(): Int {
        return value.size()
    }
}

fun MemtableValue?.isValid(): Boolean {
    return this != null && this.flag != MemTableValueFlag.DELETED
}

fun MemtableValue?.isDeleted(): Boolean {
    return this != null && this.flag == MemTableValueFlag.DELETED
}
