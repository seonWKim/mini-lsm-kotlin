package org.example.lsm.memtable

import org.example.common.ComparableByteArray
import org.example.lsm.iterator.IteratorFlag
import org.example.lsm.iterator.IteratorMeta

data class MemtableValue(
    val value: ComparableByteArray,
    val meta: IteratorMeta? = null
) {
    fun size(): Int {
        return value.size()
    }
}

fun MemtableValue?.isValid(): Boolean {
    return this != null && this.meta?.flag != IteratorFlag.DELETED
}

fun MemtableValue?.isDeleted(): Boolean {
    return this != null && this.meta?.flag == IteratorFlag.DELETED
}
