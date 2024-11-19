package org.seonwkim.lsm.memtable

import org.seonwkim.common.ComparableByteArray
import org.seonwkim.lsm.iterator.IteratorFlag
import org.seonwkim.lsm.iterator.IteratorMeta

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
