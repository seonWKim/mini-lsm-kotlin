package org.seonWKim.lsm.memtable

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.lsm.iterator.IteratorFlag
import org.seonWKim.lsm.iterator.IteratorMeta

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
