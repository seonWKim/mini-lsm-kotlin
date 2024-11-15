package org.example

import java.util.concurrent.ConcurrentSkipListMap

typealias MemTableKey = ComparableByteArray

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

enum class MemtableValueFlag {
    NORMAL,
    DELETED
}

class MemTable(
    val map: ConcurrentSkipListMap<MemTableKey, MemtableValue>,
    val wal: Wal?,
    val id: Int,
) {
    companion object {
        fun create(id: Int): MemTable {
            return MemTable(
                map = ConcurrentSkipListMap(),
                wal = Wal.default(),
                id = id
            )
        }
    }

    private var approximateSize: Int = 0

    fun get(key: ComparableByteArray): MemtableValue? {
        return map[key]
    }

    fun put(key: ComparableByteArray, value: MemtableValue) {
        if (key.array.isEmpty()) {
            throw IllegalArgumentException("key should not be empty")
        }
        approximateSize += key.size() + value.size()
        map[key] = value
    }

    fun delete(key: ComparableByteArray) {
        val value = map[key]
        if (value != null) {
            approximateSize -= key.size() + value.size()
            map[key] = MemtableValue(ComparableByteArray.empty(), MemtableValueFlag.DELETED)
        }
    }

    fun forTestingGetSlice(key: ComparableByteArray): MemtableValue? {
        return map[key]
    }

    fun forTestingPutSlice(key: ComparableByteArray, value: ComparableByteArray) {
        map[key] = MemtableValue(value, MemtableValueFlag.NORMAL)
    }

    fun approximateSize(): Int {
        return approximateSize
    }

    fun countEntries(): Int {
        return map.size
    }
}
