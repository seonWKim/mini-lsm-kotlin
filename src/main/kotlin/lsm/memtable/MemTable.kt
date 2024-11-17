package org.example.lsm.memtable

import org.example.common.Bound
import org.example.common.ComparableByteArray
import org.example.lsm.Wal
import org.example.lsm.iterator.MemTableIterator
import java.util.concurrent.ConcurrentSkipListMap

typealias MemTableKey = ComparableByteArray

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

    fun iterator(lower: Bound, upper: Bound): MemTableIterator {
        return MemTableIterator(this, lower, upper)
    }

    fun forTestingGetSlice(key: ComparableByteArray): MemtableValue? {
        return map[key]
    }

    fun forTestingPutSlice(key: ComparableByteArray, value: ComparableByteArray) {
        map[key] = MemtableValue(value, ValueFlag.NORMAL)
    }

    fun forTestingScanSlice(lower: Bound, upper: Bound): MemTableIterator {
        return MemTableIterator(this, lower, upper)
    }

    fun approximateSize(): Int {
        return approximateSize
    }

    fun countEntries(): Int {
        return map.size
    }
}
