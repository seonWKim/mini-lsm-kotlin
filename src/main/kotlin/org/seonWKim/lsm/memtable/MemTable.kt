package org.seonWKim.lsm.memtable

import org.seonWKim.common.Bound
import org.seonWKim.common.ComparableByteArray
import org.seonWKim.lsm.Wal
import org.seonWKim.lsm.iterator.IteratorFlag
import org.seonWKim.lsm.iterator.IteratorMeta
import org.seonWKim.lsm.iterator.MemTableIterator
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
        map[key] = MemtableValue(value, IteratorMeta(IteratorFlag.NORMAL))
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
