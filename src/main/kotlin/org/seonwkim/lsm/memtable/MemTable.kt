package org.seonwkim.lsm.memtable

import org.seonwkim.common.Bound
import org.seonwkim.common.ComparableByteArray
import org.seonwkim.lsm.Wal
import org.seonwkim.lsm.iterator.IteratorFlag
import org.seonwkim.lsm.iterator.IteratorMeta
import org.seonwkim.lsm.iterator.MemTableIterator
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

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

    private val approximateSize: AtomicInteger = AtomicInteger(0)

    fun get(key: ComparableByteArray): MemtableValue? {
        return map[key]
    }

    fun put(key: ComparableByteArray, value: MemtableValue) {
        if (key.array.isEmpty()) {
            throw IllegalArgumentException("key should not be empty")
        }

        // TODO: what if map already contains same key? should we only add value size?
        approximateSize.addAndGet(key.size() + value.size())
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
        return approximateSize.get()
    }

    fun countEntries(): Int {
        return map.size
    }
}
