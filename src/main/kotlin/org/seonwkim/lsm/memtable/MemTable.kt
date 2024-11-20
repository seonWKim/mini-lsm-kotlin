package org.seonwkim.lsm.memtable

import org.seonwkim.common.Bound
import org.seonwkim.common.ComparableByteArray
import org.seonwkim.common.TimestampedKey
import org.seonwkim.lsm.Wal
import org.seonwkim.lsm.iterator.MemTableIterator
import org.seonwkim.lsm.sstable.SsTableBuilder
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

class MemTable(
    val map: ConcurrentSkipListMap<TimestampedKey, MemtableValue>,
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
        return get(TimestampedKey(key))
    }

    fun get(key: TimestampedKey): MemtableValue? {
        return map[key]
    }

    fun put(key: ComparableByteArray, value: MemtableValue) {
        put(TimestampedKey(key), value)
    }

    fun put(key: TimestampedKey, value: MemtableValue) {
        if (key.isEmpty()) {
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
        return get(TimestampedKey(key))
    }

    fun forTestingPutSlice(key: ComparableByteArray, value: ComparableByteArray) {
        put(TimestampedKey(key), MemtableValue(value))
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

    fun flush(builder: SsTableBuilder) {
        for ((key, value) in map.entries) {
            builder.add(key, value.value)
        }
    }
}
