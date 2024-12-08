package org.github.seonwkim.lsm.memtable

import org.github.seonwkim.common.Bound
import org.github.seonwkim.common.TimestampedByteArray
import org.github.seonwkim.lsm.Wal
import org.github.seonwkim.lsm.iterator.MemTableIterator
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Represents an in-memory table (MemTable) in the LSM storage system.
 *
 * @property id The unique identifier for this MemTable.
 * @property map The underlying data structure that stores the key-value pairs.
 * @property wal The Write-Ahead Log (WAL) associated with this MemTable.
 */
class MemTable(
    private val id: Int,
    private val map: ConcurrentSkipListMap<TimestampedByteArray, TimestampedByteArray>,
    private val wal: Wal?,
    private val approximateSize: AtomicInteger = AtomicInteger(0)
) {
    companion object {
        fun create(id: Int): MemTable {
            return MemTable(
                id = id,
                map = ConcurrentSkipListMap(),
                wal = null,
            )
        }

        fun createWithWal(id: Int, path: Path): MemTable {
            return MemTable(
                id = id,
                map = ConcurrentSkipListMap(),
                wal = Wal.create(path),
            )
        }

        fun recoverFromWal(id: Int, path: Path): MemTable {
            val map = ConcurrentSkipListMap<TimestampedByteArray, TimestampedByteArray>()
            val wal = Wal.recover(path, map)
            var approximateSize = 0
            map.forEach { (key, value) -> approximateSize += (key.size() + value.size()) }
            return MemTable(
                id = id,
                map = map,
                wal = wal,
                approximateSize = AtomicInteger(approximateSize)
            )
        }
    }

    fun id(): Int {
        return id
    }

    fun get(key: TimestampedByteArray): TimestampedByteArray? {
        return map[key]
    }

    fun put(key: TimestampedByteArray, value: TimestampedByteArray) {
        if (key.isEmpty()) {
            throw IllegalArgumentException("key should not be empty")
        }

        // TODO: what if map already contains same key? should we only add value size?
        map[key] = value
        approximateSize.addAndGet(key.size() + value.size())
        wal?.put(key, value)
    }

    fun iterator(lower: Bound, upper: Bound): MemTableIterator {
        return MemTableIterator.create(this, lower, upper)
    }

    fun forTestingGetSlice(key: TimestampedByteArray): TimestampedByteArray? {
        return get(key)
    }

    fun forTestingPutSlice(key: TimestampedByteArray, value: TimestampedByteArray) {
        put(key, value)
    }

    fun forTestingScanSlice(lower: Bound, upper: Bound): MemTableIterator {
        return MemTableIterator.create(this, lower, upper)
    }

    fun isEmpty(): Boolean {
        return entriesSize() == 0
    }

    fun entriesSize(): Int {
        return map.size
    }

    fun approximateSize(): Int {
        return approximateSize.get()
    }

    fun flush(builder: SsTableBuilder) {
        for ((key, value) in map.entries) {
            builder.add(key, value)
        }
    }

    fun syncWal() {
        wal?.sync()
    }

    fun copy(): MemTable {
        return MemTable(
            map = ConcurrentSkipListMap<TimestampedByteArray, TimestampedByteArray>(map),
            wal = wal,
            id = id
        )
    }

    fun iterator(): MutableIterator<MutableMap.MutableEntry<TimestampedByteArray, TimestampedByteArray>> {
        return this.map.iterator()
    }
}
