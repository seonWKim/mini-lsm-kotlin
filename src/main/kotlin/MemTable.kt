package org.example

import java.util.concurrent.ConcurrentSkipListMap

class MemTable(
    val map: ConcurrentSkipListMap<ComparableByteArray, ComparableByteArray>,
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

    fun get(key: ComparableByteArray): ComparableByteArray? {
        return map[key]
    }

    fun put(key: ComparableByteArray, value: ComparableByteArray) {
        approximateSize += key.size() + value.size()
        map[key] = value
    }

    fun delete(key: ComparableByteArray) {
        val value = map[key]
        if (value != null) {
            approximateSize -= key.size() + value.size()
            map.remove(key)
        }
    }

    fun forTestingGetSlice(key: ComparableByteArray): ComparableByteArray? {
        return map[key]
    }

    fun forTestingPutSlice(key: ComparableByteArray, value: ComparableByteArray) {
        map[key] = value
    }

    fun approximateSize(): Int {
        return approximateSize
    }
}
