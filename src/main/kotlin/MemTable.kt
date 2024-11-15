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
                id = 0
            )
        }
    }

    private var approximateSize: Int = 0

    fun forTestingPutSlice(key: ComparableByteArray, value: ComparableByteArray) {
        map[key] = value
    }

    fun forTestingGetSlice(key: ComparableByteArray): ComparableByteArray? {
        return map[key]
    }

    fun approximateSize(): Int {
        TODO()
    }
}
