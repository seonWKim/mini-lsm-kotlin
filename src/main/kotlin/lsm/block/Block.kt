package org.example.lsm.block

import org.example.common.SIZEOF_U16
import org.example.common.toInt
import org.example.common.toU16

class Block(
    val data: List<UByte>,
    val offset: List<UByte>
) {
    companion object {
        fun decode(data: List<UByte>): Block {
            val entryOffsetLength = data.subList(data.size - SIZEOF_U16, data.size).toInt()

            val dataEnd = data.size - SIZEOF_U16 - entryOffsetLength * SIZEOF_U16
            return Block(
                data = data.subList(0, dataEnd),
                offset = data.subList(dataEnd, data.size - SIZEOF_U16)
            )
        }
    }

    fun encode(): List<UByte> {
        val offsetLength = offset.size
        return data + offset + offsetLength.toU16()
    }
}
