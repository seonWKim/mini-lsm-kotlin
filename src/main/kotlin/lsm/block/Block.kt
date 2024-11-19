package org.seonWKim.lsm.block

import org.seonWKim.common.ComparableByteArray
import org.seonWKim.common.SIZE_OF_U16_IN_BYTE
import org.seonWKim.common.toU16ByteArray
import org.seonWKim.common.toU16Int

class Block(
    val data: ComparableByteArray,
    val offsets: ComparableByteArray
) {
    companion object {
        fun decode(data: ComparableByteArray): Block {
            val entryOffsetLength = data.slice(data.size() - SIZE_OF_U16_IN_BYTE, data.size()).toU16Int()

            val dataEnd = data.size() - SIZE_OF_U16_IN_BYTE - entryOffsetLength * SIZE_OF_U16_IN_BYTE
            return Block(
                data = data.slice(0, dataEnd),
                offsets = data.slice(dataEnd, data.size() - SIZE_OF_U16_IN_BYTE)
            )
        }
    }

    fun encode(): ComparableByteArray {
        val offsetLength = offsets.size() / SIZE_OF_U16_IN_BYTE
        return data + offsets + offsetLength.toU16ByteArray()
    }
}
