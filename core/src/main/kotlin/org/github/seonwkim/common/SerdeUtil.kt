package org.github.seonwkim.common

import com.fasterxml.jackson.databind.ObjectMapper

object SerdeUtil {
    val objectMapper = ObjectMapper()

    fun serialize(value: Any): ByteArray {
        return objectMapper.writeValueAsBytes(value)
    }

    inline fun <reified T> deserialize(bytes: ByteArray): T {
        return objectMapper.readValue(bytes, T::class.java)
    }
}
