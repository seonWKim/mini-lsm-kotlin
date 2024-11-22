package org.github.seonwkim.common

import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.Test

class ExtensionsTest {
    @Test
    fun `test rotateLeft`() {
        val int1 = 0b00000000000000000000000000000001u
        val actual1 = int1.rotateLeft(3)
        assertEquals(0b00000000000000000000000000001000u, actual1)

        val int2 = 0b11000000000000000000000000000000u
        val actual2 = int2.rotateLeft(3)
        assertEquals(0b00000000000000000000000000000110u, actual2)

        val int3 = 0b00000000000000000000000000000111u
        val actual3 = int3.rotateLeft(0)
        assertEquals(0b00000000000000000000000000000111u, actual3)
    }
}
