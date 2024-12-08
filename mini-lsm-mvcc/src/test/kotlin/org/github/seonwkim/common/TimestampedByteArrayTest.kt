package org.github.seonwkim.common

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TimestampedByteArrayTest {

    @Test
    fun `ComparableByteArray should compare inner UByteArray appropriately`() {
        val arr1 = "abc".toTimestampedByteArray()
        val arr2 = "abcd".toTimestampedByteArray()
        assertTrue { arr1 < arr2 }

        val arr3 = "b".toTimestampedByteArray()
        val arr4 = "a".toTimestampedByteArray()
        assertTrue { arr3 > arr4 }

        val arr5 = "aa".toTimestampedByteArray()
        val arr6 = "aa".toTimestampedByteArray()
        assertTrue { arr5.compareTo(arr6) == 0 }
    }

    @Test
    fun `ComparableByteArray should compare inner UByteArray appropriately 2`() {
        val arr1 = "abcd".toTimestampedByteArray()
        val arr2 = "abc".toTimestampedByteArray()
        val arr3 = "cd".toTimestampedByteArray()
        val sortedList = listOf(arr1, arr2, arr3).sorted()
        assertEquals(sortedList[0], arr2)
        assertEquals(sortedList[1], arr1)
        assertEquals(sortedList[2], arr3)
    }

    @Test
    fun `test`() {
        val array = arrayOf(1, 2, 3)
        val listView = array.asList()

        println(listView) // Output: [1, 2, 3]

        array[0] = 10
        println(listView) // Output: [10, 2, 3]
    }
}
