package lsm

import org.example.lsm.ComparableByteArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ComparableByteArrayTest {

    @Test
    fun `ComparableByteArray should compare inner ByteArray appropriately`() {
        val arr1 = ComparableByteArray("abc".toByteArray())
        val arr2 = ComparableByteArray("abcd".toByteArray())
        assertTrue { arr1 < arr2 }

        val arr3 = ComparableByteArray("b".toByteArray())
        val arr4 = ComparableByteArray("a".toByteArray())
        assertTrue { arr3 > arr4 }

        val arr5 = ComparableByteArray("aa".toByteArray())
        val arr6 = ComparableByteArray("aa".toByteArray())
        assertTrue { arr5.compareTo(arr6) == 0 }
    }

    @Test
    fun `ComparableByteArray should compare inner ByteArray appropriately 2`() {
        val arr1 = ComparableByteArray("abcd".toByteArray())
        val arr2 = ComparableByteArray("abc".toByteArray())
        val arr3 = ComparableByteArray("cd".toByteArray())
        val sortedList = listOf(arr1, arr2, arr3).sorted()
        assertEquals(sortedList[0], arr2)
        assertEquals(sortedList[1], arr1)
        assertEquals(sortedList[2], arr3)
    }
}
