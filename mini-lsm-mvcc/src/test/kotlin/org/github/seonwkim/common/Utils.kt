package org.github.seonwkim.common

import org.opentest4j.AssertionFailedError

fun assertByteArrayEquals(
    expected: TimestampedByteArray?,
    actual: TimestampedByteArray?,
    message: String? = null
): Boolean {
    return assertByteArrayEquals(
        expected?.getByteArray()?.toComparableByteArray(),
        actual?.getByteArray()?.toComparableByteArray(),
        message
    )
}


fun assertByteArrayEquals(
    expected: ComparableByteArray?,
    actual: ComparableByteArray?,
    message: String? = null
): Boolean {
    if (expected != actual) {
        if (message != null) {
            throw AssertionFailedError(message)
        } else {
            throw AssertionFailedError("expected($expected) != actual($actual)")
        }
    }

    return true
}
