package org.example

fun String.toComparableByteArray(): ComparableByteArray {
    return ComparableByteArray(this.toByteArray())
}
