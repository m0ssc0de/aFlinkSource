package org.example.custom.source

import java.io.Serializable

data class EnumeratorState(var currentUntil: Long, var deadSplits: List<IntRangeSplit> = listOf()): Serializable
