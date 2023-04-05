package network.subquery.source

import network.subquery.source.SubquerySplit
import java.io.Serializable

data class EnumeratorState(var currentUntil: Long, var deadSplits: List<SubquerySplit> = listOf()): Serializable
