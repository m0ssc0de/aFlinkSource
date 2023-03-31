package org.example.custom.source

import org.apache.flink.api.connector.source.SourceSplit
import java.io.Serializable
import java.net.URL


@kotlinx.serialization.Serializable
class IntRangeSplit(val from: Long, val until: Long, var url: String, var done: Boolean) : SourceSplit, Serializable {
    //TODO block hash of 1 or 2 blocks
    override fun splitId(): String {
        return "${from}_${until}_${url}"
    }
}
