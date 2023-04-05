package network.subquery.source

import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.core.io.SimpleVersionedSerializer
import java.net.URL

class SubquerySource : Source<BlockTraceOuterClass.BlockTrace, SubquerySplit, EnumeratorState> {
    private lateinit var url:URL
    private var urls = mutableListOf<URL>()
    private var from: Long? = null
    private var maxBatchSize: Long? = null

    constructor(url: URL): this() {
        this.url = url
    }

    constructor(urls: MutableList<URL>): this() {
        this.urls = urls
    }

    constructor(url: URL, from: Long): this(url){
        this.from = from
    }

    constructor(url: URL, from: Long, maxBatchSize: Long): this(url, from) {
        this.maxBatchSize = maxBatchSize
    }

    constructor()

    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

//    override fun createReader(readerContext: SourceReaderContext): SourceReader<BlockTraceOuterClass.BlockTrace, IntRangeSplit> = IntRangeReader(readerContext)
    override fun createReader(readerContext: SourceReaderContext): SourceReader<BlockTraceOuterClass.BlockTrace, SubquerySplit> = SubqueryReader(readerContext)

    override fun createEnumerator(enumContext: SplitEnumeratorContext<SubquerySplit>): SplitEnumerator<SubquerySplit, EnumeratorState> {
        println("createEnumerator")
        if (this.from != null && this.maxBatchSize != null) {
            println("createEnumerator from non-null")
            return SubqueryEnumerator(enumContext, url, this.from!!, this.maxBatchSize!!)
        } else {
            println("createEnumerator from null ${this.from}, ${this.maxBatchSize}")
            return SubqueryEnumerator(enumContext, url)
        }
    }

    // Enumerator is initialized with previous enumerator state.
    override fun restoreEnumerator(enumContext: SplitEnumeratorContext<SubquerySplit>, checkpoint: EnumeratorState): SplitEnumerator<SubquerySplit, EnumeratorState> = SubqueryEnumerator(enumContext, checkpoint)

    override fun getSplitSerializer(): SimpleVersionedSerializer<SubquerySplit> = SimpleSerializer()

    override fun getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer<EnumeratorState> = SimpleSerializer()
}
