package org.example.custom.source

import BlockTrace
import NodeClient
import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.core.io.InputStatus
import java.net.URL

import java.util.List
import java.util.concurrent.CompletableFuture

class IntRangeReader(private val context: SourceReaderContext) : SourceReader<BlockTraceOuterClass.BlockTrace, IntRangeSplit> {
    private var currentSplit: IntRangeSplit? = null
    private var availability: CompletableFuture<Void> = CompletableFuture.completedFuture(null)

    private var nodeClient: NodeClient? = null


    override fun start() {
        //TODO url come with split
        val url = URL("https://node-7038644315796209664.sk.onfinality.io/rpc?apikey=1461e43a-4f35-4dc3-95a7-938c274a528a")
        this.nodeClient = NodeClient(url)
    }

    override fun pollNext(output: ReaderOutput<BlockTraceOuterClass.BlockTrace>): InputStatus {

        if (currentSplit != null && !currentSplit!!.done) {
            println("FROM ${currentSplit!!.from} UNTIL ${currentSplit!!.until}")
            for (i in currentSplit!!.from..currentSplit!!.until) {
                println("fetching $i")
                var t : BlockTraceOuterClass.BlockTrace = BlockTraceOuterClass.BlockTrace.newBuilder().build()
                try {
                    t = nodeClient!!.getTracing(nodeClient!!.getBlockHash(i))
                } catch (e: Exception) {
                    println("Exception caught: ${e.javaClass.simpleName}: ${e.message}")
                    e.printStackTrace()
                    throw e
                } finally {
                    println("HASH ${t.blockHash}")
                }
//                t.blockHeight = i
                try {
//                    t.blockHeight = i
//                    output.collect(t)
                    output.collect(BlockTraceOuterClass.BlockTrace.newBuilder(t).setBlockHeight(i).build())
                } catch (e: Exception) {
                    println("Collect Exception caught: ${e.javaClass.simpleName}: ${e.message}")
                    e.printStackTrace()
                    throw e
                } finally {
                    println("HASH ${t.blockHash}")
                }
//                output.collect(t)
//                println("COLLECT $i ${t.eventsCount}")
                println("COLLECT $i")
            }
            currentSplit!!.done = true
            if (availability.isDone) {
                availability = CompletableFuture()
                context.sendSplitRequest()
            }
            return InputStatus.NOTHING_AVAILABLE
//            return InputStatus.END_OF_INPUT
        }
//        if (currentSplit != null && !currentSplit!!.done) {
//            for (i in currentSplit!!.from..currentSplit!!.until) {
//                val t = nodeClient!!.getTracing(nodeClient!!.getBlockHash(i))
//                output.collect(t)
//                currentSplit!!.done = true
//            }
//            return InputStatus.NOTHING_AVAILABLE
//        } else {
//            if (availability.isDone) {
//                availability = CompletableFuture()
//                context.sendSplitRequest()
//            }
//            return InputStatus.END_OF_INPUT
//        }
        if (availability.isDone) {
            availability = CompletableFuture()
            context.sendSplitRequest()
        }
        return InputStatus.NOTHING_AVAILABLE
    }

    override fun snapshotState(checkpointId: Long): MutableList<IntRangeSplit>? {
        return listOf(currentSplit!!).toMutableList()
    }

    override fun isAvailable(): CompletableFuture<Void> {
        return availability
    }

    override fun addSplits(splits: MutableList<IntRangeSplit>?) {
        // One split is assigned per task in enumerator therefore we only use the first index.
        currentSplit = splits?.get(0)
        // Data availability is over since we got a split.
        availability.complete(null)
    }

    override fun notifyNoMoreSplits() {
        // Not implemented since we expect our source to be boundless.
    }

    override fun close() {}
}
