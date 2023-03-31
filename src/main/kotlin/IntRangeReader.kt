package org.example.custom.source

import BlockTrace
import NodeClient
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import org.apache.flink.api.connector.source.ReaderOutput
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.core.io.InputStatus
import java.net.URL

import java.util.List
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.system.measureTimeMillis

class IntRangeReader(private val context: SourceReaderContext) : SourceReader<BlockTraceOuterClass.BlockTrace, IntRangeSplit> {
    private var currentSplit: IntRangeSplit? = null
    private var availability: CompletableFuture<Void> = CompletableFuture.completedFuture(null)

    private var nodeClient: NodeClient? = null


    override fun start() {
        this.nodeClient = NodeClient()
    }

    override fun pollNext(output: ReaderOutput<BlockTraceOuterClass.BlockTrace>): InputStatus {

        if (currentSplit != null && !currentSplit!!.done) {
            nodeClient?.updateURL(URL(currentSplit!!.url))
            println("Reader execu FROM ${currentSplit!!.from} UNTIL ${currentSplit!!.until}")
            var responsTimeHistory = ConcurrentLinkedQueue<Long>()
            var maxConcurrency = 100 // initial value for maxConcurrency
            val semaphore = Semaphore(maxConcurrency)
            val mutex = Mutex()
            val job = GlobalScope.launch {
                coroutineScope {
                    (currentSplit!!.from.. currentSplit!!.until).toList().map {blockHeight ->
                        launch {
                            semaphore.acquire()
                            var traceBlock = BlockTraceOuterClass.BlockTrace.newBuilder().build()
                            try{
                                val responseTime = measureTimeMillis {
                                    var t = nodeClient!!.getTracing(nodeClient!!.getBlockHash(blockHeight))
                                    traceBlock = BlockTraceOuterClass.BlockTrace.newBuilder(t).setBlockHeight(blockHeight).build()
                                }
                                responsTimeHistory.add(responseTime)// TODO: dynamic maxConcurrency base on the this value
                            } catch (e: Exception) {
                                println("Fetching Exception caught: ${e.javaClass.simpleName}: ${e.message}")
                                e.printStackTrace()
                                throw e
                            } finally {
                            }

                            try {
                                mutex.withLock {
                                    output.collect(traceBlock)
                                }
                            } catch (e: Exception) {
                                println("Collecting Exception caught: ${e.javaClass.simpleName}: ${e.message}")
                                e.printStackTrace()
                                throw e
                            } finally {
                            }
                        }
                    }.joinAll()
                }
            }
            runBlocking {
                job.join() // Wait for the job to finish
                println("3 after joinAll")
                currentSplit!!.done = true
                if (availability.isDone) {
                    availability = CompletableFuture()
                    context.sendSplitRequest()
                }
                return@runBlocking InputStatus.NOTHING_AVAILABLE
            }
        }
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
