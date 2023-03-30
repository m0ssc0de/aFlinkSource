package org.example.custom.source

import NodeClient
import kotlinx.coroutines.runInterruptible
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.w3c.dom.Node
import java.net.URL
import kotlin.concurrent.thread

class IntEnumerator : SplitEnumerator<IntRangeSplit, EnumeratorState> {
    private var updated_from: Long? = null
    private var updated_until: Long? = null
    private var assigned_until: Long? = null

    private val max_batch_size: Long = 1// TODO: 1. init from constructor 2. concurrent processing in reader

    private var nodeClient: NodeClient

    private var state: EnumeratorState? = null
    private var context:SplitEnumeratorContext<IntRangeSplit>?  = null

    constructor(context: SplitEnumeratorContext<IntRangeSplit>, state: EnumeratorState)  {
        this.context = context
        this.state = state
        val url = URL("https://node-7038644315796209664.sk.onfinality.io/rpc?apikey=1461e43a-4f35-4dc3-95a7-938c274a528a")
        this.nodeClient = NodeClient(url)
    }
    constructor(context: SplitEnumeratorContext<IntRangeSplit>): this(context, EnumeratorState(0))

    override fun start() {
        if ((updated_from == null) || (updated_until == null )) {
            updated_from = nodeClient.getBlockHeight(nodeClient.getFinalized())
            updated_until = updated_from
        }
    }

    private fun updateBlockHeight() {
            val newBlockHeight = nodeClient.getBlockHeight(nodeClient.getFinalized())
            if (newBlockHeight > updated_until!!) {
                updated_until = newBlockHeight.toLong()
            }
    }


    override fun handleSplitRequest(subtaskId: Int, requesterHostname: String?) {
        updateBlockHeight()
        // returned splits are prioritized
        if (state!!.deadSplits.isNotEmpty()) {
            val split = state!!.deadSplits[0]
            state!!.deadSplits = state!!.deadSplits.drop(1)
            println("assignSplit $split")
            context!!.assignSplit(split, subtaskId)
        } else {
            when(assigned_until) {
                null -> {
                    val from = updated_from
                    val until = if (from?.plus(this.max_batch_size-1)!! <= updated_until!!) {
                        from?.plus(this.max_batch_size-1)
                    } else {
                        updated_until
                    }
                    context!!.assignSplit(IntRangeSplit(from!!, until!!, false), subtaskId)
                    assigned_until = until
                    if (state == null) {
                        state = EnumeratorState(assigned_until!!, listOf())
                    }
                    state!!.currentUntil = assigned_until!!
                }
                else -> {
                    while (assigned_until == updated_until) {
                        updateBlockHeight()
                    }
                    val from = assigned_until!!+1
                    val until = if (from?.plus(this.max_batch_size-1)!! <= updated_until!!) {
                        from?.plus(this.max_batch_size-1)
                    } else {
                        updated_until
                    }
                    context!!.assignSplit(IntRangeSplit(from, until!!, false), subtaskId)
                    assigned_until = until
                    state!!.currentUntil = assigned_until!!
                }
            }
        }
    }

    override fun addSplitsBack(splits: MutableList<IntRangeSplit>?, subtaskId: Int) {
        println("BACK from $subtaskId")
        if (splits?.isNotEmpty() == true) {
            println("SIZE ${splits.size}")
            println("SPLITS0 ${splits[0].splitId()}")
            println("SPLITS1 ${splits[1].splitId()}")
            state!!.deadSplits = splits + state!!.deadSplits
        }
    }

    override fun addReader(subtaskId: Int) {}

    override fun snapshotState(checkpointId: Long): EnumeratorState = state!!

    override fun close() {}
}
