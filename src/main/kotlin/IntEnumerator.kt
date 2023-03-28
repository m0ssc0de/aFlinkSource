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
//            if (state == null) {
//                println("state == null")
//                context!!.assignSplit(IntRangeSplit(updated_from!!, updated_until!!, false), subtaskId)
//                state = EnumeratorState(updated_until!!, listOf())
//            } else {
//                while (state!!.currentUntil == updated_until) {
//                    updateBlockHeight()
//                }
//                context!!.assignSplit(IntRangeSplit(updated_from!!, updated_until!!, false), subtaskId)
//                state!!.currentUntil = updated_until!!
//            }
            when(assigned_until) {
                null -> {
//                    println("assigned updated_from:$updated_from updated_until:$updated_until")
//                    context!!.assignSplit(IntRangeSplit(updated_from!!, updated_until!!, false), subtaskId)
                    assigned_until = updated_until
                    if (state == null) {
                        state = EnumeratorState(updated_until!!, listOf())
                    }
                    state!!.currentUntil = updated_until!!
                }
                else -> {
                    while (assigned_until == updated_until) {
                        updateBlockHeight()
                    }
                    println("assigned assigned_until+1:${assigned_until!!+1} updated_until:$updated_until to $subtaskId")
                    context!!.assignSplit(IntRangeSplit(assigned_until!!+1, updated_until!!, false), subtaskId)
                    assigned_until = updated_until
                    state!!.currentUntil = updated_until!!
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
