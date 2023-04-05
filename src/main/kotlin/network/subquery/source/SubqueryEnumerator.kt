package network.subquery.source

import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import java.net.URL

class SubqueryEnumerator : SplitEnumerator<SubquerySplit, EnumeratorState> {
    private var updated_from: Long? = null
    private var updated_until: Long? = null
    private var assigned_until: Long? = null

    private var max_batch_size: Long = 10// TODO: -1. init from constructor 2. concurrent processing in reader

    private var nodeClient: NodeClient
    private var url: URL

    private var state: EnumeratorState? = null
    private var context:SplitEnumeratorContext<SubquerySplit>?  = null

    constructor(context: SplitEnumeratorContext<SubquerySplit>, state: EnumeratorState, url: URL = URL(""))  {
        this.context = context
        this.state = state
        this.url = url
        this.nodeClient = NodeClient(url)
    }

    constructor(context: SplitEnumeratorContext<SubquerySplit>, url: URL, from: Long): this(context, EnumeratorState((0)), url)  {
        this.updated_from = from
    }
    constructor(context: SplitEnumeratorContext<SubquerySplit>) :this(context, EnumeratorState(0))
    constructor(context: SplitEnumeratorContext<SubquerySplit>, url: URL, from:Long, max_batch_size: Long) :this(context, url, from) {
        this.max_batch_size = max_batch_size
    }

    constructor(context: SplitEnumeratorContext<SubquerySplit>, url: URL):this(context, EnumeratorState(0), url)


    override fun start() {
        if (updated_until == null) {
            updated_until = nodeClient.getBlockHeight(nodeClient.getFinalized())
        }
        if (updated_from == null) {
            updated_from = updated_until
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
                        from.plus(this.max_batch_size-1)
                    } else {
                        updated_until
                    }
                    context!!.assignSplit(SubquerySplit(from!!, until!!, url.toString(), false), subtaskId)
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
                    context!!.assignSplit(SubquerySplit(from, until!!,url.toString(), false), subtaskId)
                    assigned_until = until
                    state!!.currentUntil = assigned_until!!
                }
            }
        }
    }

    override fun addSplitsBack(splits: MutableList<SubquerySplit>?, subtaskId: Int) {
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
