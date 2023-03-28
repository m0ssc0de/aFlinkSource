//import org.apache.flink.api.connector.source.SourceReaderContext
//import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds
//import org.apache.flink.connector.base.source.reader.SourceReaderBase
//import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue
//import org.example.custom.source.EnumeratorState
//import org.example.custom.source.IntRangeSplit
//
//class IntRangeReaderN(readerContext: SourceReaderContext) : SourceReaderBase<IntRangeSplit, BlockTraceOuterClass.BlockTrace, IntRangeSplit, EnumeratorState>(elementsQueue) {
//    override fun onSplitFinished(p0: MutableMap<String, EnumeratorState>?) {
//        println("onSplitFinished")
//        TODO("Not yet implemented")
//    }
//
//    override fun initializedState(p0: IntRangeSplit?): EnumeratorState {
//        println("initializedState")
//        TODO("Not yet implemented")
//    }
//
//    override fun toSplitType(p0: String?, p1: EnumeratorState?): IntRangeSplit {
//        println("toSplitType")
//        TODO("Not yet implemented")
//    }
//}