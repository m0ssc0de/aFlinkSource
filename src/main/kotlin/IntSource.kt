package org.example.custom.source

import BlockTrace
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.connector.source.SourceReader
import org.apache.flink.api.connector.source.SourceReaderContext
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.api.connector.source.SplitEnumeratorContext
import org.apache.flink.core.io.SimpleVersionedSerializer

class IntSource : Source<BlockTraceOuterClass.BlockTrace, IntRangeSplit, EnumeratorState> {

    override fun getBoundedness(): Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

//    override fun createReader(readerContext: SourceReaderContext): SourceReader<BlockTraceOuterClass.BlockTrace, IntRangeSplit> = IntRangeReader(readerContext)
    override fun createReader(readerContext: SourceReaderContext): SourceReader<BlockTraceOuterClass.BlockTrace, IntRangeSplit> = IntRangeReader(readerContext)

    override fun createEnumerator(enumContext: SplitEnumeratorContext<IntRangeSplit>): SplitEnumerator<IntRangeSplit, EnumeratorState> = IntEnumerator(enumContext)

    // Enumerator is initialized with previous enumerator state.
    override fun restoreEnumerator(enumContext: SplitEnumeratorContext<IntRangeSplit>, checkpoint: EnumeratorState): SplitEnumerator<IntRangeSplit, EnumeratorState> = IntEnumerator(enumContext, checkpoint)

    override fun getSplitSerializer(): SimpleVersionedSerializer<IntRangeSplit> = SimpleSerializer()

    override fun getEnumeratorCheckpointSerializer(): SimpleVersionedSerializer<EnumeratorState> = SimpleSerializer()
}
