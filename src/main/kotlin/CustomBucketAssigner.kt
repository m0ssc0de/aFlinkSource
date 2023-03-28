import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import kotlin.math.roundToInt

class CustomBucketAssigner<IN> : BucketAssigner<IN, String> {
    override fun getBucketId(element: IN, context: BucketAssigner.Context): String {
        val blockHeight = (element as BlockTraceOuterClass.BlockTrace).blockHeight
        val dividedBy1_000_000 = base_dir + "000" + (blockHeight / 1_000_000).toFloat().roundToInt()
        val dividedBy100_000 = sub_dir + "000" + (blockHeight / 100_000).toFloat().roundToInt()
        return "$dividedBy1_000_000/$dividedBy100_000/$blockHeight"
    }

    override fun getSerializer(): SimpleVersionedSerializer<String> {
        return SimpleVersionedStringSerializer.INSTANCE
    }

    companion object {
        private const val serialVersionUID = 1L
        private const val base_dir = "DividedBy1_000_000="
        private const val sub_dir = "DividedBy100_000="
    }
}