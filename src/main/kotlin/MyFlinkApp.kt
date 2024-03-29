import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import network.subquery.source.SubquerySource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL


object MyFlinkApp {
    private val LOG: Logger = LoggerFactory.getLogger(MyFlinkApp::class.java)

    @JvmStatic
    fun main(args: Array<String>) {

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(1000)
        env.config.registerTypeWithKryoSerializer(BlockTraceOuterClass.BlockTrace::class.java, ProtobufSerializer::class.java)
        env.config.registerTypeWithKryoSerializer(SubquerySource::class.java, ProtobufSerializer::class.java)

        var sourceStream = env.fromSource(
                SubquerySource(
                        URL("https://node-7038644315796209664.sk.onfinality.io/rpc?apikey=1461e43a-4f35-4dc3-95a7-938c274a528a"),
                        0,
                        10,
                ),
                WatermarkStrategy.noWatermarks(),
                "aSource")
                .setParallelism(1)
//        val outputBasePath = Path("./data/")
        val outputBasePath = Path("gs://moss-temp/tracing")
        val config = OutputFileConfig
            .builder()
            .withPartSuffix(".parquet")
            .build()
        var sink = FileSink.forBulkFormat(outputBasePath, ParquetProtoWriters.forType(BlockTraceOuterClass.BlockTrace::class.java))
            .withRollingPolicy( OnCheckpointRollingPolicy.build() )
            .withBucketAssigner(
                CustomBucketAssigner()
            )
            .withOutputFileConfig(config)
            .build()
        sourceStream.sinkTo(sink)

        env.execute("My Flink App")
    }
}


