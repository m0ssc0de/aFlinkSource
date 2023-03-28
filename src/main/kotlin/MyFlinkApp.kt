import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.example.custom.source.IntSource
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object MyFlinkApp {
    private val LOG: Logger = LoggerFactory.getLogger(MyFlinkApp::class.java)

    @JvmStatic
    fun main(args: Array<String>) {

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.enableCheckpointing(10);
        env.config.registerTypeWithKryoSerializer(BlockTraceOuterClass.BlockTrace::class.java, ProtobufSerializer::class.java)

        var sourceStream = env.fromSource(IntSource(),
                WatermarkStrategy.noWatermarks(),
                "aSource")
                .setParallelism(1)
//                .print()
//                .setParallelism(1)
        var outputBasePath = Path("./data/");
        val config = OutputFileConfig
                .builder()
                .withPartPrefix("sdjalksdaslkdj")
                .withPartSuffix(".parquet")
                .build()
        var sink = FileSink.forBulkFormat(outputBasePath, ParquetProtoWriters.forType(BlockTraceOuterClass.BlockTrace::class.java))
                .withRollingPolicy( OnCheckpointRollingPolicy.build() )
                .withOutputFileConfig(config)
                .build()
        sourceStream.sinkTo(sink)

        env.execute("My Flink App")
    }
}

