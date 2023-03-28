import com.google.gson.Gson
import com.google.protobuf.util.JsonFormat
import kotlinx.serialization.Serializable
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.net.URL
import java.util.concurrent.TimeUnit


class NodeClient {
//    val url: URL? = null
    private var url: URL? = URL("https://node-7038644315796209664.sk.onfinality.io/rpc?apikey=1461e43a-4f35-4dc3-95a7-938c274a528a")
    var c: Int = 0

    @Serializable
    data class RPCFinalizedResponse (val jsonrpc: String, val id: Int, var result: String)
    @Serializable
    data class Result (var number: String)
    @Serializable
    data class RPCHeaderResponse (val jsonrpc: String, val id: Int, var result: Result)


    @Serializable
    public data class BlockTraceResult(val blockTrace: BlockTrace)

    @Serializable
    data class RPCTraceResponse(val jsonrpc: String, val id: Int, val result: BlockTraceResult)

    constructor(url: URL) {
        this.url = url
    }

    public fun getFinalized(): String {
        val json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"jsonrpc\": \"2.0\",\n" +
                "    \"method\": \"chain_getFinalizedHead\",\n" +
                "    \"params\": [\n" +
                "    ]\n" +
                "}"
        return postJsonRPC(json, RPCFinalizedResponse::class.java).result
    }

    public fun getBlockHeight(hash: String): Long {
        val json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"jsonrpc\": \"2.0\",\n" +
                "    \"method\": \"chain_getHeader\",\n" +
                "    \"params\": [\n" +
                "        \"${hash}\"\n" +
                "    ]\n" +
                "}"
        val blockHash = postJsonRPC(json, RPCHeaderResponse::class.java).result.number
        return Integer.decode(blockHash).toLong()//TODO
    }

    public fun getBlockHash(height: Long): String {
        val json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"jsonrpc\": \"2.0\",\n" +
                "    \"method\": \"chain_getBlockHash\",\n" +
                "    \"params\": [\n" +
                "        ${height}\n" +
                "    ]\n" +
                "}"
        return postJsonRPC(json, RPCFinalizedResponse::class.java).result
    }

    public fun getTracing(hash: String): BlockTraceOuterClass.BlockTrace{
        val json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"jsonrpc\": \"2.0\",\n" +
                "    \"method\": \"state_traceBlock\",\n" +
                "    \"params\": [\n" +
                "        \"${hash}\",\n" +
                "        \"pallet,frame,state\",\n" +
                "        \"\",\n" +
                "        \"\"\n" +
                "    ]\n" +
                "}"
//        "        \"pallet,frame,state\",\n" +
        val response = execPostJsonRPC(json)
        val traceBuilder = BlockTraceOuterClass.BlockTraceRPCResponse.newBuilder()
        JsonFormat.parser().ignoringUnknownFields().merge(response.body?.charStream(), traceBuilder)
        return traceBuilder.build().result.blockTrace
//        var a = postJsonRPC(json, RPCTraceResponse::class.java).result.blockTrace
//        val traceBuilder = BlockTraceOuterClass.BlockTrace.newBuilder()
//        JsonFormat.parser().ignoringUnknownFields().merge(a.toString(), traceBuilder)
//        return traceBuilder.build()
//        BlockTraceResult
//        var r = postJsonRPC(json, BlockTraceOuterClass.BlockTraceRPCResponse::class.java)
//        var bt = r.result.blockTrace
//        println("jsonrpc ${r.jsonrpc}")
//        println("block Hash ${r.result.blockTrace.blockHash}")
//        return bt
    }

    private fun <T> postJsonRPC(json: String,  classOfT: Class<T>): T {
        val gson = Gson()

        val response = execPostJsonRPC(json)

        val r =  gson.fromJson(response.body?.charStream(), classOfT)
        response.close()

        return r
    }

    private fun execPostJsonRPC(json: String): Response {
        val client = OkHttpClient()

        val eagerClient = client.newBuilder()
//                .readTimeout(20000, TimeUnit.MILLISECONDS)
//                .callTimeout(20000, TimeUnit.MILLISECONDS)
//                .connectTimeout(20000, TimeUnit.MILLISECONDS)
//                .writeTimeout(20000, TimeUnit.MILLISECONDS)
                .build()

        val requestBody = json.toRequestBody("application/json".toMediaTypeOrNull())

        val request = Request.Builder()
                .url(this.url!!)
                .addHeader("Content-Type", "application/json")
                .post(requestBody)
                .build()
        val response = eagerClient.newCall(request).execute()
        if (!response.isSuccessful) {
            throw IOException("Unexpected HTTP response: ${response.code}")
        }
        return response
    }
}
