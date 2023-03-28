import kotlinx.serialization.Serializable
@Serializable
data class StringValues(val key: String, val value: String, val value_encoded: String, val ext_id: String, val method: String)

@Serializable
data class Data(val stringValues: StringValues)

@Serializable
data class Event(val target: String, val data: Data)

@Serializable
public data class BlockTrace(val blockHash: String, val parentHash: String, val tracingTargets: String,
                             val storageKeys: String, val methods: String,
                             val events: List<Event>
)
