package org.example.custom.source

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.core.io.SimpleVersionedSerializer
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import javax.management.RuntimeErrorException

class SimpleSerializer<T> : SimpleVersionedSerializer<T> {
    override fun getVersion(): Int {
        return 1
    }

    override fun serialize(obj: T): ByteArray {
        val boas = ByteArrayOutputStream()
        val oos = ObjectOutputStream(boas)
        oos.writeObject(obj)
        var ba = boas.toByteArray()
        boas.close()
        oos.close()
        println("SERIALIZE ${obj.toString()}")
        return ba
    }

    override fun deserialize(version: Int, serialized: ByteArray): T {
        val bais = ByteArrayInputStream(serialized)
        val ois = ObjectInputStream(bais)
        var obj = ois.readObject() as T
        bais.close()
        ois.close()
        println("DESERIALIZE ${obj.toString()}")
        return obj
}
}
