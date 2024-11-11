package xdbc

import com.github.sbt.jni.nativeLoader

import java.nio.ByteBuffer

@nativeLoader("xclient0")
class XClient(name: String) {

  @native def initialize(server_host: String, name: String, tableName: String, tid: Long, iformat: Int, buffer_size: Int, bpool_size: Int, rcv_par: Int, decomp_par: Int, write_par: Int): Long

  @native def startReceiving0(pointer: Long, tableName: String): Long

  @native def hasNext0(pointer: Long): Int

  @native def getBuffer0(pointer: Long, resBuf: ByteBuffer, tupleSize: Int): Int

  @native def markBufferAsRead0(pointer: Long, bufferId: Int): Int

  @native def finalize0(pointer: Long): Int

}

