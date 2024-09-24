package xdbc

import com.github.sbt.jni.nativeLoader

import java.nio.ByteBuffer

@nativeLoader("xclient0")
class XClient(name: String) {

  @native def initialize(name: String): Long

  @native def startReceiving0(pointer: Long, tableName: String): Long

  @native def hasNext0(pointer: Long): Int

  @native def getBuffer0(pointer: Long, resBuf: ByteBuffer, tupleSize: Int): Int

  @native def markBufferAsRead0(pointer: Long, bufferId: Int): Int

  @native def finalize0(pointer: Long): Int

}

