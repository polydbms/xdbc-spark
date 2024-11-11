package xdbc

import example.Schemata.getSchemaStruct
import example.{SchemaConverter, Schemata}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.nio.{ByteBuffer, ByteOrder}
import java.util
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

class XDBCReader extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val tableName = options.get("path")
    Schemata.getSchemaStruct(tableName)
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val tableName = properties.get("path")
    //println(s"In getTable passing ${tableName}")
    new XDBCTable(XDBCRuntimeEnv.fromOptions(properties))
  }
}

class XDBCPartition extends InputPartition

class XDBCPartitionReaderFactory(xdbcEnv: XDBCRuntimeEnv) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new XDBCPartitionReader(xdbcEnv)
}

class XDBCPartitionReader(xdbcEnv: XDBCRuntimeEnv) extends PartitionReader[InternalRow] {

  println(s"XDBCPartitionReader got table ${xdbcEnv.tableName}")

  //TODO: handle these dynamically based on schema and introduce params

  val TUPLE_SIZE = SchemaConverter.calculateTupleSizeInBytes(xdbcEnv.tableName)
  val total_tuples = Schemata.schemaRowsMap.getOrElse(xdbcEnv.tableName, throw new IllegalArgumentException(s"Table name '$xdbcEnv.tableName' not found in schemaTupleSizeMap."))
  var totalRead = new java.util.concurrent.atomic.AtomicLong


  val bb = ByteBuffer.allocateDirect(xdbcEnv.buffer_size * 1024).order(ByteOrder.nativeOrder())

  val xc = new XClient("spark-XDBC")
  var pointer = xc.initialize(xdbcEnv.server_host, "spark-XDBC", xdbcEnv.tableName, xdbcEnv.transfer_id, xdbcEnv.iformat, xdbcEnv.buffer_size, xdbcEnv.bufferpool_size, xdbcEnv.rcv_par, xdbcEnv.decomp_par, xdbcEnv.write_par)
  /*val f = Future {
    xc.startReceiving0(pointer, "lineitem_sf10")
  }*/
  xc.startReceiving0(pointer, xdbcEnv.tableName)
  var curBufId = xc.getBuffer0(pointer, bb, TUPLE_SIZE)
  bb.rewind()
  val extractRowForCol = Schemata.generateRowExtractorForCol(xdbcEnv.tableName)
  val extractRow = Schemata.generateRowExtractorforRow(xdbcEnv.tableName)
  val fieldSizeMap = SchemaConverter.convertJsonToFieldSizeMap(xdbcEnv.tableName)
  val schema = getSchemaStruct(xdbcEnv.tableName)
  var bufferTupleIndex = 0
  var tuplesPerBuffer = bb.limit() / TUPLE_SIZE
  var currentOffset = 0

  override def next(): Boolean = {
    //println(s"${totalRead.get()} < ${total_tuples} (${totalRead.get() < total_tuples})")
    totalRead.get() < total_tuples
  }

  override def get(): InternalRow = {

    //val row = extractRow(bb)
    var row: InternalRow = null
    if (xdbcEnv.iformat == 1) {
      row = extractRow(bb)
    }
    else {
      row = extractRowForCol(bb, bufferTupleIndex, tuplesPerBuffer)
      //TODO: Avoid counting by setting the buffer position manually in generated code
      bufferTupleIndex += 1

    }


    totalRead.getAndIncrement()

    if ((xdbcEnv.iformat == 2) & (bufferTupleIndex == tuplesPerBuffer) ||
      (xdbcEnv.iformat == 1) && (!bb.hasRemaining && totalRead.get() < total_tuples)) {
      //xc.markBufferAsRead0(pointer, curBufId)
      //println("Entered buffer finished case")

      if (xc.hasNext0(pointer) == 1) {
        curBufId = xc.getBuffer0(pointer, bb, TUPLE_SIZE)
        tuplesPerBuffer = bb.limit() / TUPLE_SIZE
        bufferTupleIndex = 0
        currentOffset = 0

        //val time = s"[${java.time.LocalDateTime.now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))}]"
        if (curBufId < 0)
          //println(f"${time} Got ${curBufId} while ${totalRead}/${total_tuples}")
          if (bb.capacity() != TUPLE_SIZE * xdbcEnv.buffer_size || bb.limit() != TUPLE_SIZE * xdbcEnv.buffer_size) {

            //println(s"${time} [Spark] before buf ${bb.position()} / ${bb.limit()} of ${bb.capacity()}")
            bb.rewind()
            //println(s"${time} [Spark] after buf ${bb.position()} / ${bb.limit()} of ${bb.capacity()}")
          }

      }
      bb.rewind()
    }
    row
  }

  override def close(): Unit = {
    xc.finalize0(pointer)
  }
}