package xdbc

import example.Schemata
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
    new XDBCTable(tableName)
  }
}

class XDBCPartition extends InputPartition

class XDBCPartitionReaderFactory(tableName: String) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new XDBCPartitionReader(tableName)
}

class XDBCPartitionReader(tableName: String) extends PartitionReader[InternalRow] {

  println(s"XDBCPartitionReader got table ${tableName}")

  def getFixedString(bb: ByteBuffer, size: Int): UTF8String = {
    val bytes = new Array[Byte](size)
    bb.get(bytes)
    UTF8String.fromBytes(bytes).trim()
  }
  /*def getFixedString(bb: ByteBuffer, size: Int): String = {
    val bytes = new Array[Byte](size)
    bb.get(bytes)
    new String(bytes).trim
  }*/

  //TODO: handle these dynamically based on schema and introduce params
  val BUFFER_SIZE = 63556
  val TUPLE_SIZE = 165
  val total_tuples = 59986052L
  var totalRead = new java.util.concurrent.atomic.AtomicLong


  val bb = ByteBuffer.allocateDirect(BUFFER_SIZE * TUPLE_SIZE).order(ByteOrder.nativeOrder())

  val xc = new XClient("spark-XDBC")
  var pointer = xc.initialize("spark-XDBC")
  /*val f = Future {
    xc.startReceiving0(pointer, "lineitem_sf10")
  }*/
  xc.startReceiving0(pointer, "lineitem_sf10")
  var curBufId = xc.getBuffer0(pointer, bb, TUPLE_SIZE)
  bb.rewind()

  override def next(): Boolean = totalRead.get() < total_tuples

  override def get(): InternalRow = {

    val l_orderkey = bb.getInt()
    val l_partkey = bb.getInt()
    val l_suppkey = bb.getInt()
    val l_linenumber = bb.getInt()
    val l_quantity = bb.getDouble()
    val l_extendedprice = bb.getDouble()
    val l_discount = bb.getDouble()
    val l_tax = bb.getDouble()
    val l_returnflag = getFixedString(bb, 1)
    val l_linestatus = getFixedString(bb, 1)
    val l_shipdate = getFixedString(bb, 11)
    val l_commitdate = getFixedString(bb, 11)
    val l_receiptdate = getFixedString(bb, 11)
    val l_shipinstruct = getFixedString(bb, 26)
    val l_shipmode = getFixedString(bb, 11)
    val l_comment = getFixedString(bb, 45)

    totalRead.getAndIncrement()

    if (!bb.hasRemaining && totalRead.get() < total_tuples) {
      //xc.markBufferAsRead0(pointer, curBufId)

      if (xc.hasNext0(pointer) == 1) {
        var curBufId = xc.getBuffer0(pointer, bb, TUPLE_SIZE)

        val time = s"[${java.time.LocalDateTime.now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))}]"
        if (curBufId < 0)
          //println(f"${time} Got ${curBufId} while ${totalRead}/${total_tuples}")
          if (bb.capacity() != TUPLE_SIZE * BUFFER_SIZE || bb.limit() != TUPLE_SIZE * BUFFER_SIZE) {

            //println(s"${time} [Spark] before buf ${bb.position()} / ${bb.limit()} of ${bb.capacity()}")
            bb.rewind()
            //println(s"${time} [Spark] after buf ${bb.position()} / ${bb.limit()} of ${bb.capacity()}")
          }

      }
      bb.rewind()
    }


    val test = InternalRow(
      l_orderkey,
      l_partkey,
      l_suppkey,
      l_linenumber,
      l_quantity,
      l_extendedprice,
      l_discount,
      l_tax,
      l_returnflag,
      l_linestatus,
      l_shipdate,
      l_commitdate,
      l_receiptdate,
      l_shipinstruct,
      l_shipmode,
      l_comment
    )
    //println(test)
    test
  }

  override def close(): Unit = {
    xc.finalize0(pointer)
  }
}