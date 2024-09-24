package xdbcj

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.{ConnectException, InetSocketAddress}
import java.nio.{BufferUnderflowException, ByteBuffer, ByteOrder}
import java.nio.channels.{ClosedChannelException, SocketChannel}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, PriorityBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicIntegerArray}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


class PGReader extends TableProvider {

  val lineitemSchema = StructType(Seq(
    StructField("l_orderkey", IntegerType),
    StructField("l_partkey", IntegerType),
    StructField("l_suppkey", IntegerType),
    StructField("l_linenumber", IntegerType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType)
  ))

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = lineitemSchema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new PGTable()
}


class SimplePartition extends InputPartition

class SimplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new SimplePartitionReader()
}

class SimplePartitionReader extends PartitionReader[InternalRow] {

  //init
  val sleepTime = 1
  val total_tuples = 10000000L
  val buffer_size = 1000
  val tuple_size = 48
  val bufferPoolSize = 1000
  val finishedReading = new AtomicBoolean(false)
  val finishedDeser = new AtomicBoolean(false)

  var totalRead = new java.util.concurrent.atomic.AtomicLong
  var rows = new ConcurrentLinkedQueue[InternalRow]()
  val bufferPool = new Array[ByteBuffer](bufferPoolSize)
  //1 = write, 0 = read
  val bpTracker = new AtomicIntegerArray(bufferPoolSize)
  var getReadIndex = new java.util.concurrent.atomic.AtomicInteger

  for (i <- 0 until bufferPoolSize) {
    bufferPool(i) = ByteBuffer.allocateDirect(buffer_size * tuple_size)
    bufferPool(i).order(ByteOrder.nativeOrder())
    bufferPool(i).clear()
    bpTracker.set(i, 1)
  }

  val clientSocketChannel = SocketChannel.open()
  clientSocketChannel.connect(new InetSocketAddress("127.0.0.1", 1234))


  //clientSocketChannel.configureBlocking(false)

  //val timeReadStart = System.nanoTime()
  //read
  val f = Future {
    var bpi = 0
    var bytesRead = 0
    var totalBytesRead = 0L


    while (!clientSocketChannel.finishConnect()) {
      println("connecting...")
    }

    val msgBuf = ByteBuffer.wrap("Give\n".getBytes(StandardCharsets.UTF_8))
    clientSocketChannel.write(msgBuf)



    //fetch
    while (bytesRead != -1) {

      //println(s"working on $bpi with bytes read ${totalBytesRead} with total ${tuple_size * total_tuples}")
      while (bpTracker.get(bpi) == 0) {
        //println(s"Write: blocked at buffer  ${bpi} with bytes read ${bytesRead}")
        Thread.sleep(sleepTime)
      }
      bufferPool(bpi).clear()

      while (bufferPool(bpi).hasRemaining && bytesRead != -1) {
        bytesRead = clientSocketChannel.read(bufferPool(bpi))
        if (bytesRead > 0)
          totalBytesRead += bytesRead
        Thread.sleep(sleepTime)
      }

      bufferPool(bpi).flip()
      bpTracker.set(bpi, 0)
      //println(s"Write: Flag array ${bpTracker}")
      bpi += 1
      if (bpi == bufferPoolSize)
        bpi = 0
    }

    //clientSocketChannel.close()
    finishedReading.set(true)
    "finished reading"
  }


  /*val f2 = Future {
    var readIndex = 0
    var totalCnt = 0L
    while (totalCnt < total_tuples) {

      while (bpTracker.get(readIndex) == 1) {
        Thread.sleep(sleepTime)
        println("Deserialize blocked at buffer" + readIndex)
      }

      val bb = bufferPool(readIndex)
      while (bb.hasRemaining) {

        try {
          val l_orderkey = bb.getInt()
          val l_partkey = bb.getInt()
          val l_suppkey = bb.getInt()
          val l_linenumber = bb.getInt()
          val l_quantity = bb.getDouble()
          val l_extendedprice = bb.getDouble()
          val l_discount = bb.getDouble()
          val l_tax = bb.getDouble()

          totalCnt += 1
          rows.add(
            InternalRow(l_orderkey, l_partkey, l_suppkey, l_linenumber,
              l_quantity, l_extendedprice, l_discount, l_tax)
          )

        }
        catch {
          case e: BufferUnderflowException => println(s"Exception with msg: ${e.getMessage}")
        }

      }
      //println("Size of queue " + rows.size() + " total deserialized count " + totalCnt)
      //totalRead += tuples_per_batch
      bpTracker.set(readIndex, 1)
      //println(s"Deserialize: Flag array ${bpTracker}")
      //println(s"deserialized buffer $readIndex")
      readIndex += 1
      if (readIndex == bufferPoolSize) {
        readIndex = 0
      }

    }

    finishedDeser.set(true)
    "finished writing"
  }*/

  /*val res = {
    for {
      res1 <- f
      res2 <- f2
    } yield (res1 + "" + res2)
  }
  res onComplete {
    case Success(str) => println(str)
    case Failure(e) => println(e.getMessage)
  }*/

  f onComplete {
    case Success(str) => println(str)
    case Failure(e) => println(e.getMessage)
  }


  def next = totalRead.get() < total_tuples


  def get = {


    /*while (rows.isEmpty) {
      println("Get: blocked at tuple " + totalRead)
      Thread.sleep(sleepTime)
    }

    //print("generating: " + rows.peek())
    totalRead += 1

    /* if (totalRead == total_tuples) {
       while (!finishedReading.get() && !finishedDeser.get())
         Thread.sleep(sleepTime)
     }*/

    rows.poll()*/


    while (bpTracker.get(getReadIndex.get()) == 1) {
      Thread.sleep(sleepTime)
      //println(s"Get: blocked at buffer: ${getReadIndex} and tuple ${totalRead}")
    }

    val bb = bufferPool(getReadIndex.get())

    val l_orderkey = bb.getInt()
    val l_partkey = bb.getInt()
    val l_suppkey = bb.getInt()
    val l_linenumber = bb.getInt()
    val l_quantity = bb.getDouble()
    val l_extendedprice = bb.getDouble()
    val l_discount = bb.getDouble()
    val l_tax = bb.getDouble()

    if (!bb.hasRemaining) {
      bpTracker.set(getReadIndex.get(), 1)
      //println(s"GET: ${bpTracker}")
      //println(s"GET: Read buffer ${getReadIndex}")
      getReadIndex.incrementAndGet()
      if (getReadIndex.get() == bufferPoolSize) {
        getReadIndex.set(0)
      }
    }
    //println(totalRead)

    totalRead.getAndIncrement()

    /*if (totalRead == total_tuples) {
      while (!finishedReading.get() && !finishedDeser.get()) {
        println("Sleeping last")
        Thread.sleep(sleepTime)
      }
    }*/

    InternalRow(l_orderkey, l_partkey, l_suppkey, l_linenumber
      , l_quantity, l_extendedprice, l_discount, l_tax)


  }

  def close() = {
    clientSocketChannel.close()
    println("closing")
    while (clientSocketChannel.isConnected) {
      Thread.sleep(sleepTime)
    }
  }

}