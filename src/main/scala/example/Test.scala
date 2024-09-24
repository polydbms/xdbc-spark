package example


import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.nio.{BufferUnderflowException, ByteBuffer, ByteOrder}
import java.util.concurrent.{ConcurrentLinkedQueue, PriorityBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicIntegerArray}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ThreadRunner() {

  val sleepTime = 1000
  val total_tuples = 10000000L
  val buffer_size = 1000
  val tuple_size = 48
  val bufferPoolSize = 1000
  val finishedReading = new AtomicBoolean(false)
  val finishedDeser = new AtomicBoolean(false)

  var hasNext = true

  var totalRead = 0
  var rows = new ConcurrentLinkedQueue[(Int, Int, Int, Int, Double, Double, Double, Double)]()
  val bufferPool = new Array[ByteBuffer](bufferPoolSize)
  //1 = write, 0 = read
  val bpTracker = new AtomicIntegerArray(bufferPoolSize)


  for (i <- 0 until bufferPoolSize) {
    bufferPool(i) = ByteBuffer.allocateDirect(buffer_size * tuple_size)
    bufferPool(i).order(ByteOrder.nativeOrder())
    bufferPool(i).clear()
    bpTracker.set(i, 1)
  }

  val clientSocketChannel = SocketChannel.open()
  clientSocketChannel.connect(new InetSocketAddress("127.0.0.1", 1234))
  clientSocketChannel.configureBlocking(false)

  val f = Future {

    var bpi = 0;

    while (!clientSocketChannel.finishConnect()) {
      println("connecting...")
    }

    val msgBuf = ByteBuffer.wrap("Give\n".getBytes(StandardCharsets.UTF_8))
    clientSocketChannel.write(msgBuf)

    var bytesRead = 0
    while (bytesRead != -1) {
      bytesRead = clientSocketChannel.read(bufferPool(bpi))
      while (bufferPool(bpi).hasRemaining) {
        //bufferPool(bpi).clear()
        Thread.sleep(sleepTime)
        println(s"Reading because bytes read $bytesRead")
        val curBytes = clientSocketChannel.read(bufferPool(bpi))
        println(s"Reading another ${curBytes}")
        bytesRead += curBytes


      }

      if (!bufferPool(bpi).hasRemaining) {
        //println("read: " + bytesRead)

        //println("Wrote to buffer no " + bpi)
        bufferPool(bpi).flip()
        bpTracker.set(bpi, 0)
        //println(s"Write: Flag array ${bpTracker}")
        bpi += 1
        if (bpi == bufferPoolSize)
          bpi = 0

        while (bpTracker.get(bpi) == 0) {
          println("Write: blocked at buffer  " + bpi)
          Thread.sleep(sleepTime)
        }
      }

    }

    finishedReading.set(true)


    "finished reading"
  }


  val f2 = Future {
    var readIndex = 0;
    var totalCnt = 0L
    while (totalCnt < total_tuples) {

      while (bpTracker.get(readIndex) == 1) {
        Thread.sleep(sleepTime)
        //println("Deserialize blocked at buffer" + readIndex)
      }

      val bb = bufferPool(readIndex)

      while (bb.hasRemaining) {

        //try {
        val l_orderkey = bb.getInt()
        val l_partkey = bb.getInt()
        val l_suppkey = bb.getInt()
        val l_linenumber = bb.getInt()
        val l_quantity = bb.getDouble()
        val l_extendedprice = bb.getDouble()
        val l_discount = bb.getDouble()
        val l_tax = bb.getDouble()

        totalCnt += 1
        val tuple = Tuple8(l_orderkey, l_partkey, l_suppkey, l_linenumber,
          l_quantity, l_extendedprice, l_discount, l_tax)

        //println(s"deserialized ${tuple}")

        rows.add(tuple)

        //println(s"size of queue ${rows.size()}")


        /*  }
          catch {
            case e: BufferUnderflowException => println(s"Exception with msg: ${e.getMessage}")
          }*/

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
  }

  def next = totalRead < total_tuples


  def get = {
    while (rows.isEmpty)
      Thread.sleep(sleepTime)
    totalRead += 1
    //println(s"got ${totalRead}")
    rows.poll()
  }

  def close = {
    clientSocketChannel.close()
    Thread.sleep(sleepTime)
    println("closing")
    while (clientSocketChannel.isConnected) {
      Thread.sleep(sleepTime)

    }
  }
}

object Test extends App {

  val t = new ThreadRunner

  var cnt = 0
  while (t.next) {
    cnt += 1
    t.get
    //println(s"printing ${t.get}")
  }
  println(cnt)

  t.close

}
