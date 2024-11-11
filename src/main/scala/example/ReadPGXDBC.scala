package example

import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit


object ReadPGXDBC {

  def main(args: Array[String]) {

    if (args.length != 9) {
      println("Usage: <tableName> <buffer_size> <bufferpool_size> <rcv_par> <decomp_par> <write_par> <iformat> <transfer_id>")
      System.exit(1)
    }

    val optionsMap: Map[String, String] = Map(
      "tableName" -> args(0),
      "buffer_size" -> args(1),
      "bufferpool_size" -> args(2),
      "rcv_par" -> args(3),
      "decomp_par" -> args(4),
      "write_par" -> "1", //TODO: introduce multiple readers
      "iformat" -> args(6),
      "transfer_id" -> args(7),
      "server_host" -> args(8)
    )

    val time1 = System.nanoTime()
    val spark = SparkSession.builder.appName("PG Reader XDBC").getOrCreate()
    //val df = spark.read.format("xdbcj.PGReader").load("public.pg1_sf1_lineitem")
    val df = spark.read.format("xdbc.XDBCReader").options(optionsMap).load(args(0))
    df.createOrReplaceTempView("lineitem_xdbc")
    //df.cache()
    println(df.rdd.getNumPartitions)

    //val res = spark.sql("SELECT COUNT(l_orderkey),MIN(l_orderkey), MAX(l_orderkey), AVG(l_orderkey) FROM lineitem_xdbc")
    val res = spark.sql(s"SELECT ${Schemata.generateCountString(args(0))} FROM lineitem_xdbc")
    res.explain()
    res.show()

    /*val res2 = spark.sql("SELECT DISTINCT(l_comment) FROM lineitem_xdbc")
    res2.explain()
    res2.show*/

    val elapsedTimeXDBC = TimeUnit.MILLISECONDS.convert((System.nanoTime() - time1), TimeUnit.NANOSECONDS)

    spark.stop

    println(s"Elapsed time XDBC: ${elapsedTimeXDBC}ms")


  }
}
