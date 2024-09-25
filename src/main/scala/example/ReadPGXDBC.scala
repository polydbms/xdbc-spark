package example

import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit


object ReadPGXDBC {

  def main(args: Array[String]) {

    var tableToRead = "lineitem_sf10"
    if (args.length > 0) {
      tableToRead = args(0)
      println(s"TableToRead: $tableToRead")
    }

    val time1 = System.nanoTime()
    val spark = SparkSession.builder.appName("PG Reader XDBC").getOrCreate()
    //val df = spark.read.format("xdbcj.PGReader").load("public.pg1_sf1_lineitem")
    val df = spark.read.format("xdbc.XDBCReader").load(tableToRead)
    df.createOrReplaceTempView("lineitem_xdbc")
    df.cache()
    println(df.rdd.getNumPartitions)

    val res = spark.sql("SELECT COUNT(l_orderkey),MIN(l_orderkey), MAX(l_orderkey), AVG(l_orderkey) FROM lineitem_xdbc")
    //res.explain()
    res.show()

    /*val res2 = spark.sql("SELECT * FROM lineitem_xdbc LIMIT 5")
    res2.show*/

    val elapsedTimeXDBC = TimeUnit.MILLISECONDS.convert((System.nanoTime() - time1), TimeUnit.NANOSECONDS)

    spark.stop

    println(s"Elapsed time XDBC: ${elapsedTimeXDBC}s")


  }
}
