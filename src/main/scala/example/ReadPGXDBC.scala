package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import java.util.Properties
import java.util.concurrent.TimeUnit


object ReadPGXDBC {

  def main(args: Array[String]) {


    val time1 = System.nanoTime()
    val spark = SparkSession.builder.appName("PG Reader XDBC").getOrCreate()

    //val df = spark.read.format("xdbcj.PGReader").load("public.pg1_sf1_lineitem")
    val df = spark.read.format("xdbc.XDBCReader").load("lineitem_sf10")
    df.createOrReplaceTempView("lineitem_xdbc")
    df.cache()
    println(df.rdd.getNumPartitions)

    val res = spark.sql("SELECT COUNT(l_orderkey),MIN(l_orderkey), MAX(l_orderkey), AVG(l_orderkey) FROM lineitem_xdbc")
    //res.explain()
    res.show()

    val elapsedTimeXDBC = TimeUnit.MILLISECONDS.convert((System.nanoTime() - time1), TimeUnit.NANOSECONDS)

    spark.stop

    println(s"Elapsed time XDBC: ${elapsedTimeXDBC}s")


  }
}
