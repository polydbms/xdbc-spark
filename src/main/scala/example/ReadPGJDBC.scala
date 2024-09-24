package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import java.util.Properties
import java.util.concurrent.TimeUnit


object ReadPGJDBC {

  def main(args: Array[String]) {


    val spark2 = SparkSession.builder.appName("PG Reader JDBC").getOrCreate()

    val connectionProperties = new Properties()

    connectionProperties.put("Driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "123456")
    connectionProperties.put("fetchsize", "250000")
    connectionProperties.put("pushDownPredicate", "false")

    val time12 = System.nanoTime()
    val numPartitions = 4
    val df2 = spark2.read.jdbc("jdbc:postgresql://pg1:5432/db1", "public.lineitem_sf10", "l_orderkey", 0, 60000000, numPartitions, connectionProperties)
    df2.createOrReplaceTempView("lineitem_jdbc")
    df2.cache
    println(df2.rdd.getNumPartitions)
    val res2 = spark2.sql("SELECT COUNT(l_orderkey), AVG(l_orderkey), MIN(l_orderkey), MAX(l_orderkey) FROM lineitem_jdbc")
    //res2.explain()
    res2.show()


    val elapsedTimeJDBC = TimeUnit.MILLISECONDS.convert((System.nanoTime() - time12), TimeUnit.NANOSECONDS)

    println(s"Elapsed time JDBC: ${elapsedTimeJDBC}s")

    spark2.stop()


  }
}
