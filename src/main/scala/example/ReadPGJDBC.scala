package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import java.util.Properties
import java.util.concurrent.TimeUnit


object ReadPGJDBC {

  def main(args: Array[String]) {

    var numPartitions = 4
    var tableToRead = "lineitem_sf10"
    if (args.length > 1) {
      tableToRead = args(0)
      println(s"TableToRead: $tableToRead")
      numPartitions = args(1).toInt

    }

    val spark2 = SparkSession.builder.appName("PG Reader JDBC").getOrCreate()

    val connectionProperties = new Properties()

    connectionProperties.put("Driver", "org.postgresql.Driver")
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "123456")
    connectionProperties.put("fetchsize", "250000")
    connectionProperties.put("pushDownPredicate", "false")
    connectionProperties.put("pushDownAggregate", "false")
    connectionProperties.put("pushDownLimit", "false")
    connectionProperties.put("pushDownOffset", "false")
    connectionProperties.put("pushDownTableSample", "false")

    val time12 = System.nanoTime()

    var min = 0
    var max = 60000000
    var key = "l_orderkey"

    tableToRead match {
      case "lineitem_sf10" => {
        key = "l_orderkey"
        min = 0
        max = 60000000
      }
      case "ss13husallm" => {
        key = "SERIALNO"
        min = 1
        max = 1492845
      }
      case "iotm" => {
        key = "id"
        min = 0
        max = 5833098
      }
      case "inputeventsm" => {
        key = "subject_id"
        min = 10000032
        max = 19999987
      }
    }

    val df2 = spark2.read.jdbc("jdbc:postgresql://pg1:5432/db1", s"public.${tableToRead}", key, min, max, numPartitions, connectionProperties)
    /*val df2 = spark2.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://pg1:5432/db1")
      .option("dbtable", "public.lineitem_sf10")
      .option("user", "postgres")
      .option("password", "123456")
      .option("fetchsize", "250000")
      .option("pushDownPredicate", "false")
      .option("pushDownLimit", "false")
      .load()*/
    df2.createOrReplaceTempView("lineitem_jdbc")
    //df2.cache
    println(df2.rdd.getNumPartitions)
    //val res2 = spark2.sql("SELECT COUNT(l_orderkey), AVG(l_orderkey), MIN(l_orderkey), MAX(l_orderkey) FROM lineitem_jdbc")
    val res2 = spark2.sql(s"SELECT ${Schemata.generateCountString(tableToRead)} FROM lineitem_jdbc")
    res2.explain()
    res2.show()


    val elapsedTimeJDBC = TimeUnit.MILLISECONDS.convert((System.nanoTime() - time12), TimeUnit.NANOSECONDS)

    println(s"Elapsed time JDBC: ${elapsedTimeJDBC}ms")

    spark2.stop()


  }
}
