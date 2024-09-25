package example

import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

object Schemata {

  val schemaTupleSizeMap: Map[String, Integer] = Map(
    "lineitem_sf10" -> 165,
    "inputeventsm" -> 0,
    "ss13husallm" -> 0,
    "iotm" -> 0
  )

  val schemaRowsMap: Map[String, Long] = Map(
    "lineitem_sf10" -> 59986052L,
    "inputeventsm" -> 8978893,
    "ss13husallm" -> 1476313,
    "iotm" -> 5833099L
  )

  //def getSchemaStruct(tableName: String): StructType = SchemaConverter.convertJsonToSchema(tableName)
  def getSchemaStruct(tableName: String): StructType = StructType(Seq(
    StructField("l_orderkey", IntegerType),
    StructField("l_partkey", IntegerType),
    StructField("l_suppkey", IntegerType),
    StructField("l_linenumber", IntegerType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", StringType),
    StructField("l_commitdate", StringType),
    StructField("l_receiptdate", StringType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType)
  ))

}
