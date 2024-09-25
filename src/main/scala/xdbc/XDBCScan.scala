package xdbc

import example.Schemata
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

class XDBCScan(tableName: String) extends Scan with Batch {

  override def readSchema(): StructType = Schemata.getSchemaStruct(tableName)
  /*  override def readSchema(): StructType = StructType(Seq(
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
    ))*/

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new XDBCPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = new XDBCPartitionReaderFactory(tableName)
}
