package example

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

object Schemata {

  val schemaRowsMap: Map[String, Long] = Map(
    "lineitem_sf10" -> 59986052L,
    "inputeventsm" -> 8978893L,
    "ss13husallm" -> 1476313L,
    "iotm" -> 5833099L
  )

  def generateCountString(tableName: String): String = {
    val schemaMap = getSchemaStruct(tableName)
    schemaMap.fields.map(field => s"COUNT(${field.name})").mkString(", ")
  }

  def generateRowExtractorForCol(tableName: String): (ByteBuffer, Int, Int) => InternalRow = {
    val schema = getSchemaStruct(tableName)
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

    val fieldSizeMap = SchemaConverter.convertJsonToFieldSizeMap(tableName)

    val functionCode = schema.fields.zipWithIndex.map { case (field, index) =>
      field.dataType match {
        case IntegerType =>
          s"""
             |{
             |  val position = currentOffset + bufferTupleIndex * 4
             |  currentOffset += tuplesPerBuffer * 4
             |  bb.getInt(position)
             |}
         """.stripMargin
        case DoubleType =>
          s"""
             |{
             |  val position = currentOffset + bufferTupleIndex * 8
             |  currentOffset += tuplesPerBuffer * 8
             |  bb.getDouble(position)
             |}
         """.stripMargin
        case StringType =>
          val size = fieldSizeMap.getOrElse(field.name, throw new IllegalArgumentException(s"Unknown size for ${field.name}"))
          s"""
             |{
             |  val position = currentOffset + bufferTupleIndex * $size
             |  val bytes = new Array[Byte]($size)
             |  bb.position(position)
             |  bb.get(bytes, 0, $size)
             |  currentOffset += tuplesPerBuffer * $size
             |  org.apache.spark.unsafe.types.UTF8String.fromBytes(bytes).trim()
             |}
         """.stripMargin
        case _ =>
          throw new IllegalArgumentException(s"Unsupported data type for field ${field.name}")
      }
    }.mkString(", ")

    // Generate code to extract columns in columnar format for the given row
    val code =
      s"""
      (bb: java.nio.ByteBuffer, bufferTupleIndex: Int, tuplesPerBuffer: Int) => {
        var currentOffset = 0
        org.apache.spark.sql.catalyst.InternalRow(
          $functionCode
        )
      }
    """

    // Compile the generated code at runtime
    val compiledCode = tb.eval(tb.parse(code)).asInstanceOf[(ByteBuffer, Int, Int) => InternalRow]
    compiledCode
  }

  def generateRowExtractorforRow(tableName: String): ByteBuffer => InternalRow = {
    val schema = getSchemaStruct(tableName)
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

    val fieldSizeMap = SchemaConverter.convertJsonToFieldSizeMap(tableName)

    val functionCode = schema.fields.map { field =>
      field.dataType match {
        case IntegerType => s"bb.getInt()"
        case DoubleType => s"bb.getDouble()"
        case StringType =>
          val size = fieldSizeMap.getOrElse(field.name, throw new IllegalArgumentException(s"Unknown size for ${field.name}"))
          s"""
             |{
             |  val bytes = new Array[Byte]($size)
             |  bb.get(bytes)
             |  org.apache.spark.unsafe.types.UTF8String.fromBytes(bytes).trim()
             |}
           """.stripMargin
        case _ => throw new IllegalArgumentException(s"Unsupported data type for field ${field.name}")
      }
    }.mkString(", ")

    val code =
      s"""
        (bb: java.nio.ByteBuffer) => {
          org.apache.spark.sql.catalyst.InternalRow(
            $functionCode
          )
        }
      """

    val compiledCode = tb.eval(tb.parse(code)).asInstanceOf[ByteBuffer => InternalRow]
    compiledCode
  }


  def getSchemaStruct(tableName: String): StructType = SchemaConverter.convertJsonToSchema(tableName)
  /*def getSchemaStruct(tableName: String): StructType = StructType(Seq(
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
*/
}
