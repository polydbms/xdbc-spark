package example

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import play.api.libs.json._
import scala.io.Source
import scala.collection.JavaConverters._

object SchemaConverter {

  // Case class to map the JSON structure
  case class ColumnDefinition(name: String, `type`: String, size: Int)

  // Implicit Reads to parse JSON into case class
  implicit val columnDefinitionReads: Reads[ColumnDefinition] = Json.reads[ColumnDefinition]

  // Function to read the file and return the content as a string
  def readFileAsString(filePath: String): String = {
    val fileSource = Source.fromFile(filePath)
    val fileContent = fileSource.getLines().mkString("\n")
    fileSource.close()
    fileContent
  }

  // Function to convert JSON to Spark StructType schema
  def convertJsonToSchema(name: String): StructType = {
    val jsonString = readFileAsString(s"/xdbc-client/tests/schemas/${name}.json")
    val json = Json.parse(jsonString)
    val columns = json.as[List[ColumnDefinition]]

    // Map to Spark StructField
    val fields = columns.map { col =>
      val dataType = col.`type` match {
        case "INT" => IntegerType
        case "DOUBLE" => DoubleType
        case "CHAR" => StringType
        case "STRING" => StringType
        case _ => StringType
      }
      StructField(col.name, dataType)
    }

    // Create StructType from fields
    StructType(fields)
  }
}
