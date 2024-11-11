package xdbc

import example.Schemata
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class XDBCTable(xdbcEnv: XDBCRuntimeEnv) extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = {
    val schema = Schemata.getSchemaStruct(xdbcEnv.tableName)
    println(schema)
    schema
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new XDBCScanBuilder(xdbcEnv)
}
