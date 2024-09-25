package xdbc

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class XDBCScanBuilder(tableName: String) extends ScanBuilder {
  override def build(): Scan = new XDBCScan(tableName)
}
