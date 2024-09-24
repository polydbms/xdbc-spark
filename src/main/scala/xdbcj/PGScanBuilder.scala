package xdbcj

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class PGScanBuilder extends ScanBuilder {
  override def build(): Scan = new PGScan
}
