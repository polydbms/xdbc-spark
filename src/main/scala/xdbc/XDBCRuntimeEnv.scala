package xdbc

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class XDBCRuntimeEnv(
                           server_host: String,
                           tableName: String,
                           buffer_size: Int,
                           bufferpool_size: Int,
                           rcv_par: Int,
                           decomp_par: Int,
                           write_par: Int,
                           iformat: Int,
                           transfer_id: Long
                         )

object XDBCRuntimeEnv {

  // Method to create an XDBCRuntimeEnv from a java.util.Map[String, String]
  def fromOptions(options: java.util.Map[String, String]): XDBCRuntimeEnv = {
    // Convert java.util.Map to Scala Map
    val scalaOptions = options.asScala.toMap

    // Extract the values from the scalaOptions map, with conversions from String to Int
    val serverHost = scalaOptions.getOrElse("server_host", throw new IllegalArgumentException("Missing server_host"))
    val tableName = scalaOptions.getOrElse("tableName", throw new IllegalArgumentException("Missing tableName"))
    val bufferSize = scalaOptions.getOrElse("buffer_size", throw new IllegalArgumentException("Missing buffer_size")).toInt
    val bufferpoolSize = scalaOptions.getOrElse("bufferpool_size", throw new IllegalArgumentException("Missing bufferpool_size")).toInt
    val rcvPar = scalaOptions.getOrElse("rcv_par", throw new IllegalArgumentException("Missing rcv_par")).toInt
    val decompPar = scalaOptions.getOrElse("decomp_par", throw new IllegalArgumentException("Missing decomp_par")).toInt
    val writePar = scalaOptions.getOrElse("write_par", throw new IllegalArgumentException("Missing write_par")).toInt
    val iformat = scalaOptions.getOrElse("iformat", throw new IllegalArgumentException("Missing iformat")).toInt
    val transferId = scalaOptions.getOrElse("transfer_id", throw new IllegalArgumentException("Missing transfer_id")).toLong

    // Create and return the XDBCRuntimeEnv instance
    XDBCRuntimeEnv(
      server_host = serverHost,
      tableName = tableName,
      buffer_size = bufferSize,
      bufferpool_size = bufferpoolSize,
      rcv_par = rcvPar,
      decomp_par = decompPar,
      write_par = writePar,
      iformat = iformat,
      transfer_id = transferId
    )
  }
}
