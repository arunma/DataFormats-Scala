package parquet

import java.io.File

import com.arunma.tweet.TweetThrift
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.thrift.ThriftParquetWriter


object ParquetSerDe {
  def serializeWithAvroSchema[T <: SpecificRecord](lt: List[T], schema: Schema, file: File): Unit = {
    val dataFileWriter = AvroParquetWriter
      .builder[T](new Path(file.getAbsolutePath))
      .withSchema(schema)
      .withConf(new Configuration())
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()

    lt.foreach(dataFileWriter.write)
    dataFileWriter.close()
  }

  def serializeWithThriftSchema(lt: List[TweetThrift], file: File): Unit = {
    val dataFileWriter = new ThriftParquetWriter[TweetThrift](new Path(file.getAbsolutePath),
      classOf[TweetThrift],
      CompressionCodecName.UNCOMPRESSED)
    lt.foreach(dataFileWriter.write)
    dataFileWriter.close()
  }


  def deserializeWithAvroSchema[T <: SpecificRecord](file: File, schema: Schema): Unit = {
    val readSupport = new AvroReadSupport[T]()
    val dataFileReader = ParquetReader.builder(readSupport, new Path(file.getAbsolutePath)).build()

    Iterator.continually(dataFileReader.read()).takeWhile(_ != null).foreach(println)
  }
}
