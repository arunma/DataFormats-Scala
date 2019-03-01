package avro

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.Files

import com.arunma.tweet.avro.TweetAvro
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.SpecificDatumWriter

object AvroSerDe {

  def serialize(tweet: TweetAvro, file:File) = {
    val out = new BufferedOutputStream(new FileOutputStream(file))
    val datumWriter = new SpecificDatumWriter[TweetAvro]()
    val dataFileWriter = new DataFileWriter[TweetAvro](datumWriter)
    dataFileWriter.create(TweetAvro.SCHEMA$, out)
    dataFileWriter.append(tweet)
    dataFileWriter.close()
    out.flush()
  }

  def deserialize[T](file:File, schema: Schema): Option[T] ={
    val bytes = Files.readAllBytes(file.toPath)
    val in = new SeekableByteArrayInput(bytes)
    val datumReader = new GenericDatumReader[T](schema)
    val dataFileReader = new DataFileReader[T](in, datumReader)

    if (dataFileReader.hasNext) Some(dataFileReader.next()) else None
  }
}
