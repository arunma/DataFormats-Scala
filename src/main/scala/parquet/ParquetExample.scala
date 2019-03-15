package parquet

import java.io.File
import java.nio.file.Files

import com.tweet.avro.TweetAvro
import com.tweet.thrift.TweetThrift

object ParquetExample {

  def main(args: Array[String]): Unit = {

    val file = new File ("serialized_parquet_file.parquet")
    //Serialize
    val tweet1 = TweetAvro
      .newBuilder
      .setTarget(1)
      .setId(123)
      .setDate("Saturday 8th, June")
      .setUser("alpha1")
      .setText("Parquet tweet1")
      .build()


    val tweet2 = TweetAvro
      .newBuilder
      .setTarget(2)
      .setId(234)
      .setDate("Sunday 9th, June")
      .setUser("alpha2")
      .setText("Parquet tweet2")
      .build()

/*
    val tweet1 = new TweetThrift(1, 123, "Saturday 8th, June", "alpha1")
    tweet1.setText("Parquet Tweet1")

    val tweet2 = new TweetThrift(2, 234, "Sunday 9th, June", "alpha2")
    tweet2.setText("Parquet Tweet2")*/

    Files.deleteIfExists(file.toPath)

    //Thrift schema
    //ParquetSerDe.serializeWithThriftSchema(List(tweet1, tweet2), file)

    //Avro schema
    ParquetSerDe.serializeWithAvroSchema(List(tweet1, tweet2), TweetAvro.SCHEMA$, file)

    //Deserialize with exact schema
    println("Deserializing Full Tweet")
    val returnTweet = ParquetSerDe.deserializeWithAvroSchema(file, TweetAvro.SCHEMA$)
    println (returnTweet)

  }

}
