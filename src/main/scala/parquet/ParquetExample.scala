package parquet

import java.io.File

import com.tweet.avro.TweetAvro

object ParquetExample {

  def main(args: Array[String]): Unit = {

    val file = new File ("serialized_parquet_file.parquet")
    //Serialize
    /*val tweet1 = TweetAvro
      .newBuilder
      .setTarget(1)
      .setId(123)
      .setDate("Saturday 8th, June")
      .setUser("nus1")
      .setText("Parquet tweet1")
      .build()


    val tweet2 = TweetAvro
      .newBuilder
      .setTarget(2)
      .setId(234)
      .setDate("Sunday 9th, June")
      .setUser("nus2")
      .setText("Parquet tweet2")
      .build()


    ParquetSerDe.serialize(List(tweet1, tweet2), TweetAvro.SCHEMA$, file)*/

    //Thrift schema
   /* val tweet = new TweetThrift(1, 123, "Saturday 8th, June", "arunma")
    tweet.setText("First tweet")
    ParquetSerDe.serializeWithThriftSchema(List(tweet1, tweet2), file)*/

    //Deserialize with exact schema
    println("Deserializing Full Tweet")
    val returnTweet = ParquetSerDe.deserializeWithAvroSchema(file, TweetAvro.SCHEMA$)
    println (returnTweet)

  }

}
