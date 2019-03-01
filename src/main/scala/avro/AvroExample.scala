package avro

import java.io.File

import com.arunma.tweet.avro.{TweetAvro, UserTweetAvro}

object AvroExample {

  def main(args: Array[String]): Unit = {

    val file = new File ("serialized_avro_file.avro")
    //Serialize
    val tweet = TweetAvro
      .newBuilder
      .setTarget(1)
      .setId(123)
      .setDate("Saturday 8th, June")
      .setUser("arunma")
      .setText("Proto tweet")
      .build()

    tweet.setText("First tweet")
    AvroSerDe.serialize(tweet, file)

    //Deserialize with exact schema
    println("Deserializing Full Tweet")
    val returnTweet = AvroSerDe.deserialize(file, TweetAvro.getClassSchema)
    println (returnTweet)

    //Deserialize with minimal schema
    println("Deserializing Minimal Tweet")
    val userAndTweet = AvroSerDe.deserialize(file, UserTweetAvro.getClassSchema)
    println (userAndTweet)

  }

}
