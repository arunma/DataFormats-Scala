package thrift

import java.io.File

import com.arunma.tweet.TweetThrift

object ThriftExample {

  def main(args: Array[String]): Unit = {

    val file = new File ("serialized_thrift_file.t")
    //Serialize
    val tweet = new TweetThrift(1, 123, "Saturday 8th, June", "arunma")
    tweet.setText("First tweet")
    ThriftSerDe.serialize(tweet, file)

    //Deserialize with exact schema
    println("Deserializing Full Tweet")
    val returnTweet = ThriftSerDe.deserializeFullTweet(file)
    println (returnTweet)

    //Deserialize with minimal schema
    println("Deserializing Minimal Tweet")
    val userAndTweet = ThriftSerDe.deserializePartWithDifferentSchema(file)
    println (userAndTweet)

  }

}
