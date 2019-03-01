package thrift

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.Files

import com.arunma.tweet.{TweetThrift, UserTweetThrift}
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol, TJSONProtocol, TProtocolFactory}
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum, TSerializer}

object ThriftSerDe {

  private def serialize(protocol: ThriftProtocol, source: TBase[_, _ <: TFieldIdEnum]): Array[Byte] = {
    val thriftSerializer = new TSerializer(protocol.factory)
    thriftSerializer.serialize(source)
  }

  private def deserialize(bytes: Array[Byte], placeHolder: TBase[_, _ <: TFieldIdEnum]): Unit = {
    val thriftDeserializer = new TDeserializer()
    thriftDeserializer.deserialize(placeHolder, bytes)
  }

  def serialize(tweet: TweetThrift, file: File): Unit = {
    val byteArray = serialize(ThriftBinaryProtocol, tweet)
    val bos = new BufferedOutputStream(new FileOutputStream(file))
    bos.write(byteArray)
    bos.flush()
  }

  def deserializeFullTweet(file: File): TweetThrift = {
    val byteArray = Files.readAllBytes(file.toPath)
    val placeHolder = new TweetThrift()
    deserialize(byteArray, placeHolder)
    placeHolder
  }

  def deserializePartWithDifferentSchema(file: File): UserTweetThrift = {
    val byteArray = Files.readAllBytes(file.toPath)
    val userAndTweet = new UserTweetThrift()
    deserialize(byteArray, userAndTweet)
    userAndTweet
  }
}

trait ThriftProtocol {
  def factory: TProtocolFactory
}

case object ThriftJSONProtocol extends ThriftProtocol {
  val factory = new TJSONProtocol.Factory()
}

case object ThriftBinaryProtocol extends ThriftProtocol {
  val factory = new TBinaryProtocol.Factory()
}

case object ThriftCompactProtocol extends ThriftProtocol {
  val factory = new TCompactProtocol.Factory()
}