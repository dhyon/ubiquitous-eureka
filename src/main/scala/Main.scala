import akka.actor.ActorSystem
import akka.stream.scaladsl._

import java.io.InputStream
import java.net.URL
import java.security.{DigestInputStream, MessageDigest}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{BufferedSource, Source => FileSource}
import scala.util.Using

object Main extends App {

  implicit val system: ActorSystem = ActorSystem("MyActorSystem")

  // read in lines from file
  val urls = Using(FileSource.fromFile("urls.txt")) { in: BufferedSource =>
    in.getLines().toList
  }.getOrElse(List.empty)

  // get max number of parallel requests from args, default to 10
  val parallelism = args.headOption.getOrElse("10").toInt

  // fire off futures and print results, maintaining input order
  Source.fromIterator(() => urls.iterator)
    .mapAsync(parallelism = parallelism)(getMd5)
    .runForeach(println)
    .onComplete(_ => System.exit(0))


  /**
   * @param url URL string
   * @return md5 hash of resource
   */
  def getMd5(url: String): Future[String] = Future {
    //    println(s"debug: start for ${url}")
    val md5 = MessageDigest.getInstance("MD5")
    // read in bytes to message digest
    Using(new URL(url).openStream) { in: InputStream =>
      val buffer = new Array[Byte](8192)
      val dis = new DigestInputStream(in, md5)
      try {
        while (dis.read(buffer) != -1) {}
      } finally {
        dis.close()
      }
    }
    //    println(s"debug: end for ${url}")
    md5.digest.map("%02x".format(_)).mkString
  }
}