package sttp.client4.curl.fs2

import cats.effect.IO
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4._
import sttp.client4.curl.cats.BlockingConvertToFuture
import sttp.client4.impl.fs2.Fs2StreamingTest
import sttp.client4.testing.HttpTest.endpoint

import scala.concurrent.duration._

class CurlFs2StreamingTest extends Fs2StreamingTest with BlockingConvertToFuture {
  // released when the process ends
  override val backend: StreamBackend[IO, Fs2Streams[IO]] =
    CurlFs2Backend.resource().allocated.map(_._1).unsafeRunSync()

  "pause and resume the transfer when the consumer is slower than the network" in {
    // much more than what the backend buffers, so that the transfer is paused a couple of times
    val size = 4 * 1024 * 1024
    basicRequest
      .post(uri"$endpoint/streaming/echo")
      .body(Array.fill[Byte](size)('x'.toByte))
      .response(asStreamAlwaysUnsafe(streams))
      .send(backend)
      .flatMap(_.body.chunkN(256 * 1024).evalTap(_ => IO.sleep(100.millis)).map(_.size.toLong).compile.foldMonoid)
      .toFuture()
      .map(_ shouldBe size.toLong)
  }
}
