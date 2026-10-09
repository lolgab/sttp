package sttp.client4.curl.fs2

import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.io.file.Files
import fs2.{Chunk, Stream}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4._
import sttp.client4.curl.{DetachedResources, GenericCurlBackend}
import sttp.client4.curl.cats.{CurlDrivers, CurlResponseStream}
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal.{CurlCode, CurlInfo, CurlSpaces}
import sttp.client4.impl.cats.CatsMonadAsyncError
import sttp.client4.internal.{BodyFromResponseAs, SttpFile}
import sttp.client4.wrappers.FollowRedirectsBackend
import sttp.client4.ws.{GotAWebSocketException, NotAWebSocketException}
import sttp.model.{ResponseMetadata, StatusCode}
import sttp.monad.MonadError

import scala.scalanative.libc.stdlib.free
import scala.scalanative.unsafe._

/** A curl backend which never blocks a thread on network I/O (see [[sttp.client4.curl.cats.CurlCatsAsyncBackend]]), and
  * additionally supports streaming response bodies (and request bodies, which are however buffered in memory) using
  * fs2.
  *
  * While the body of a response is streamed, its transfer is paused when the consumer is slower than the network.
  */
class CurlFs2Backend private (drivers: CurlDrivers, verbose: Boolean)
    extends GenericCurlBackend[IO, Fs2Streams[IO]](new CatsMonadAsyncError[IO], verbose)
    with StreamBackend[IO, Fs2Streams[IO]] {

  override protected def performCurl(c: CurlHandle): IO[CurlCode.CurlCode] =
    drivers.assign(c).flatMap(_.perform(c)).map(CurlCode(_))

  override protected def cleanupCurl(c: CurlHandle): IO[Unit] = drivers.cleanup(c, super.cleanupCurl(c))

  override protected def isStreamResponse(leaf: GenericResponseAs[_, _]): Boolean =
    leaf match {
      case _: ResponseAsStream[_, _, _, _] | _: ResponseAsStreamUnsafe[_, _] => true
      case _                                                                  => false
    }

  override protected def streamBodyToBytes(stream: Any): IO[Array[Byte]] =
    stream.asInstanceOf[Stream[IO, Byte]].compile.to(Chunk).map(_.toArray)

  override protected def handleStream[T](
      request: GenericRequest[T, R],
      curl: CurlHandle,
      spaces: CurlSpaces,
      resources: DetachedResources
  ): IO[Response[T]] =
    IO.uncancelable { poll =>
      for {
        driver <- drivers.assign(curl)
        body = CurlResponseStream.attach(driver, curl)
        released <- IO.ref(false)
        // the transfer, the handle and the memory are released exactly once, when the response body is not needed
        // anymore: when it's consumed or when the consumer gives up, or if getting the response fails
        release = released.getAndSet(true).flatMap { alreadyReleased =>
          (body.stop *> cleanupCurl(curl) *> IO {
            free((!spaces.bodyResp)._1)
            free((!spaces.headersResp)._1)
            free(spaces.bodyResp.asInstanceOf[Ptr[Byte]])
            free(spaces.headersResp.asInstanceOf[Ptr[Byte]])
            free(spaces.httpCode.asInstanceOf[Ptr[Byte]])
            resources.release()
          }).unlessA(alreadyReleased)
        }
        response <- (for {
          _ <- body.start
          _ <- poll(body.awaitHeaders)
          // libcurl handles can't be used outside of the lock while their transfer is in progress
          info <- driver.withLock {
            curl.info(CurlInfo.ResponseCode, spaces.httpCode)
            ((!spaces.httpCode).toInt, fromCString((!spaces.headersResp)._1))
          }
          code = info._1
          parsed = parseHeadersAndStatus(info._2)
          statusText = parsed._1
          headers = parsed._2
          meta = ResponseMetadata(StatusCode(code), statusText, headers)
          _ <- IO(request.options.onBodyReceived(meta))
          stream = Stream
            .repeatEval(body.next)
            .unNoneTerminate
            .flatMap(bytes => Stream.chunk(Chunk.array(bytes)))
            .onFinalize(release)
          responseBody <- bodyFromResponseAs(request.response, meta, Left((stream, release)))
        } yield Response[T](
          body = responseBody,
          code = StatusCode(code),
          statusText = statusText,
          headers = headers,
          history = Nil,
          request = request.onlyMetadata
        )).onError { case _ => release }.onCancel(release)
      } yield response
    }

  /** The raw response is the body stream, together with the action which releases the transfer. */
  private lazy val bodyFromResponseAs =
    new BodyFromResponseAs[IO, (Stream[IO, Byte], IO[Unit]), Nothing, Stream[IO, Byte]]()(
      new CatsMonadAsyncError[IO]: MonadError[IO]
    ) {
      override protected def withReplayableBody(
          response: (Stream[IO, Byte], IO[Unit]),
          replayableBody: Either[Array[Byte], SttpFile]
      ): IO[(Stream[IO, Byte], IO[Unit])] =
        IO.pure(replayableBody match {
          case Left(bytes)     => (Stream.chunk(Chunk.array(bytes)), IO.unit)
          case Right(sttpFile) => (Files[IO].readAll(sttpFile.toPath, 32 * 1024), IO.unit)
        })

      override protected def regularIgnore(response: (Stream[IO, Byte], IO[Unit])): IO[Unit] =
        response._1.compile.drain

      override protected def regularAsByteArray(response: (Stream[IO, Byte], IO[Unit])): IO[Array[Byte]] =
        response._1.compile.to(Chunk).map(_.toArray)

      override protected def regularAsFile(response: (Stream[IO, Byte], IO[Unit]), file: SttpFile): IO[SttpFile] =
        response._1.through(Files[IO].writeAll(file.toPath)).compile.drain.as(file)

      override protected def regularAsStream(
          response: (Stream[IO, Byte], IO[Unit])
      ): IO[(Stream[IO, Byte], () => IO[Unit])] =
        IO.pure((response._1, () => response._2))

      override protected def handleWS[T](
          responseAs: GenericWebSocketResponseAs[T, _],
          meta: ResponseMetadata,
          ws: Nothing
      ): IO[T] = ws

      override protected def cleanupWhenNotAWebSocket(
          response: (Stream[IO, Byte], IO[Unit]),
          e: NotAWebSocketException
      ): IO[Unit] = response._2

      override protected def cleanupWhenGotWebSocket(response: Nothing, e: GotAWebSocketException): IO[Unit] = response
    }
}

object CurlFs2Backend {

  /** Creates a backend. When the resource is released, the requests which are still in progress fail, and the libcurl
    * multi handles are freed.
    *
    * @param verbose
    *   If true, logs request and response summary to the console.
    */
  def resource(verbose: Boolean = false): Resource[IO, StreamBackend[IO, Fs2Streams[IO]]] =
    CurlDrivers.resource.map(drivers => FollowRedirectsBackend(new CurlFs2Backend(drivers, verbose)))
}
