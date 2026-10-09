package sttp.client4.curl.cats

import cats.effect.{FiberIO, IO}
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal.CurlOption
import sttp.client4.curl.internal.CurlCode

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.scalanative.libc.string.memcpy
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** The body of a response, which is pushed by libcurl while the transfer is performed by a [[CurlMultiDriver]], and
  * pulled in chunks using [[next]].
  *
  * Backpressure: when more than `HighWater` bytes are buffered, the write callback pauses the transfer
  * (`CURL_WRITEFUNC_PAUSE`) - libcurl then stops reading from the socket. The transfer is resumed once the consumer
  * has drained the buffer below `LowWater`.
  *
  * The state is guarded by the lock of the driver: the write callback is called by libcurl while the lock is held, and
  * everything else uses [[CurlMultiDriver.withLock]].
  */
private[curl] final class CurlResponseStream private (driver: CurlMultiDriver, easy: CurlHandle, id: Long) {
  import CurlResponseStream._

  // --- guarded by the driver's lock ---
  private val chunks = mutable.Queue.empty[Array[Byte]]
  private var buffered = 0
  private var bodyStarted = false
  private var paused = false
  private var done: Option[Either[Throwable, Int]] = None
  private var pending: () => Boolean = null

  @volatile private var transfer: Option[FiberIO[Unit]] = None

  /** Starts the transfer in the background. */
  def start: IO[Unit] =
    driver
      .supervise(
        driver
          .perform(easy)
          .attempt
          .flatMap(result => driver.withLock { done = Some(result); wake() })
      )
      .map(f => transfer = Some(f))

  /** Completes when the response headers are available: when the first part of the body is received, or when the
    * transfer is finished. Fails if the transfer failed before that.
    */
  def awaitHeaders: IO[Unit] =
    waitFor[Option[Throwable]] {
      if (bodyStarted) Some(None)
      else done.map(_.fold(e => Some(e), code => failure(code)))
    }.flatMap(_.fold(IO.unit)(IO.raiseError))

  /** The next chunk of the body, or `None` when the whole body was received. */
  def next: IO[Option[Array[Byte]]] =
    waitFor[Step](pollNext()).flatMap {
      case Chunk(bytes, resume) => (if (resume) driver.resume(easy) else IO.unit).as(Some(bytes))
      case End                  => IO.pure(None)
      case Failed(e)            => IO.raiseError(e)
    }

  /** Stops the transfer, if it's still in progress. The easy handle can be cleaned up after that. */
  def stop: IO[Unit] =
    IO(sinks.remove(id)) *> IO(transfer).flatMap(_.fold(IO.unit)(_.cancel))

  // ---------------------------------------------------------------------------------------------------------------

  private def failure(code: Int): Option[Throwable] =
    if (code == 0) None else Some(new RuntimeException(s"Command failed with status ${CurlCode(code)}"))

  /** Must be called under the lock. */
  private def pollNext(): Option[Step] =
    if (chunks.nonEmpty) {
      val chunk = chunks.dequeue()
      buffered -= chunk.length
      val resume = paused && buffered <= LowWater
      if (resume) paused = false
      Some(Chunk(chunk, resume))
    } else
      done.map {
        case Right(0)    => End
        case Right(code) => Failed(new IOException(s"curl transfer failed with CURLcode $code"))
        case Left(e)     => Failed(e)
      }

  /** Waits until `poll`, which is evaluated under the driver's lock, returns a result. */
  private def waitFor[A](poll: => Option[A]): IO[A] =
    IO.async[A] { cb =>
      driver.withLock {
        def attempt(): Boolean = poll match {
          case Some(a) => cb(Right(a)); true
          case None    => false
        }
        if (attempt()) None
        else {
          pending = () => attempt()
          Some(driver.withLock { pending = null })
        }
      }
    }

  /** Must be called under the lock. */
  private def wake(): Unit =
    if (pending != null && pending()) pending = null

  /** The write callback, called by libcurl under the driver's lock. */
  private def onWrite(data: Ptr[Byte], length: Int): CSize =
    if (buffered >= HighWater) {
      paused = true
      wake()
      PauseWrite
    } else {
      val chunk = new Array[Byte](length)
      if (length > 0) { val _ = memcpy(chunk.atUnsafe(0), data, length.toUSize) }
      chunks.enqueue(chunk)
      buffered += length
      bodyStarted = true
      wake()
      length.toUSize
    }
}

private[curl] object CurlResponseStream {
  private val HighWater = 256 * 1024
  private val LowWater = 64 * 1024
  private val PauseWrite: CSize = 0x10000001.toUSize // CURL_WRITEFUNC_PAUSE

  private sealed trait Step
  private final case class Chunk(bytes: Array[Byte], resume: Boolean) extends Step
  private case object End extends Step
  private final case class Failed(e: Throwable) extends Step

  private val nextId = new AtomicLong(0)
  private val sinks = new ConcurrentHashMap[Long, CurlResponseStream]()

  private val writeCallback: CFuncPtr4[Ptr[Byte], CSize, CSize, Ptr[Byte], CSize] =
    (data: Ptr[Byte], size: CSize, nmemb: CSize, userp: Ptr[Byte]) => {
      val sink = sinks.get(CurlMultiDriver.ptrToId(userp))
      // returning less than the length aborts the transfer
      if (sink == null) 0.toUSize else sink.onWrite(data, (size * nmemb).toInt)
    }

  /** Makes libcurl write the body of the response to the returned stream, which is not started yet. */
  def attach(driver: CurlMultiDriver, easy: CurlHandle): CurlResponseStream = {
    val id = nextId.incrementAndGet()
    val stream = new CurlResponseStream(driver, easy, id)
    sinks.put(id, stream)
    val _ = (
      easy.option(CurlOption.WriteFunction, writeCallback),
      easy.option(CurlOption.WriteData, CurlMultiDriver.idToPtr(id))
    )
    stream
  }
}
