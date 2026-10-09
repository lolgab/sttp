package sttp.client4.curl.cats

import cats.effect.{FileDescriptorPoller, IO, Ref, Resource}
import cats.syntax.all._
import sttp.client4.Backend
import sttp.client4.curl.AbstractCurlBackend
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal.{CurlApi, CurlCode, CurlMCode}
import sttp.client4.impl.cats.CatsMonadError
import sttp.client4.wrappers.FollowRedirectsBackend

import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.libc.stdlib
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** A curl backend which never blocks a thread on network I/O.
  *
  * It uses libcurl's multi socket interface (`curl_multi_socket_action`): libcurl reports which sockets it wants to
  * wait on and for how long, and the backend delegates the waiting to the cats-effect runtime's polling system
  * (epoll on Linux, kqueue on macOS), so curl shares the event loop with the rest of the application.
  *
  * libcurl multi handles aren't thread-safe, so each handle is guarded by a fiber-aware mutex (waiting fibers are
  * suspended, no thread is blocked), which is only held for the short, non-blocking libcurl calls. In multi-threaded mode, requests are spread over one multi
  * handle per available processor (the default size of the cats-effect compute pool) to avoid contention; in
  * single-threaded mode, a single handle is used.
  *
  * If the runtime has no [[FileDescriptorPoller]] (e.g. a custom `IORuntime` with the default sleep-based polling
  * system), the backend falls back to driving each transfer from the blocking thread pool.
  */
class CurlCatsAsyncBackend private (drivers: Vector[CurlMultiDriver], next: Ref[IO, Int], verbose: Boolean)
    extends AbstractCurlBackend[IO](new CatsMonadError[IO], verbose)
    with Backend[IO] {

  // the driver which performed a given handle, as the handle has to be cleaned up in a way synchronized with it
  private val performedBy = new ConcurrentHashMap[Long, CurlMultiDriver]()

  override protected def performCurl(c: CurlHandle): IO[CurlCode.CurlCode] =
    if (drivers.isEmpty) CurlCatsAsyncBackend.performBlocking(c)
    else
      next.modify(i => ((i + 1) % drivers.length, i)).flatMap { i =>
        val driver = drivers(i)
        IO(performedBy.put(c.toLong, driver)) *> driver.perform(c).map(CurlCode(_))
      }

  override protected def cleanupCurl(c: CurlHandle): IO[Unit] =
    IO(Option(performedBy.remove(c.toLong))).flatMap {
      case Some(driver) => driver.cleanup(c)
      case None         => super.cleanupCurl(c)
    }
}

object CurlCatsAsyncBackend {

  /** The number of multi handles: one per available processor in multi-threaded mode, one otherwise. */
  private def multiHandles: Int =
    if (LinktimeInfo.isMultithreadingEnabled) Math.max(1, Runtime.getRuntime.availableProcessors()) else 1

  /** Creates a backend. When the resource is released, the requests which are still in progress fail, and the libcurl
    * multi handles are freed.
    *
    * @param verbose
    *   If true, logs request and response summary to the console.
    */
  def resource(verbose: Boolean = false): Resource[IO, Backend[IO]] =
    for {
      poller <- Resource.eval(FileDescriptorPoller.find)
      // without a poller, each transfer gets its own multi handle, see `performBlocking`
      drivers <- poller.fold(Resource.pure[IO, List[CurlMultiDriver]](Nil))(p =>
        List.fill(multiHandles)(CurlMultiDriver.resource(p)).sequence
      )
      next <- Resource.eval(Ref.of[IO, Int](0))
    } yield FollowRedirectsBackend(new CurlCatsAsyncBackend(drivers.toVector, next, verbose))

  /** Fallback when there's no poller: a dedicated multi handle per transfer, driven on the blocking pool in short
    * slices, so that the fiber stays cancelable.
    */
  private def performBlocking(c: CurlHandle): IO[CurlCode.CurlCode] =
    IO {
      val multi = CurlApi.multiInit
      val running = stdlib.calloc(1.toUSize, sizeof[CInt]).asInstanceOf[Ptr[CInt]]
      val rc = multi.addHandle(c)
      (multi, running, rc)
    }.bracket { case (multi, running, rc) =>
      def loop: IO[CurlCode.CurlCode] =
        IO.blocking {
          val pc = multi.perform(running)
          if (pc != CurlMCode.Ok) Some(CurlCode.FailedInit)
          else if (!running == 0) Some(CurlCode(Math.max(multi.infoReadResult(null), 0)))
          else {
            val _ = multi.poll(50, null)
            None
          }
        }.flatMap {
          case Some(code) => IO.pure(code)
          case None       => IO.cede *> loop
        }
      if (rc != CurlMCode.Ok) IO.raiseError(new RuntimeException(s"curl_multi_add_handle failed with $rc"))
      else loop
    } { case (multi, running, _) =>
      IO {
        val _ = multi.removeHandle(c)
        multi.cleanup()
        stdlib.free(running.asInstanceOf[Ptr[Byte]])
      }
    }
}
