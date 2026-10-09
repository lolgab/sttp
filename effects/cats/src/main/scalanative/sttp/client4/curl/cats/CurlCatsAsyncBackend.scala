package sttp.client4.curl.cats

import cats.effect.{FileDescriptorPoller, IO, Ref, Resource}
import cats.syntax.all._
import sttp.client4.Backend
import sttp.client4.curl.AbstractCurlBackend
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal.CurlCode
import sttp.client4.impl.cats.CatsMonadError
import sttp.client4.wrappers.FollowRedirectsBackend

import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.meta.LinktimeInfo

/** A curl backend which never blocks a thread on network I/O.
  *
  * It uses libcurl's multi socket interface (`curl_multi_socket_action`): libcurl reports which sockets it wants to
  * wait on and for how long, and the backend delegates the waiting to the cats-effect runtime's polling system
  * (epoll on Linux, kqueue on macOS), so curl shares the event loop with the rest of the application.
  *
  * libcurl multi handles aren't thread-safe, so each handle is guarded by a fiber-aware mutex (waiting fibers are
  * suspended, no thread is blocked), which is only held for the short, non-blocking libcurl calls. In multi-threaded
  * mode, requests are spread over one multi handle per available processor (the default size of the cats-effect
  * compute pool) to avoid contention; in single-threaded mode, a single handle is used.
  *
  * The runtime must provide a [[FileDescriptorPoller]]. The default `IORuntime` does on Linux (epoll) and macOS
  * (kqueue), but not on other platforms (e.g. Windows): creating the backend fails there.
  */
class CurlCatsAsyncBackend private (drivers: Vector[CurlMultiDriver], next: Ref[IO, Int], verbose: Boolean)
    extends AbstractCurlBackend[IO](new CatsMonadError[IO], verbose)
    with Backend[IO] {

  // the driver which performed a given handle, as the handle has to be cleaned up in a way synchronized with it
  private val performedBy = new ConcurrentHashMap[Long, CurlMultiDriver]()

  override protected def performCurl(c: CurlHandle): IO[CurlCode.CurlCode] =
    next.modify(i => ((i + 1) % drivers.length, i)).flatMap { i =>
      val driver = drivers(i)
      IO(performedBy.put(c.toLong, driver)) *> driver.perform(c).map(CurlCode(_))
    }

  override protected def cleanupCurl(c: CurlHandle): IO[Unit] =
    IO(Option(performedBy.remove(c.toLong))).flatMap {
      case Some(driver) => driver.cleanup(c)
      case None         => super.cleanupCurl(c) // the transfer was never started
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
      poller <- Resource.eval(FileDescriptorPoller.get)
      drivers <- List.fill(multiHandles)(CurlMultiDriver.resource(poller)).sequence
      next <- Resource.eval(Ref.of[IO, Int](0))
    } yield FollowRedirectsBackend(new CurlCatsAsyncBackend(drivers.toVector, next, verbose))
}
