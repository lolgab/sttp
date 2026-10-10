package sttp.client4.curl.cats

import cats.effect.{IO, Resource}
import sttp.client4.Backend
import sttp.client4.curl.AbstractCurlBackend
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal.CurlCode
import sttp.client4.impl.cats.CatsMonadError
import sttp.client4.wrappers.FollowRedirectsBackend


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
  * The runtime must provide a [[cats.effect.FileDescriptorPoller]]. The default `IORuntime` does on Linux (epoll) and macOS
  * (kqueue), but not on other platforms (e.g. Windows): creating the backend fails there.
  */
class CurlCatsAsyncBackend private (drivers: CurlDrivers, verbose: Boolean)
    extends AbstractCurlBackend[IO](new CatsMonadError[IO], verbose)
    with Backend[IO] {

  override protected def performCurl(c: CurlHandle): IO[CurlCode.CurlCode] =
    drivers.assign(c).flatMap(_.perform(c)).map(CurlCode(_))

  override protected def cleanupCurl(c: CurlHandle): IO[Unit] = drivers.cleanup(c, super.cleanupCurl(c))
}

object CurlCatsAsyncBackend {

  /** Creates a backend. When the resource is released, the requests which are still in progress fail, and the libcurl
    * multi handles are freed.
    *
    * @param verbose
    *   If true, logs request and response summary to the console.
    */
  def resource(verbose: Boolean = false): Resource[IO, Backend[IO]] =
    CurlDrivers.resource.map(drivers => FollowRedirectsBackend(new CurlCatsAsyncBackend(drivers, verbose)))
}
