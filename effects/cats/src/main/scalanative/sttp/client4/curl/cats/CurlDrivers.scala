package sttp.client4.curl.cats

import cats.effect.{FileDescriptorPoller, IO, Ref, Resource}
import cats.syntax.all._
import sttp.client4.curl.internal.CurlApi._

import java.util.concurrent.ConcurrentHashMap
import scala.scalanative.meta.LinktimeInfo

/** The multi handles (drivers) used by a curl backend, spreading the transfers over them. */
private[curl] final class CurlDrivers private (drivers: Vector[CurlMultiDriver], next: Ref[IO, Int]) {

  // the driver which performs a given handle, as the handle has to be cleaned up in a way synchronized with it
  private val assigned = new ConcurrentHashMap[Long, CurlMultiDriver]()

  /** Picks the driver for the given handle, and remembers the choice until the handle is cleaned up. */
  def assign(c: CurlHandle): IO[CurlMultiDriver] =
    next.modify(i => ((i + 1) % drivers.length, i)).flatMap { i =>
      val driver = drivers(i)
      IO(assigned.put(c.toLong, driver)).as(driver)
    }

  /** Cleans up the handle, in a way synchronized with the driver which performed it. */
  def cleanup(c: CurlHandle, default: IO[Unit]): IO[Unit] =
    IO(Option(assigned.remove(c.toLong))).flatMap {
      case Some(driver) => driver.cleanup(c)
      case None         => default // the transfer was never started
    }
}

private[curl] object CurlDrivers {

  /** The number of multi handles: one per available processor in multi-threaded mode, one otherwise. */
  private def multiHandles: Int =
    if (LinktimeInfo.isMultithreadingEnabled) Math.max(1, Runtime.getRuntime.availableProcessors()) else 1

  /** Fails if the runtime doesn't provide a [[FileDescriptorPoller]]. */
  def resource: Resource[IO, CurlDrivers] =
    for {
      poller <- Resource.eval(FileDescriptorPoller.get)
      drivers <- List.fill(multiHandles)(CurlMultiDriver.resource(poller)).sequence
      next <- Resource.eval(Ref.of[IO, Int](0))
    } yield new CurlDrivers(drivers.toVector, next)
}
