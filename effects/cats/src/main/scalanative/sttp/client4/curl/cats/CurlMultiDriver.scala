package sttp.client4.curl.cats

import cats.effect.{FiberIO, IO, Ref, Resource}
import cats.effect.std.{Mutex, Supervisor}
import cats.syntax.all._
import cats.effect.{FileDescriptorPoller, FileDescriptorPollHandle}
import sttp.client4.curl.internal.CurlApi._
import sttp.client4.curl.internal._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.duration._
import scala.scalanative.runtime.{fromRawPtr, toRawPtr, Intrinsics}
import scala.scalanative.unsafe._

/** Drives a single libcurl multi handle using the `curl_multi_socket_action` interface, integrated with cats-effect's
  * [[FileDescriptorPoller]] (epoll/kqueue). No thread is ever blocked waiting for network I/O: curl tells us (through
  * the socket callback) which sockets it is interested in, we register them with the runtime's poller, and call
  * `curl_multi_socket_action` when they become ready. Timeouts requested by curl (timer callback) are scheduled with
  * `IO.sleep`.
  *
  * Thread-safety: libcurl multi handles can be used from different threads, but never concurrently. Every call into the
  * multi handle is made while holding `lock`, a fiber-aware [[Mutex]]: a fiber waiting for it is suspended, no (compute)
  * thread is ever blocked on a monitor. The callbacks are invoked synchronously from within the libcurl calls, so they
  * run under the lock as well, and only record state. All the other work (starting watchers, completing requests)
  * happens outside of the lock. The libcurl multi calls are documented to be non-blocking, so they are suspended with
  * `IO.apply` rather than `IO.blocking`. Fibers can freely migrate between the worker threads.
  *
  * Which state is a `Ref` and which is not: the state touched by libcurl's callbacks (`sockets`, `completions`, the timer
  * request, `closed`) is plain mutable state guarded by `lock`, as the callbacks are synchronous C functions which can't
  * run `IO`. The fibers watching the sockets are effectful, immutable state, kept in a `Ref` and updated while holding
  * `reconcileMutex`, which serializes the effectful reconciliation.
  *
  * To scale across worker threads, the backend uses several drivers.
  */
private[cats] final class CurlMultiDriver private (
    id: Long,
    multi: CurlMultiHandle,
    runningPtr: Ptr[CInt],
    easyOut: Ptr[Ptr[Curl]],
    poller: FileDescriptorPoller,
    supervisor: Supervisor[IO],
    lock: Mutex[IO],
    reconcileMutex: Mutex[IO],
    fibers: Ref[IO, CurlMultiDriver.Fibers]
) {
  import CurlMultiDriver._

  // --- state guarded by `lock`, mutated by libcurl's callbacks ---
  private var closed = false
  // set when libcurl changed the sockets or the timer, i.e. when the watchers have to be reconciled
  private var dirty = false
  private final class Sock(var what: Int, val gen: Long, var gained: Int = 0)
  private val sockets = mutable.HashMap.empty[Int, Sock]
  private var socketGen = 0L
  private var timeoutMs: Long = -1L
  private var timerGen = 0L
  private final class Completion(val easy: CurlHandle, val cb: Either[Throwable, Int] => Unit)
  private val completions = mutable.HashMap.empty[Long, Completion]

  // ---------------------------------------------------------------------------------------------------------------
  // callbacks (invoked by libcurl, under `lock`)

  private def onSocket(fd: Int, what: Int): Unit = {
    dirty = true
    if (what == PollRemove) { val _ = sockets.remove(fd) }
    else
      sockets.get(fd) match {
        case Some(s) =>
          s.gained |= (what & ~s.what)
          s.what = what
        case None =>
          socketGen += 1
          val _ = sockets.put(fd, new Sock(what, socketGen))
      }
  }

  private def onTimer(ms: Long): Unit = {
    dirty = true
    timeoutMs = ms
    timerGen += 1
  }

  // ---------------------------------------------------------------------------------------------------------------
  // public API

  /** Performs the transfer of `easy`, which must be fully configured. The handle stays owned by the caller, who has to
    * clean it up after this effect completes, fails or is cancelled. */
  def perform(easy: CurlHandle): IO[Int] = IO.async[Int] { cb =>
    val key = easy.toLong
    locked {
      if (closed) { cb(Left(new IllegalStateException("The curl backend is closed"))); false }
      else {
        val _ = completions.put(key, new Completion(easy, cb))
        val rc = multi.addHandle(easy)
        if (rc != CurlMCode.Ok) {
          val _ = completions.remove(key)
          cb(Left(new RuntimeException(s"curl_multi_add_handle failed with $rc")))
          false
        } else true
      }
    }.flatMap { added =>
      if (added) reconcile.as(Some(cancel(easy, key)))
      else IO.pure(None)
    }
  }

  /** Cleans up an easy handle which was performed by this driver. libcurl handles which belong to the same multi handle
    * share state, so this can't happen concurrently with other calls into the multi handle.
    */
  def cleanup(easy: CurlHandle): IO[Unit] = locked(easy.cleanup())

  /** Stops the driver: transfers still in progress fail. The watcher fibers are cancelled by the supervisor. */
  private def shutdown: IO[Unit] =
    locked {
      closed = true
      registry.remove(id)
      val failed = completions.values.toList
      completions.clear()
      failed.foreach(c => multi.removeHandle(c.easy))
      failed
    }.flatMap(_.traverse_(c => IO(c.cb(Left(new IllegalStateException("The curl backend is closed"))))))

  // ---------------------------------------------------------------------------------------------------------------
  // internals

  private def cancel(easy: CurlHandle, key: Long): IO[Unit] =
    locked {
      if (!closed && completions.remove(key).isDefined) {
        val _ = multi.removeHandle(easy)
      }
    } *> reconcile

  private def locked[A](body: => A): IO[A] = lock.lock.surround(IO(body))

  /** Runs a libcurl multi call under the lock if `cond` holds, then completes the finished transfers and, if libcurl
    * changed the sockets or the timer, brings the watchers/timer in line with that. Only waiting for the lock is
    * cancelable: once the call is made, curl must not be interrupted half-way and the completions have to be delivered.
    * Returns whether the call was made.
    */
  private def pumpIf(cond: => Boolean)(call: => Unit): IO[Boolean] =
    IO.uncancelable { poll =>
      poll(lock.lock.allocated).flatMap { case (_, release) =>
        IO {
          if (!closed && cond) {
            call
            val needsReconcile = dirty
            Some((collectDone(), needsReconcile))
          } else None
        }.attempt.flatTap(_ => release).rethrow
      }.flatMap {
        case None => IO.pure(false)
        case Some((done, needsReconcile)) =>
          IO(done.foreach { case (cb, code) => cb(Right(code)) }) *> reconcile.whenA(needsReconcile).as(true)
      }
    }

  private def pump(call: => Unit): IO[Unit] = pumpIf(true)(call).void

  /** Must be called under the lock. */
  private def collectDone(): List[(Either[Throwable, Int] => Unit, Int)] = {
    var res = List.empty[(Either[Throwable, Int] => Unit, Int)]
    var code = multi.infoReadResult(easyOut)
    while (code != -1) {
      val easy = !easyOut
      val _ = multi.removeHandle(easy)
      completions.remove(easy.toLong).foreach(c => res = (c.cb, code) :: res)
      code = multi.infoReadResult(easyOut)
    }
    res
  }

  /** Brings the fibers watching the sockets and the timer in line with what curl asked for. */
  private def reconcile: IO[Unit] =
    reconcileMutex.lock.surround {
      for {
        snapshot <- locked {
          val desired = sockets.map { case (fd, s) => (fd, s.gen) }.toMap
          val kicks = sockets.collect { case (fd, s) if s.gained != 0 => (fd, s.gained) }.toList
          sockets.valuesIterator.foreach(_.gained = 0)
          dirty = false
          Snapshot(desired, kicks, timerGen, timeoutMs)
        }
        current <- fibers.get
        // watchers of sockets which are gone, or were closed and reopened under the same fd number
        stale = current.watchers.collect { case (fd, w) if !snapshot.desired.get(fd).contains(w.gen) => fd }.toList
        _ <- stale.traverse_(fd => cancelAsync(current.watchers(fd).fiber))
        kept = current.watchers -- stale
        retired = current.retired ++ stale.map(fd => (fd, current.watchers(fd).gen) -> current.watchers(fd).fiber)
        toStart = snapshot.desired.filterNot { case (fd, _) => kept.contains(fd) }.toList
        started <- toStart.traverse { case (fd, gen) =>
          startWatcher(fd, gen, retired.collect { case ((`fd`, _), f) => f }.toList).map(f => fd -> Watcher(gen, f))
        }
        timerChange <-
          if (current.timer.exists(_._1 == snapshot.timerGen)) IO.pure((current.timer, false))
          else {
            val cancelOld = current.timer.traverse_(t => cancelAsync(t._2))
            if (snapshot.timeoutMs < 0) cancelOld.as((None, false))
            else if (snapshot.timeoutMs == 0) cancelOld.as((None, true))
            else
              cancelOld *> supervisor.supervise(IO.sleep(snapshot.timeoutMs.millis) *> pump(timeoutAction())).map {
                f => (Some((snapshot.timerGen, f)), false)
              }
          }
        (timer, fireNow) = timerChange
        _ <- fibers.set(Fibers(kept ++ started, retired -- toStart.flatMap { case (fd, _) => retired.keys.filter(_._1 == fd) }, timer))
      } yield (snapshot.kicks, fireNow)
    }.flatMap { case (kicks, fireNow) =>
      // the interest of a socket was extended: its readiness edge might have been consumed already
      val kick = kicks.traverse_ { case (fd, gained) =>
        serve(fd, PollIn).whenA((gained & PollIn) != 0) *> serve(fd, PollOut).whenA((gained & PollOut) != 0)
      }
      pump(timeoutAction()).whenA(fireNow) *> kick
    }

  /** Cancels without waiting: the fiber being cancelled might be the one running this code. */
  private def cancelAsync(f: FiberIO[Unit]): IO[Unit] = f.cancel.start.void

  private def timeoutAction(): Unit = { val _ = CCurl.multiSocketAction(multi, SocketTimeout, 0, runningPtr) }

  /** Watches the socket until cancelled. The `previous` watchers of the same fd number (the socket was closed and a
    * new one opened) are awaited first, so that they don't deregister the new socket.
    */
  private def startWatcher(fd: Int, gen: Long, previous: List[FiberIO[Unit]]): IO[FiberIO[Unit]] = {
    val blocked: IO[Either[Unit, Nothing]] = IO.pure(Left(()))
    def loop(h: FileDescriptorPollHandle): IO[Unit] =
      IO.race(
        h.pollReadRec[Unit, Nothing](())(_ => serve(fd, PollIn) *> blocked),
        h.pollWriteRec[Unit, Nothing](())(_ => serve(fd, PollOut) *> blocked)
      ).void
    // when the socket can't be watched, curl is told that it failed, so that the transfers fail instead of hanging
    val failSocket = pump { val _ = CCurl.multiSocketAction(multi, fd, CselectErr, runningPtr) }
    // the watcher is not retired anymore after it finishes; this is serialized with `reconcile` via the mutex
    val forget = reconcileMutex.lock.surround(fibers.update(f => f.copy(retired = f.retired - ((fd, gen)))))
    supervisor.supervise(
      (previous.traverse_(_.join) *>
        poller
          .registerFileDescriptor(fd, monitorReadReady = true, monitorWriteReady = true)
          .attempt
          .use {
            case Left(_)  => failSocket
            case Right(h) => loop(h).handleErrorWith(_ => failSocket)
          })
        // deregistering fails if curl already closed the socket; there is nothing to do about it then
        .handleError(_ => ())
        .guarantee(forget)
    )
  }

  /** The readiness notifications are edge-triggered (epoll), so we have to keep driving curl as long as the socket
    * stays ready and curl is interested in it - curl might not drain it in one go.
    */
  private def serve(fd: Int, flag: Int): IO[Unit] = {
    def interested: Boolean = sockets.get(fd).exists(s => (s.what & flag) != 0)
    def go(n: Int): IO[Unit] =
      pumpIf(interested && CCurl.fdReady(fd, flag) != 0) { val _ = CCurl.multiSocketAction(multi, fd, flag, runningPtr) }
        .flatMap {
          case false => IO.unit
          case true  => if (n >= 31) IO.cede *> go(0) else go(n + 1)
        }
    go(0)
  }

}

private[cats] object CurlMultiDriver {
  // CURL_POLL_* (also the same values as CURL_CSELECT_IN/OUT)
  private val PollIn = 1
  private val PollOut = 2
  private val PollRemove = 4
  private val SocketTimeout = -1
  private val CselectErr = 4

  // CURLMOPT_*
  private val MultiSocketFunction = 20001
  private val MultiSocketData = 10002
  private val MultiTimerFunction = 20004
  private val MultiTimerData = 10005

  private val nextId = new AtomicLong(0)
  private val registry = new ConcurrentHashMap[Long, CurlMultiDriver]()

  private def idToPtr(id: Long): Ptr[Byte] = fromRawPtr[Byte](Intrinsics.castLongToRawPtr(id))
  private def ptrToId(p: Ptr[Byte]): Long = Intrinsics.castRawPtrToLong(toRawPtr(p))

  private val socketCallback: CFuncPtr5[Ptr[Curl], CInt, CInt, Ptr[Byte], Ptr[Byte], CInt] =
    (_: Ptr[Curl], fd: CInt, what: CInt, userp: Ptr[Byte], _: Ptr[Byte]) => {
      val d = registry.get(ptrToId(userp))
      if (d != null) d.onSocket(fd, what)
      0
    }

  private val timerCallback: CFuncPtr3[Ptr[CurlM], CLong, Ptr[Byte], CInt] =
    (_: Ptr[CurlM], ms: CLong, userp: Ptr[Byte]) => {
      val d = registry.get(ptrToId(userp))
      if (d != null) d.onTimer(ms.toLong)
      0
    }

  private final case class Watcher(gen: Long, fiber: FiberIO[Unit])

  /** The effectful part of the driver's state. */
  private final case class Fibers(
      watchers: Map[Int, Watcher],
      // fibers of removed watchers; a new watcher on the same fd number waits for the old one to deregister first
      retired: Map[(Int, Long), FiberIO[Unit]],
      timer: Option[(Long, FiberIO[Unit])]
  )

  private final case class Snapshot(desired: Map[Int, Long], kicks: List[(Int, Int)], timerGen: Long, timeoutMs: Long)

  /** Creates a driver with its own multi handle. When released, the driver fails the transfers in progress, stops the
    * fibers watching the sockets, and frees the multi handle.
    */
  def resource(poller: FileDescriptorPoller): Resource[IO, CurlMultiDriver] =
    for {
      id <- Resource.eval(IO(nextId.incrementAndGet()))
      zone <- Resource.make(IO(Zone.open()))(z => IO(z.close()))
      runningPtr <- Resource.eval(IO { implicit val z: Zone = zone; alloc[CInt]() })
      easyOut <- Resource.eval(IO { implicit val z: Zone = zone; alloc[Ptr[Curl]]() })
      // released after the driver and the fibers, when nobody can call into the handle anymore
      multi <- Resource.make(IO(CurlApi.multiInit))(m => IO(m.cleanup()))
      lock <- Resource.eval(Mutex[IO])
      reconcileMutex <- Resource.eval(Mutex[IO])
      fibers <- Resource.eval(Ref.of[IO, Fibers](Fibers(Map.empty, Map.empty, None)))
      supervisor <- Supervisor[IO](await = false)
      driver <- Resource.make(IO {
        val d = new CurlMultiDriver(id, multi, runningPtr, easyOut, poller, supervisor, lock, reconcileMutex, fibers)
        registry.put(id, d)
        val userp = idToPtr(id)
        val _ = (
          CCurl.multiSetoptPtr(multi, MultiSocketFunction, CFuncPtr.toPtr(socketCallback)),
          CCurl.multiSetoptPtr(multi, MultiSocketData, userp),
          CCurl.multiSetoptPtr(multi, MultiTimerFunction, CFuncPtr.toPtr(timerCallback)),
          CCurl.multiSetoptPtr(multi, MultiTimerData, userp)
        )
        d
      })(d => d.shutdown)
    } yield driver
}
