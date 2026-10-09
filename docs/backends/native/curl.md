# Scala Native (curl) backend

A Scala Native (0.5.x) backend implemented using [Curl](https://github.com/curl/curl/blob/master/include/curl/curl.h).

To use, add the following dependency to your project:

```
"com.softwaremill.sttp.client4" %%% "core" % "@VERSION@"
```

and initialize one of the backends:

```scala
import sttp.client4.curl.*

val backend = CurlBackend()
val tryBackend = CurlTryBackend()
```

You need to have an environment with Scala Native [setup](https://scala-native.readthedocs.io/en/latest/user/setup.html)
with additionally installed `libcrypto` (included in OpenSSL) and `curl` in version `7.56.0` or newer.

## scala-cli example

Try the following example:

```scala
// hello.scala

//> using platform native
//> using dep com.softwaremill.sttp.client4::core_native0.5:@VERSION@

import sttp.client4.*
import sttp.client4.curl.CurlBackend

@main def run(): Unit =
  val backend = CurlBackend()
  println(basicRequest.get(uri"http://httpbin.org/ip").send(backend))
```

## ZIO-based
To use in an sbt project, add the following dependency:

```
"com.softwaremill.sttp.client4" %%% "zio" % @VERSION@
```

Create the backend instance for example via `scoped()`
which will also ensure that acquired resources (if any) are released once out of `Scope`:

```scala
//> using platform native
//> using nativeVersion 0.5.10
//> using scala 3
//> using dep com.softwaremill.sttp.client4::zio::@VERSION@

import sttp.client4.*
import sttp.client4.curl.zio.CurlZioBackend
import zio.*

object Main extends ZIOAppDefault:
  def run = for
    backend <- CurlZioBackend.scoped()
    res <- basicRequest.get(uri"http://httpbin.org/ip").send(backend)
    _ <- Console.printLine(res)    
  yield ()
```

## Cats Effect-based (asynchronous)

To use in an sbt project, add the following dependency:

```
"com.softwaremill.sttp.client4" %%% "cats" % @VERSION@
```

`CurlCatsAsyncBackend` is a non-blocking backend, which uses libcurl's
[multi socket interface](https://curl.se/libcurl/c/libcurl-multi.html) (`curl_multi_socket_action`). libcurl reports
which sockets it wants to wait on and for how long; the backend delegates the waiting to the Cats Effect runtime's
polling system (epoll on Linux, kqueue on macOS). No thread is blocked on network I/O, and curl shares the event loop
with the rest of the application.

```scala
//> using platform native
//> using scala 3
//> using dep com.softwaremill.sttp.client4::cats::@VERSION@

import cats.effect.{IO, IOApp}
import sttp.client4.*
import sttp.client4.curl.cats.CurlCatsAsyncBackend

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    CurlCatsAsyncBackend.resource().use { backend =>
      basicRequest.get(uri"http://httpbin.org/ip").send(backend).flatMap(IO.println)
    }
```

The backend is created as a `Resource`: when it is released, requests which are still in progress fail, and the libcurl
multi handles are freed.

### Threading

libcurl multi handles are not thread-safe, so each one is guarded by a fiber-aware mutex (fibers waiting for it are
suspended, no thread is blocked), held only for the short, non-blocking libcurl calls. When Scala Native multithreading
is enabled, requests are spread over one multi handle per available processor (the default size of the Cats Effect
compute pool) to avoid contention. In single-threaded mode, a single multi handle is used.

### Requirements

The backend relies on the `FileDescriptorPoller` of the Cats Effect runtime. The default `IORuntime` on Scala Native
provides one on Linux (epoll) and macOS (kqueue), but not on other platforms (e.g. Windows); there, creating the backend
fails with `No FileDescriptorPoller installed in this IORuntime`.

Limitations: WebSockets are not supported, as with the other curl backends. Responses read using `asInputStream` work,
but they are driven by the shared blocking implementation (`curl_multi_perform` + `curl_multi_poll` on the calling
thread), so they are not asynchronous. Streaming responses are supported by the fs2-based backend, described below.

## fs2-based (asynchronous, streaming)

To use in an sbt project, add the following dependency:

```
"com.softwaremill.sttp.client4" %%% "fs2" % @VERSION@
```

`CurlFs2Backend` has the same properties as `CurlCatsAsyncBackend` (it requires the runtime's `FileDescriptorPoller`,
and is created as a `Resource`), and additionally supports `Stream`-based response bodies:

```scala
//> using platform native
//> using scala 3
//> using dep com.softwaremill.sttp.client4::fs2::@VERSION@

import cats.effect.{IO, IOApp}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.*
import sttp.client4.curl.fs2.CurlFs2Backend

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    CurlFs2Backend.resource().use { backend =>
      basicRequest
        .get(uri"http://httpbin.org/stream-bytes/100000")
        .response(asStreamAlways(Fs2Streams[IO])(_.chunks.map(_.size).compile.foldMonoid))
        .send(backend)
        .flatMap(response => IO.println(s"Received ${response.body} bytes"))
    }
```

The body of a response is not buffered in memory: when the consumer of the stream is slower than the network, the
transfer is paused (`curl_easy_pause`) once about 256 KB are buffered, and resumed when the consumer catches up.
Streams are also accepted as request bodies, but a request body is read into memory before the request is sent.
