package sttp.client.httpclient.monix

import java.nio.ByteBuffer

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import sttp.client._
import sttp.client.httpclient.WebSocketHandler
import sttp.client.impl.monix.{TaskMonadAsyncError, convertMonixTaskToFuture}
import sttp.client.monad.MonadError
import sttp.client.testing.ConvertToFuture
import sttp.client.testing.websocket.WebsocketHandlerTest
import sttp.client.ws.WebSocket

import scala.concurrent.duration._

class MonixWebsocketHandlerTest extends WebsocketHandlerTest[Task, WebSocketHandler] {
  implicit val backend: SttpBackend[Task, Observable[ByteBuffer], WebSocketHandler] =
    HttpClientMonixBackend().runSyncUnsafe()
  implicit val convertToFuture: ConvertToFuture[Task] = convertMonixTaskToFuture
  implicit val monad: MonadError[Task] = TaskMonadAsyncError

  def createHandler: Option[Int] => WebSocketHandler[WebSocket[Task]] = _ => MonixWebSocketHandler(5)

  it should "handle backpressure correctly" in {
    basicRequest
      .get(uri"$wsEndpoint/ws/echo")
      .openWebsocket(createHandler(Some(3)))
      .flatMap { response =>
        val ws = response.result
        send(ws, 1000) >> eventually(10.millis, 100) { ws.isOpen.map(_ shouldBe true) }
      }
      .toFuture()
  }

  override def eventually[T](interval: FiniteDuration, attempts: Int)(f: => Task[T]): Task[T] = {
    (Task.sleep(interval) >> f).onErrorRestart(attempts.toLong)
  }
}
