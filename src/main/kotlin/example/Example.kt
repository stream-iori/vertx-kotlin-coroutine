package example

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpServer
import io.vertx.ext.coroutine.*

/**
 * Created by stream.
 */

class ExampleVerticle : CoroutineVerticle() {
  override fun start() {
    //make all the handler under coroutine.
    runVertxCoroutine {
      syncEventExample()
      syncResultExample()
      streamExample()
      coroutineHandlerExample()
      futureToAwait()
    }
  }

  //asyncEvent
  private suspend fun syncEventExample() {
    asyncEvent<Long> { h -> vertx.setTimer(2000L, h) }.await()
    println("This would be fire in 2s.")
  }

  //asyncResult
  private suspend fun syncResultExample() {
    val consumer = vertx.eventBus().localConsumer<String>("someAddress")
    consumer.handler { message ->
      println("consumer receive message ${message.body()}")
      message.reply("pong")
    }
    //wait consumer to complete register synchronously
    asyncResult<Void> { h -> consumer.completionHandler(h) }.await()

    //send message and wait reply synchronously
    val reply = asyncResult<Message<String>> { h -> vertx.eventBus().send("someAddress", "ping", h) }.await()
    println("Receive reply ${reply.body()}")
  }

  //streamAdaptor
  private suspend fun streamExample() {
    val adaptor = streamAdaptor<Message<Int>>()
    vertx.eventBus().localConsumer<Int>("someAddress").handler(adaptor)

    //send 10 message to consumer
    for (i in 0..10) vertx.eventBus().send("someAddress", i)

    //Receive 10 message from the consumer
    for (i in 0..10) {
      val message = adaptor.receive()
      println("got message: ${message.body()}")
    }
  }

  //Convert future to await
  private suspend fun futureToAwait() {
    val httpServerFuture = Future.future<HttpServer>()
    vertx.createHttpServer().requestHandler({ _ -> }).listen(8000, httpServerFuture.completer())
    //we can get httpServer by await on future instance.
    val httpServer = httpServerFuture.await()

    //await composite future.
    val result = CompositeFuture.all(httpServerFuture, httpServerFuture).await()
    if (result.succeeded()) {
      println("all have start up.")
    } else {
      result.cause().printStackTrace()
    }
  }

  //run coroutine in requestHandler
  private fun coroutineHandlerExample() {
    vertx.createHttpServer().requestHandler { req ->
      runVertxCoroutine {
        val timerID = asyncEvent<Long> { h -> vertx.setTimer(2000L, h) }.await()
        req.response().end("Hello, this is timerID $timerID")
      }
    }.listen(8081)
  }
}

fun main(args: Array<String>) {
  val vertx = Vertx.vertx()
  vertx.deployVerticle(ExampleVerticle())

  //embed
  initVertxToCoroutine(vertx)
  runVertxCoroutine {
    asyncEvent<Long> { h -> vertx.setTimer(1000L, h) }.await()
    println("will be fired in 1s.")
  }
}