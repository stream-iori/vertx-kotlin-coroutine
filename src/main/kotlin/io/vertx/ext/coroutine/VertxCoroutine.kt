package io.vertx.ext.coroutine

import io.vertx.core.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.CoroutineContext

/**
 * Created by stream.
 */

/**
 * Receive a single event from a handler synchronously.
 * The coroutine will be blocked until the event occurs, this action do not block vertx's eventLoop.
 */
fun <T> asyncEvent(block: (h: Handler<T>) -> Unit) = async(vertxCoroutineContext()) {
  suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
    block(Handler { event -> cont.resume(event) })
  }
}

fun <T> asyncEvent(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS, block: (h: Handler<T>) -> Unit) = async(vertxCoroutineContext()) {
  withTimeout(timeout, unit) {
    suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
      block(Handler { event -> cont.resume(event) })
    }
  }
}

/**
 * Invoke an asynchronous operation and obtain the result synchronous.
 * The coroutine will be blocked until the event occurs, this action do not block vertx's eventLoop.
 */
fun <T> asyncResult(block: (h: Handler<AsyncResult<T>>) -> Unit) = async(vertxCoroutineContext()) {
  suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
    block(Handler { asyncResult ->
      if (asyncResult.succeeded()) cont.resume(asyncResult.result())
      else cont.resumeWithException(asyncResult.cause())
    })
  }
}

fun <T> asyncResult(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS, block: (h: Handler<AsyncResult<T>>) -> Unit) = async(vertxCoroutineContext()) {
  withTimeout(timeout, unit) {
    suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
      block(Handler { asyncResult ->
        if (asyncResult.succeeded()) cont.resume(asyncResult.result())
        else cont.resumeWithException(asyncResult.cause())
      })
    }
  }
}

/**
 * Awaits for completion of future without blocking eventLoop
 */
suspend fun <T> Future<T>.await(): T = when {
  succeeded() -> result()
  failed() -> throw cause()
  else -> suspendCancellableCoroutine { cont: CancellableContinuation<T> ->
    setHandler { asyncResult ->
      if (asyncResult.succeeded()) cont.resume(asyncResult.result() as T)
      else cont.resumeWithException(asyncResult.cause())
    }
  }
}

/**
 * Create an adaptor that converts a stream of events from a handler into a receiver which allows the events to be
 * received synchronously.
 */
fun <T> streamAdaptor() = HandlerReceiverAdaptorImpl<T>(vertxCoroutineContext())

/**
 * Like {@link #streamAdaptor()} but using the specified `Channel` instance. This is useful if you want to
 * fine-tune the behaviour of the adaptor.
 */
fun <T> streamAdaptor(channel: Channel<T>) = HandlerReceiverAdaptorImpl(vertxCoroutineContext(), channel)


/**
 * Converts this deferred value to the instance of Future.
 * The deferred value is cancelled when the resulting future is cancelled or otherwise completed.
 */
fun <T> Deferred<T>.asFuture(): Future<T> {
  val future = Future.future<T>()
  future.setHandler({ asyncResult ->
    //if fail, we cancel this job
    if (asyncResult.failed()) cancel(asyncResult.cause())
  })
  invokeOnCompletion {
    try {
      future.complete(getCompleted())
    } catch (exception: Exception) {
      future.fail(exception)
    }
  }
  return future
}

/**
 * Convert a standard handler to a handler which runs on a coroutine.
 * This is necessary if you want to do fiber blocking synchronous operation in your handler
 */
fun runVertxCoroutine(block: suspend CoroutineScope.() -> Unit) {
  launch(vertxCoroutineContext()) {
    try {
      block()
    } finally {
      //TODO try to cancel this coroutine for example vertx.cancel(timerID)
    }
  }
}

interface ReceiverAdaptor<out T> {
  suspend fun receive(): T

  suspend fun receive(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS): T
}

class HandlerReceiverAdaptorImpl<T>(val coroutineContext: CoroutineContext, val channel: Channel<T> = Channel()) : Handler<T>, ReceiverAdaptor<T> {

  override fun handle(event: T) {
    launch(coroutineContext) {
      channel.send(event)
    }
  }

  override suspend fun receive(): T = channel.receive()

  override suspend fun receive(timeout: Long, unit: TimeUnit): T = withTimeout(timeout, unit) {
    channel.receive()
  }
}

private const val VERTX_COROUTINE_DISPATCHER = "__vertx-kotlin-coroutine:dispatcher"
private var vertx: Vertx? = null

//you can init vertx instance if you running Vert.x by embed style.
fun initVertxToCoroutine(v: Vertx) {
  vertx = v
}

/**
 * Get Kotlin CoroutineContext, this coroutine should be one instance for per context.
 * @return CoroutineContext
 */
fun vertxCoroutineContext(): CoroutineContext {
  val vertxContext = vertx?.orCreateContext ?: Vertx.currentContext()
  requireNotNull(vertxContext, { "Do not in the vertx context" })
  require(vertxContext.isEventLoopContext, { "Not on the vertx eventLoop." })
  var vertxContextDispatcher = vertxContext.get<CoroutineContext>(VERTX_COROUTINE_DISPATCHER)
  if (vertxContextDispatcher == null) {
    vertxContextDispatcher = VertxContextDispatcher(vertxContext, Thread.currentThread())
    vertxContext.put(VERTX_COROUTINE_DISPATCHER, vertxContextDispatcher)
  }
  return vertxContextDispatcher
}

private class VertxContextDispatcher(val vertxContext: Context, val eventLoop: Thread) : CoroutineDispatcher() {
  override fun dispatch(context: CoroutineContext, block: Runnable) {
    if (Thread.currentThread() !== eventLoop) vertxContext.runOnContext { _ -> block.run() }
    else block.run()
  }
}

/**
 * Remove the scheduler for the current context
 */
fun removeVertxCoroutineContext() {
  val vertxContext = vertx?.orCreateContext ?: Vertx.currentContext()
  vertxContext?.remove(VERTX_COROUTINE_DISPATCHER)
}