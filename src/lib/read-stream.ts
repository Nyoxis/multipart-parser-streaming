// We need this little helper for environments that do not support
// ReadableStream.prototype[Symbol.asyncIterator] yet. See #46
export async function* readStream(stream: ReadableStream<Uint8Array>, controller?: AbortController): AsyncIterable<Uint8Array> {
  let reader = stream.getReader()
  let signal = controller?.signal
  reader.closed
    .catch((e) => {
      if (!signal?.aborted) {
        controller?.abort(e)
      }
    })

  try {
    while (true) {
      if (signal?.aborted) {
        if (!(signal.reason instanceof Error)) {
          await reader.cancel(signal.reason)
        }
        break
      }

      let result = await reader.read()
      if (result.done) break
      yield result.value
    }
  } catch(e) {
    if(!signal?.aborted) {
      controller?.abort(e)
    }
    throw(e)
  } finally {
    reader.releaseLock()
  }
}
