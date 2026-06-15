import { ContentDisposition } from '@remix-run/headers/content-disposition'
import { ContentType } from '@remix-run/headers/content-type'

import {
  encodeAsciiPattern,
  createSearch,
  createPartialTailSearch,
  type SearchFunction,
  type PartialTailSearchFunction,
} from './buffer-search.ts'
import { readStream } from './read-stream.ts'

/**
 * The base class for errors thrown by the multipart parser.
 */
export class MultipartParseError extends Error {
  /**
   * @param message The error message
   */
  constructor(message: string) {
    super(message)
    this.name = 'MultipartParseError'
  }
}

/**
 * An error thrown when the maximum allowed size of a header is exceeded.
 */
export class MaxHeaderSizeExceededError extends MultipartParseError {
  /**
   * @param maxHeaderSize The maximum header size that was exceeded
   */
  constructor(maxHeaderSize: number) {
    super(`Multipart header size exceeds maximum allowed size of ${maxHeaderSize} bytes`)
    this.name = 'MaxHeaderSizeExceededError'
  }
}

/**
 * An error thrown when the maximum allowed size of a file is exceeded.
 */
export class MaxFileSizeExceededError extends MultipartParseError {
  /**
   * @param maxFileSize The maximum file size that was exceeded
   */
  constructor(maxFileSize: number) {
    super(`File size exceeds maximum allowed size of ${maxFileSize} bytes`)
    this.name = 'MaxFileSizeExceededError'
  }
}

/**
 * An error thrown when the maximum allowed number of multipart parts is exceeded.
 */
export class MaxPartsExceededError extends MultipartParseError {
  /**
   * @param maxParts The maximum number of parts that was exceeded
   */
  constructor(maxParts: number) {
    super(`Multipart part count exceeds maximum allowed count of ${maxParts}`)
    this.name = 'MaxPartsExceededError'
  }
}

/**
 * An error thrown when the maximum allowed aggregate multipart content size is exceeded.
 */
export class MaxTotalSizeExceededError extends MultipartParseError {
  /**
   * @param maxTotalSize The maximum total size that was exceeded
   */
  constructor(maxTotalSize: number) {
    super(`Multipart content size exceeds maximum allowed size of ${maxTotalSize} bytes`)
    this.name = 'MaxTotalSizeExceededError'
  }
}

/**
 * Options for parsing a multipart message.
 */
export interface ParseMultipartOptions {
  /**
   * The boundary string used to separate parts in the multipart message,
   * e.g. the `boundary` parameter in the `Content-Type` header.
   */
  boundary: string
  /**
   * The maximum allowed size of a header in bytes. If an individual part's header
   * exceeds this size, a `MaxHeaderSizeExceededError` will be thrown.
   *
   * @default 8192 (8 KiB)
   */
  maxHeaderSize?: number
  /**
   * The maximum allowed size of a file in bytes. If an individual part's content
   * exceeds this size, a `MaxFileSizeExceededError` will be thrown.
   *
   * @default 2097152 (2 MiB)
   */
  maxFileSize?: number
  /**
   * The maximum allowed number of parts in the multipart message. If this limit
   * is exceeded, a `MaxPartsExceededError` will be thrown.
   *
   * @default 1000
   */
  maxParts?: number
  /**
   * The maximum allowed aggregate size of all part content in bytes. If this
   * limit is exceeded, a `MaxTotalSizeExceededError` will be thrown.
   *
   * @default `maxFileSize * 20 + 1048576` (1 MiB)
   */
  maxTotalSize?: number
  /**
   * The queuing strategy used by the `ReadableStream` of each parsed part.
   * By customizing the high water mark and size function, you can allow
   * smaller parts to be fully buffered in memory, preventing backpressure deadlocks.
   *
   * @example
   * ```
   * // Count-based buffering (omitting `size` counts each chunk as 1):
   * queuingStrategy: {
   *   highWaterMark: 100
   * }
   *
   * // Byte-based buffering:
   * queuingStrategy: new ByteLengthQueuingStrategy({ highWaterMark: 65536 })
   * ```
   */
  queuingStrategy?: QueuingStrategy<Uint8Array>
}

/**
 * Parse a `multipart/*` message from a buffer/iterable and yield each part as a
 * {@link BufferedMultipartPart} object.
 *
 * Note: This is a low-level API that requires manual handling of the content and boundary.
 * If you're building a web server, consider using
 * {@link import('./multipart-request.ts').parseMultipartRequest} instead.
 *
 * @param message The multipart message as a `Uint8Array` or an iterable of `Uint8Array` chunks
 * @param options Options for the parser
 * @yields Parsed {@link BufferedMultipartPart} objects from the multipart message
 * @returns An async generator that yields {@link BufferedMultipartPart} objects
 */
export async function* parseMultipart(
  message: Uint8Array | Iterable<Uint8Array>,
  options: ParseMultipartOptions,
): AsyncGenerator<BufferedMultipartPart, void, unknown> {
  let parser = new MultipartParser(options.boundary, {
    maxHeaderSize: options.maxHeaderSize,
    maxFileSize: options.maxFileSize,
    maxParts: options.maxParts,
    maxTotalSize: options.maxTotalSize,
    queuingStrategy: options.queuingStrategy
  })

  async function* parseAll() {
    if (message instanceof Uint8Array) {
      if (message.length === 0) {
        return
      }
      yield* parser.write(message)
    } else {
      for (let chunk of message) {
        yield* parser.write(chunk)
      }
    }
    parser.finish()
  }

  yield* bufferMultipart(parseAll())
}

/**
 * Simple Transformer that collects streamed data into buffered one
 */
async function* bufferMultipart(
  asyncParser: AsyncGenerator<StreamedMultipartPart, void, unknown>,
): AsyncGenerator<BufferedMultipartPart, void, unknown> {
  let { value, done } = await asyncParser.next()
  while (!done) {
    let next = asyncParser.next()
    if (value) {
      let [iterator, buffered] = await Promise.all([next, value.toBuffered()])
      ;({ value, done } = iterator)
      yield buffered
    }
  }
}

/**
 * Parse a `multipart/*` message stream and yield each part as a {@link StreamedMultipartPart} object.
 *
 * Note: This is a low-level API that requires manual handling of the content and boundary.
 * If you're building a web server, consider using
 * {@link import('./multipart-request.ts').parseMultipartRequest} instead.
 *
 * @param stream A stream containing multipart data as a `ReadableStream<Uint8Array>`
 * @param options Options for the parser
 * @yields Parsed {@link StreamedMultipartPart} objects from the multipart stream
 * @returns An async generator that yields {@link StreamedMultipartPart} objects
 */
export async function* parseMultipartStream(
  stream: ReadableStream<Uint8Array>,
  options: ParseMultipartOptions,
): AsyncGenerator<StreamedMultipartPart, void, unknown> {
  let controller = new AbortController()
  let parser = new MultipartParser(options.boundary, {
    maxHeaderSize: options.maxHeaderSize,
    maxFileSize: options.maxFileSize,
    maxParts: options.maxParts,
    maxTotalSize: options.maxTotalSize,
    queuingStrategy: options.queuingStrategy,
  }, controller)

  for await (let chunk of readStream(stream, controller)) {
    if (chunk.length === 0) {
      continue // No data to parse
    }

    yield* parser.write(chunk)
  }

  parser.finish()
}

/**
 * Options for configuring a {@link MultipartParser}.
 */
export type MultipartParserOptions = Omit<ParseMultipartOptions, 'boundary'>

const MultipartParserStateStart = 0
const MultipartParserStateAfterBoundary = 1
const MultipartParserStateHeader = 2
const MultipartParserStateBody = 3
const MultipartParserStateDone = 4

const findDoubleNewline = createSearch('\r\n\r\n')

const oneKb = 1024
const oneMb = 1024 * oneKb
const defaultMaxParts = 1000
const defaultMaxTotalSizePartAllowance = 20

/**
 * A streaming parser for `multipart/*` HTTP messages.
 */
export class MultipartParser {
  /**
   * Boundary string used to detect part separators.
   */
  readonly boundary: string

  /**
   * Maximum header size allowed for each multipart part.
   */
  readonly maxHeaderSize: number

  /**
   * Maximum file size allowed for each multipart part.
   */
  readonly maxFileSize: number

  /**
   * Maximum number of parts allowed in a multipart message.
   */
  readonly maxParts: number

  /**
   * Maximum aggregate content size allowed across all parts.
   */
  readonly maxTotalSize: number
  /**
   * The queuing strategy used by the `ReadableStream` of each parsed part.
   */
  readonly queuingStrategy?: QueuingStrategy<Uint8Array>

  #findOpeningBoundary: SearchFunction
  #openingBoundaryLength: number
  #findBoundary: SearchFunction
  #findPartialTailBoundary: PartialTailSearchFunction
  #boundaryLength: number
  #boundaryBytes: Uint8Array

  #state = MultipartParserStateStart
  #buffer: Uint8Array | null = null
  #currentPart: StreamedMultipartPart | null = null
  #partCount = 0
  #totalContentLength = 0
  /**
   * AbortController to signal stream reading abortion and receive abortion signal
   */
  #controller?: AbortController

  /**
   * @param boundary The boundary string used to separate parts
   * @param options Options for the parser
   */
  constructor(boundary: string, options?: MultipartParserOptions, controller?: AbortController) {
    this.boundary = boundary
    this.maxHeaderSize = options?.maxHeaderSize ?? 8 * oneKb
    this.maxFileSize = options?.maxFileSize ?? 2 * oneMb
    this.maxParts = options?.maxParts ?? defaultMaxParts
    this.maxTotalSize =
      options?.maxTotalSize ?? this.maxFileSize * defaultMaxTotalSizePartAllowance + oneMb
    this.queuingStrategy = options?.queuingStrategy

    this.#findOpeningBoundary = createSearch(`--${boundary}`)
    this.#openingBoundaryLength = 2 + boundary.length // length of '--' + boundary
    let boundaryPattern = `\r\n--${boundary}`
    this.#findBoundary = createSearch(boundaryPattern)
    this.#findPartialTailBoundary = createPartialTailSearch(boundaryPattern)
    this.#boundaryLength = 4 + boundary.length // length of '\r\n--' + boundary
    this.#boundaryBytes = encodeAsciiPattern(boundaryPattern)

    this.#controller = controller
  }

  /**
   * Write a chunk of data to the parser.
   *
   * @param chunk A chunk of data to write to the parser
   * @yields Parsed {@link StreamedMultipartPart} objects that became available from this chunk
   * @returns An async generator yielding `StreamedMultipartPart` objects as they are parsed
   */
  async *write(chunk: Uint8Array): AsyncGenerator<StreamedMultipartPart, void, unknown> {
    if (this.#state === MultipartParserStateDone) {
      throw new MultipartParseError('Unexpected data after end of stream')
    }

    let index = 0
    let chunkLength = chunk.length

    if (this.#buffer !== null) {
      if (this.#state === MultipartParserStateBody) {
        let carry = this.#buffer
        let carryResult = this.#analyzeCarryBoundary(carry, chunk)

        if (carryResult.kind === 'none') {
          await this.#append(carry)
        } else if (carryResult.kind === 'partial') {
          if (carryResult.start > 0) {
            await this.#append(carry.subarray(0, carryResult.start))
          }

          let tailLength = carry.length + chunk.length - carryResult.start
          let tail = new Uint8Array(tailLength)
          let carryTail = carry.subarray(carryResult.start)
          tail.set(carryTail, 0)
          tail.set(chunk, carryTail.length)
          this.#buffer = tail
          return
        } else {
          if (carryResult.start > 0) {
            await this.#append(carry.subarray(0, carryResult.start))
          }

          this.#currentPart!.close()

          this.#state = MultipartParserStateAfterBoundary

          let carryAfterStart = carry.length - carryResult.start
          index = this.#boundaryLength - carryAfterStart
        }
      } else {
        let newChunk = new Uint8Array(this.#buffer.length + chunkLength)
        newChunk.set(this.#buffer, 0)
        newChunk.set(chunk, this.#buffer.length)
        chunk = newChunk
        chunkLength = chunk.length
      }

      this.#buffer = null
    }

    while (true) {
      if (this.#state === MultipartParserStateBody) {
        if (chunkLength - index < this.#boundaryLength) {
          this.#buffer = chunk.subarray(index)
          break
        }

        let boundaryIndex = this.#findBoundary(chunk, index)
        if (boundaryIndex === -1) {
          // No boundary found, but there may be a partial match at the end of the chunk.
          let partialTailIndex = this.#findPartialTailBoundary(chunk)

          if (partialTailIndex === -1) {
            await this.#append(index === 0 ? chunk : chunk.subarray(index))
          } else {
            if (partialTailIndex > index) {
              await this.#append(chunk.subarray(index, partialTailIndex))
            }
            this.#buffer = chunk.subarray(partialTailIndex)
          }

          break
        }

        if (boundaryIndex > index) {
          await this.#append(chunk.subarray(index, boundaryIndex))
        }

        this.#currentPart!.close()

        index = boundaryIndex + this.#boundaryLength

        this.#state = MultipartParserStateAfterBoundary
      }

      if (this.#state === MultipartParserStateAfterBoundary) {
        if (chunkLength - index < 2) {
          this.#buffer = chunk.subarray(index)
          break
        }

        if (chunk[index] === 45 && chunk[index + 1] === 45) {
          this.#state = MultipartParserStateDone
          break
        }

        index += 2 // Skip \r\n after boundary

        this.#state = MultipartParserStateHeader
      }

      if (this.#state === MultipartParserStateHeader) {
        if (chunkLength - index < 4) {
          this.#buffer = chunk.subarray(index)
          break
        }

        let headerEndIndex = findDoubleNewline(chunk, index)

        if (headerEndIndex === -1) {
          if (chunkLength - index > this.maxHeaderSize) {
            this.error(new MaxHeaderSizeExceededError(this.maxHeaderSize))
          }

          this.#buffer = chunk.subarray(index)
          break
        }

        if (headerEndIndex - index > this.maxHeaderSize) {
          this.error(new MaxHeaderSizeExceededError(this.maxHeaderSize))
        }

        if (++this.#partCount > this.maxParts) {
          this.error(new MaxPartsExceededError(this.maxParts))
        }

        this.#currentPart = new StreamedMultipartPart(
          chunk.subarray(index, headerEndIndex),
          this.queuingStrategy,
          this.#controller
        )
        yield this.#currentPart

        index = headerEndIndex + 4 // Skip header + \r\n\r\n

        this.#state = MultipartParserStateBody

        continue
      }

      if (this.#state === MultipartParserStateStart) {
        if (chunkLength < this.#openingBoundaryLength) {
          this.#buffer = chunk
          break
        }

        if (this.#findOpeningBoundary(chunk) !== 0) {
          this.error(new MultipartParseError('Invalid multipart stream: missing initial boundary'))
        }

        index = this.#openingBoundaryLength

        this.#state = MultipartParserStateAfterBoundary
      }
    }
  }

  async #append(chunk: Uint8Array): Promise<void> {
    if (chunk.length === 0) {
      return
    }

    if (this.#currentPart!.contentLength + chunk.length > this.maxFileSize) {
      this.error(new MaxFileSizeExceededError(this.maxFileSize))
    }

    if (this.#totalContentLength + chunk.length > this.maxTotalSize) {
      this.error(new MaxTotalSizeExceededError(this.maxTotalSize))
    }

    await this.#currentPart!.appendChunk(chunk)
    this.#totalContentLength += chunk.length
  }

  #analyzeCarryBoundary(
    carry: Uint8Array,
    chunk: Uint8Array,
  ): { kind: 'none' } | { kind: 'partial'; start: number } | { kind: 'full'; start: number } {
    let totalLength = carry.length + chunk.length

    for (let start = 0; start < carry.length; ++start) {
      let availableLength = totalLength - start
      let compareLength = Math.min(this.#boundaryLength, availableLength)

      let matched = true
      for (let i = 0; i < compareLength; ++i) {
        let sourceIndex = start + i
        let sourceByte = sourceIndex < carry.length ? carry[sourceIndex] : chunk[sourceIndex - carry.length]
        if (sourceByte !== this.#boundaryBytes[i]) {
          matched = false
          break
        }
      }

      if (!matched) {
        continue
      }

      if (availableLength >= this.#boundaryLength) {
        return { kind: 'full', start }
      }

      return { kind: 'partial', start }
    }

    return { kind: 'none' }
  }

  /**
   * Should be called after all data has been written to the parser.
   *
   * Note: This will throw if the multipart message is incomplete or
   * wasn't properly terminated.
   */
  finish(): void {
    if (this.#state !== MultipartParserStateDone) {
      this.error(new MultipartParseError('Multipart stream not finished'))
    }
  }

  /**
   * Propagate an error to the current active part.
   */
  error(e: any): void {
    if (this.#currentPart) {
      this.#currentPart.error(e)
    }
    throw e
  }
}

let decoder: TextDecoder | undefined

function decodeUtf8(input: Uint8Array): string {
  decoder ??= new TextDecoder('utf-8', { fatal: true })
  return decoder.decode(input as BufferSource)
}

/**
 * The decoded headers for a multipart part, keyed by lower-case header name.
 */
export interface MultipartHeaders {
  readonly [name: string]: string | undefined
}

function parseMultipartHeaders(raw: string): MultipartHeaders {
  let headers: Record<string, string> = Object.create(null)

  for (let line of raw.split('\r\n')) {
    let match = line.match(/^([^:]+):(.*)/)
    if (match) {
      let name = match[1].trim().toLowerCase()
      let value = match[2].trim()
      let existingValue = headers[name]
      headers[name] = existingValue === undefined ? value : `${existingValue}, ${value}`
    }
  }

  return Object.freeze(headers)
}

/**
 * A part of a `multipart/*` HTTP message without content.
 */
export class MultipartPart {
  readonly rawHeader: Uint8Array
  #headers?: MultipartHeaders

  /**
   * @param header The raw header bytes
   */
  constructor(header: Uint8Array) {
    this.rawHeader = header
  }

  /**
   * The decoded headers associated with this part, keyed by lower-case header name.
   */
  get headers(): MultipartHeaders {
    if (!this.#headers) {
      this.#headers = parseMultipartHeaders(decodeUtf8(this.rawHeader))
    }

    return this.#headers
  }

  /**
   * True if this part originated from a file upload.
   */
  get isFile(): boolean {
    return this.filename !== undefined ||
      this.mediaType === 'application/octet-stream'
  }

  /**
   * True if this part originated from a text input field in a form submission.
   */
  get isText(): boolean {
    return !this.isFile
  }

  /**
   * The filename of the part, if it is a file upload.
   */
  get filename(): string | undefined {
    return ContentDisposition.from(this.headers['content-disposition'] ?? null)
      .preferredFilename
  }

  /**
   * The media type of the part.
   */
  get mediaType(): string | undefined {
    return ContentType.from(this.headers['content-type'] ?? null).mediaType
  }

  /**
   * The name of the part, usually the `name` of the field in the `<form>` that submitted the request.
   */
  get name(): string | undefined {
    return ContentDisposition.from(this.headers['content-disposition'] ?? null)
      .name
  }
}

/**
 * A part of a `multipart/*` HTTP message with content as ReadableStream.
 */
export class StreamedMultipartPart extends MultipartPart {
  #controller: ReadableStreamDefaultController<Uint8Array> | null = null
  #continue: (() => void) | null = null
  #contentLength = 0
  #signal?: AbortSignal
  /**
   * ReadableStream of raw content of this part.
   */
  readonly content: ReadableStream<Uint8Array>
  /**
   * Propagates AbortSignal reason to the current's part stream
   */
  readonly errorContent: () => void = () => {
    this.error(this.#signal?.reason)
  }
  constructor(
    rawHeader: Uint8Array,
    queuingStrategy?: QueuingStrategy<Uint8Array>,
    controller?: AbortController,
  ) {
    super(rawHeader)
    this.#signal = controller?.signal
    this.#signal?.addEventListener('abort', this.errorContent, { once: true })
    this.content = new ReadableStream<Uint8Array>({
      start: (controller) => {
        // Save controller so we can enqueue chunks later
        this.#controller = controller
      },
      pull: () => {
        if (this.#continue) {
          this.#continue()
          this.#continue = null
        }
      },
      cancel: (reason) => {
        this.#signal?.removeEventListener('abort', this.errorContent)
        this.#controller = null
        if (this.#continue) {
          this.#continue()
          this.#continue = null
        }
        if(!this.#signal?.aborted) {
          controller?.abort(reason)
        }
      },
    }, queuingStrategy)
  }

  /**
   * Expected length of full-length streamed content
   */
  get contentLength(): number {
    return this.#contentLength
  }

  /**
   * Appends chunk to the stream
   */
  async appendChunk(chunk: Uint8Array) {
    if (this.#controller == null || this.#controller.desiredSize == null) {
      return // skip appending chunks if stream is closed or dropped
    }
    while (this.#controller.desiredSize <= 0) {
      await new Promise((resolve) => {
        this.#continue = () => resolve(true)
      })
    }
    this.#controller.enqueue(chunk)
    this.#contentLength += chunk.length
  }

  /**
   * Signal end-of-stream
   */
  close() {
    this.#signal?.removeEventListener('abort', this.errorContent)
    if (this.#controller) {
      this.#controller.close()
      // prevent from attempting to enqueue or close again
      this.#controller = null
    }
    if (this.#continue) {
      this.#continue()
      this.#continue = null
    }
  }

  /**
   * Signal error in stream
   */
  error(e: any) {
    if (this.#controller) {
      this.#controller.error(e)
    }
  }

  /**
   * Consumes stream of content into buffered content,
   * that could be used to create Blob
   *
   * Note: Only call toBuffered() if you haven't read from the part's stream yet.
   */
  async toBuffered(): Promise<BufferedMultipartPart> {
    return this.toBufferedFromIterator(readStream(this.content))
  }

  /**
   * Bufferization abstraction for Node compatibility
   */
  async toBufferedFromIterator(
    iterator: AsyncIterable<Uint8Array>,
  ): Promise<BufferedMultipartPart> {
    return new Promise<BufferedMultipartPart>(async (resolve, reject) => {
      let chunks: Uint8Array[] = []
      try {
        for await (let value of iterator) {
          this.#contentLength -= value.length
          if (value) chunks.push(value)
        }
      } catch (e) {
        reject(e)
      }
      if (this.#contentLength !== 0) {
        reject(new MultipartParseError('Streaming part content is disturbed and buffer cannot be complete'))
      }
      return resolve(new BufferedMultipartPart(this.rawHeader, chunks))
    })
  }
}

/**
 * A part of a `multipart/*` HTTP message with buffered content.
 */
export class BufferedMultipartPart extends MultipartPart {
  /**
   * The raw content of this part as an array of `Uint8Array` chunks.
   */
  readonly content: Uint8Array[]

  constructor(rawHeader: Uint8Array, content: Uint8Array[]) {
    super(rawHeader)
    this.content = content
  }

  /**
   * The content of this part as an `ArrayBuffer`.
   */
  get arrayBuffer(): ArrayBuffer {
    return this.bytes.buffer as ArrayBuffer
  }

  /**
   * The content of this part as a single `Uint8Array`. In `multipart/form-data` messages, this is useful
   * for reading the value of files that were uploaded using `<input type="file">` fields.
   */
  get bytes(): Uint8Array {
    let buffer = new Uint8Array(this.size)

    let offset = 0
    for (let chunk of this.content) {
      buffer.set(chunk, offset)
      offset += chunk.length
    }

    return buffer
  }

  /**
   * The size of the content in bytes.
   */
  get size(): number {
    let size = 0

    for (let chunk of this.content) {
      size += chunk.length
    }

    return size
  }

  /**
   * The content of this part as a string. In `multipart/form-data` messages, this is useful for
   * reading the value of parts that originated from `<input type="text">` fields.
   *
   * Note: Do not use this for binary data, use `part.bytes` or `part.arrayBuffer` instead.
   */
  get text(): string {
    return decodeUtf8(this.bytes)
  }
}
