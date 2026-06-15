# @nyoxis/multipart-parser-streaming

A fork of `[@remix-run/multipart-parser](https://github.com/remix-run/remix/tree/main/packages/multipart-parser)` that enables true end-to-end streaming of parsed multipart body parts as Web `ReadableStream` and Node.js `Readable` streams.

`@nyoxis/multipart-parser-streaming` processes and yields multipart parts incrementally so large uploads can be piped and consumed on-the-fly without buffering them in memory.

## Features

- **File Upload Parsing** - Parse file uploads (`multipart/form-data`) with automatic field and file detection
- **True Streaming Content** - Access part content as standard `ReadableStream` (web) or `Readable` (Node.js) streams, enabling direct piping and processing without memory buffering
- **Full Multipart Support** - Support for all `multipart/*` content types (mixed, alternative, related, etc.)
- **Convenient API** - Access metadata directly, stream content via `.content`, or buffer into a `BufferedMultipartPart` with `arrayBuffer`, `bytes`, `text`, and `size` properties
- **Built-in Limits** - Header, per-part, part-count, and aggregate-size limits to prevent abuse
- **Node.js Support** - First-class Node.js support with native `http.IncomingMessage` and `stream.Readable` compatibility
- **Runtime Demos** - [Demos for every major runtime](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos)

## Why this fork?

This fork was created to enable true, end-to-end streaming of multipart body parts (yielding standard Web `ReadableStream` and Node.js `Readable` streams). 

The upstream `[@remix-run/multipart-parser](https://github.com/remix-run/remix/tree/main/packages/multipart-parser)` package buffers part contents in memory (`Uint8Array[]`) to keep the API simpler and avoid stream deadlock issues. If you need to handle large uploads by piping or processing them on-the-fly, this fork provides the streaming-first architecture required for those use cases.

Trying to support both fully-buffered and streaming-first APIs in a single package creates mismatched paradigms—the buffered version processes files sequentially after accumulation, while the streaming version yields control back-and-forth mid-stream. Keeping them separate avoids code duplication, package bloat, and API complexity.

You can read the original design discussion and trade-offs in [remix-run/remix#10781](https://github.com/remix-run/remix/discussions/10781).

## Installation

```sh
# Deno
deno add jsr:@nyoxis/multipart-parser-streaming

# Node.js (npm)
npx jsr add @nyoxis/multipart-parser-streaming

# Node.js (pnpm)
pnpm dlx jsr add @nyoxis/multipart-parser-streaming

# Node.js (yarn)
yarn dlx jsr add @nyoxis/multipart-parser-streaming

# Bun
bunx jsr add @nyoxis/multipart-parser-streaming
```

## Usage

### 1. Buffered Parser (Default)
The most common use case is when you want to parse form submissions where parts are relatively small and can be safely held in memory. For this, the default `parseMultipartRequest` function is recommended. It yields `BufferedMultipartPart` objects sequentially. Because parts are buffered internally, you can access properties like `part.bytes`, `part.text`, or `part.size` directly without deadlocks or having to manage promise pipelines:

```ts
import { MultipartParseError, parseMultipartRequest } from '@nyoxis/multipart-parser-streaming'

async function handleRequest(request: Request): Promise<void> {
  try {
    for await (let part of parseMultipartRequest(request)) {
      if (part.isFile) {
        console.log(`File received: ${part.filename}`)
        console.log(`Content type: ${part.mediaType}`)
        console.log(`Field name: ${part.name}`)
        console.log(`Content-Type header: ${part.headers['content-type']}`)

        // Save buffered bytes directly
        await saveFile(part.filename, part.bytes)
      } else {
        // Access buffered text directly
        console.log(`Field received: ${part.name} = ${JSON.stringify(part.text)}`)
      }
    }
  } catch (error) {
    if (error instanceof MultipartParseError) {
      console.error('Failed to parse multipart request:', error.message)
    } else {
      console.error('An unexpected error occurred:', error)
    }
  }
}
```

### 2. Streaming Parser
If you are uploading large files and want to stream their content directly to cloud storage or disk without holding the entire file in memory, use `parseMultipartRequestAsStreams`.

Because it returns parts as streams, you must process them without blocking the generator loop, using the pipelined `.then()` callback pattern below to prevent deadlocks:

```ts
import { MultipartParseError, parseMultipartRequestAsStreams } from '@nyoxis/multipart-parser-streaming'

async function handleRequest(request: Request): Promise<void> {
  try {
    let previousPartPromise = Promise.resolve<BufferedMultipartPart | null | void>(null)

    for await (let part of parseMultipartRequestAsStreams(request)) {
      await previousPartPromise

      if (part.isFile) {
        // Stream the file content directly to cloud storage, disk, or another API
        previousPartPromise = saveFileAsync(part.filename, part.content)
      } else {
        // Buffer simple text fields and process their results in a .then callback
        // !IMPORTANT: Don't await the promise for the current part, otherwise a deadlock will occur.
        previousPartPromise = part.toBuffered().then((buffered) => {
          console.log(`Field received: ${buffered.name} = ${JSON.stringify(buffered.text)}`)
        })
      }
    }
    await previousPartPromise
  } catch (error) {
    if (error instanceof MultipartParseError) {
      console.error('Failed to parse multipart request:', error.message)
    } else {
      console.error('An unexpected error occurred:', error)
    }
  }
}
```

## Part Headers

Each `MultipartPart` exposes decoded part headers as a plain object keyed by lower-case header name. Values are strings, and repeated headers are joined with `, `. Multipart part headers are parsed metadata from the request body, not native `Headers` objects, so access them with bracket notation:

```ts
for await (let part of parseMultipartRequest(request)) {
  let contentDisposition = part.headers['content-disposition']
  let contentType = part.headers['content-type']

  console.log(contentDisposition, contentType)
}
```

## Size Limits

A common use case when handling file uploads is limiting the overall shape of incoming multipart bodies so malicious clients cannot force unbounded growth in memory. Use `maxFileSize` to limit each part, `maxParts` to limit how many parts are accepted, and `maxTotalSize` to limit aggregate part content across the entire request. `@nyoxis/multipart-parser-streaming` applies finite defaults for each of these limits.

```ts
import {
  MultipartParseError,
  MaxFileSizeExceededError,
  MaxPartsExceededError,
  MaxTotalSizeExceededError,
  parseMultipartRequest,
} from '@nyoxis/multipart-parser-streaming/node'

const oneMb = Math.pow(2, 20)
const limits = {
  maxFileSize: 10 * oneMb,
  maxParts: 100,
  maxTotalSize: 25 * oneMb,
}

async function handleRequest(request: Request): Promise<Response> {
  try {
    for await (let part of parseMultipartRequest(request, limits)) {
      // ...
    }
  } catch (error) {
    if (error instanceof MaxFileSizeExceededError) {
      return new Response('File size limit exceeded', { status: 413 })
    } else if (error instanceof MaxPartsExceededError) {
      return new Response('Too many multipart parts', { status: 413 })
    } else if (error instanceof MaxTotalSizeExceededError) {
      return new Response('Multipart request is too large', { status: 413 })
    } else if (error instanceof MultipartParseError) {
      return new Response('Failed to parse multipart request', { status: 400 })
    } else {
      console.error(error)
      return new Response('Internal Server Error', { status: 500 })
    }
  }
}
```

## Node.js Bindings

The main module (`import {} from '@nyoxis/multipart-parser-streaming'`) assumes you're working with [the fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API) (`Request`, `ReadableStream`, etc). Support for these interfaces was added to Node.js by the [undici](https://github.com/nodejs/undici) project in [version 16.5.0](https://nodejs.org/en/blog/release/v16.5.0).

If however you're building a server for Node.js that relies on node-specific APIs like `http.IncomingMessage`, `stream.Readable`, and `buffer.Buffer` (ala Express or `http.createServer`), `@nyoxis/multipart-parser-streaming` ships with an additional module that works directly with these APIs.

```ts
import * as http from 'node:http'
import { MultipartParseError, parseMultipartRequest } from '@nyoxis/multipart-parser-streaming/node'

let server = http.createServer(async (req, res) => {
  try {
    for await (let part of parseMultipartRequest(req)) {
      // ...
    }
  } catch (error) {
    if (error instanceof MultipartParseError) {
      console.error('Failed to parse multipart request:', error.message)
    } else {
      console.error('An unexpected error occurred:', error)
    }
  }
})

server.listen(8080)
```

## Low-level API

If you're working directly with multipart boundaries and buffers/streams of multipart data that are not necessarily part of a request, `@nyoxis/multipart-parser-streaming` provides a low-level `parseMultipart()` API that you can use directly:

```ts
import { parseMultipart } from '@nyoxis/multipart-parser-streaming'

let message = new Uint8Array(/* ... */)
let boundary = '----WebKitFormBoundary56eac3x'

for (let part of parseMultipart(message, { boundary })) {
  // ...
}
```

In addition, the `parseMultipartStream` function provides an `async` generator interface for multipart data in a `ReadableStream`:

```ts
import { parseMultipartStream } from '@nyoxis/multipart-parser-streaming'

let message = new ReadableStream(/* ... */)
let boundary = '----WebKitFormBoundary56eac3x'

for await (let part of parseMultipartStream(message, { boundary })) {
  // ...
}
```

## Demos

The [`demos` directory](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos) contains a few working demos of how you can use this library:

- [`demos/bun`](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos/bun) - using multipart-parser in Bun
- [`demos/cf-workers`](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos/cf-workers) - using multipart-parser in a Cloudflare Worker and storing file uploads in R2
- [`demos/deno`](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos/deno) - using multipart-parser in Deno
- [`demos/node`](https://github.com/Nyoxis/multipart-parser-streaming/tree/main/demos/node) - using multipart-parser in Node.js

## Related Packages

- [`@remix-run/multipart-parser`](https://github.com/remix-run/remix/tree/main/packages/multipart-parser) - The original upstream package from which this fork was created
- [`form-data-parser`](https://github.com/remix-run/remix/tree/main/packages/form-data-parser) - Uses `multipart-parser` internally to parse multipart requests and generate `FileUpload`s for storage
- [`headers`](https://github.com/remix-run/remix/tree/main/packages/headers) - Used internally to parse `Content-Disposition` and `Content-Type` metadata for each `MultipartPart`

## License

See [LICENSE](./LICENSE)
