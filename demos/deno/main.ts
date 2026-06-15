// deno-lint-ignore-file prefer-const
import { BufferedMultipartPart, MultipartParseError, parseMultipartRequestAsStreams, } from '@nyoxis/multipart-parser-streaming'
// @deno-types="npm:@types/tmp"
import tmp from 'npm:tmp'

const PORT = 44100

async function requestHandler(request: Request): Promise<Response> {
  if (request.method === 'GET') {
    return new Response(
      `
<!DOCTYPE html>
<html>
  <head>
    <title>multipart-parser-streaming Deno Example</title>
  </head>
  <body>
    <h1>multipart-parser-streaming Deno Example</h1>
    <form method="post" enctype="multipart/form-data">
      <p><input name="text1" type="text" /></p>
      <p><input name="file1" type="file" /></p>
      <p><button type="submit">Submit</button></p>
    </form>
  </body>
</html>
`,
      {
        headers: { 'Content-Type': 'text/html' },
      },
    )
  }

  if (request.method === 'POST') {
    try {
      // deno-lint-ignore no-explicit-any
      let parts: any[] = []
      let previousPartPromise = Promise.resolve<BufferedMultipartPart | null | void>(null)

      for await (let part of parseMultipartRequestAsStreams(request)) {
        await previousPartPromise

        if (part.isFile) {
          let tmpfile = tmp.fileSync()

          // Stream directly to file without buffering in memory
          previousPartPromise = Deno.open(tmpfile.name, {
            write: true,
            create: true,
            truncate: true,
          })
            .then((file) => part.content.pipeTo(file.writable))
            .then(() => Deno.stat(tmpfile.name))
            .then((stat) => {
              parts.push({
                name: part.name,
                filename: part.filename,
                mediaType: part.mediaType,
                size: stat.size,
                file: tmpfile.name,
              })
            })
        } else {
          previousPartPromise = part.toBuffered().then((buffered) => {
            parts.push({ name: buffered.name, value: buffered.text })
          })
        }
      }
      await previousPartPromise

      return new Response(JSON.stringify({ parts }, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      if (error instanceof MultipartParseError) {
        return new Response(`Error: ${error.message}`, { status: 400 })
      }

      console.error(error)

      return new Response('Internal Server Error', { status: 500 })
    }
  }

  return new Response('Method Not Allowed', { status: 405 })
}

Deno.serve(
  {
    port: PORT,
    onListen() {
      console.log(`Server listening on http://localhost:${PORT} ...`)
    },
  },
  requestHandler,
)
