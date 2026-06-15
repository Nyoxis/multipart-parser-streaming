import * as fs from 'node:fs'
import * as fsPromises from 'node:fs/promises'
import * as http from 'node:http'
import { pipeline } from 'node:stream/promises'
import tmp from 'tmp'

import { MultipartParseError, parseMultipartRequestAsStreams } from '@nyoxis/multipart-parser-streaming/node'

const PORT = 44100

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'text/html' })
    res.end(`
<!DOCTYPE html>
<html>
  <head>
    <title>multipart-parser-streaming Node Example</title>
  </head>
  <body>
    <h1>multipart-parser-streaming Node Example</h1>
    <form method="post" enctype="multipart/form-data">
      <p><input name="text1" type="text" /></p>
      <p><input name="file1" type="file" /></p>
      <p><button type="submit">Submit</button></p>
    </form>
  </body>
</html>
`)
    return
  }

  if (req.method === 'POST') {
    try {
      /** @type any[] */
      let parts = []
      let previousPartPromise = Promise.resolve(null)

      for await (let part of parseMultipartRequestAsStreams(req)) {
        await previousPartPromise

        if (part.isFile) {
          let tmpfile = tmp.fileSync()
          let writeStream = fs.createWriteStream(tmpfile.name)

          // Stream the readable part directly to the write stream without buffering in memory
          previousPartPromise = pipeline(part.contentReadable, writeStream)
            .then(() => fsPromises.stat(tmpfile.name))
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

      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ parts }, null, 2))
      return
    } catch (error) {
      if (error instanceof MultipartParseError) {
        res.writeHead(400, { 'Content-Type': 'text/plain', Connection: 'close' })
        res.end(`Error: ${error.message}`)
        return
      }

      console.error(error)

      res.writeHead(500, { 'Content-Type': 'text/plain' })
      res.end('Internal Server Error')
      return
    }
  }

  res.writeHead(405)
  res.end('Method Not Allowed')
})

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT} ...`)
})
