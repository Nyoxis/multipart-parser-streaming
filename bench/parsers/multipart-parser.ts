import { parseMultipart } from '@nyoxis/multipart-parser-streaming'

import { MultipartMessage } from '../messages.ts'

const BenchmarkMaxFileSize = 100 * 1024 * 1024 // 100 MiB

export async function parse(message: MultipartMessage): Promise<number> {
  let start = performance.now()

  for await (let _ of parseMultipart(message.generateChunks(), {
    boundary: message.boundary,
    maxFileSize: BenchmarkMaxFileSize,
  })) {
    // Do nothing with the part, just iterate through it to measure parsing time
  }

  return performance.now() - start
}
