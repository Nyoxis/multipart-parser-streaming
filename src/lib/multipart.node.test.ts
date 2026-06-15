import * as assert from '@remix-run/assert'
import { describe, it } from '@remix-run/test'

import { getRandomBytes } from '../../test/utils.ts'
import { createMultipartRequest } from '../../test/utils.node.ts'

import type { BufferedMultipartPart } from './multipart.ts'
import { MaxPartsExceededError, MaxTotalSizeExceededError } from './multipart.ts'
import { parseMultipartRequest, parseMultipartRequestAsStreams } from './multipart.node.ts'

const LARGE_FILE_SIZE = 128 * 1024

describe('parseMultipartRequest (node)', () => {
  let boundary = '----WebKitFormBoundaryzv5f5B2cY6tjQ0Rn'

  it('parses an empty multipart message', async () => {
    let request = createMultipartRequest(boundary)

    let parts = []
    for await (let part of parseMultipartRequestAsStreams(request)) {
      parts.push(part)
    }

    assert.equal(parts.length, 0)
  })

  it('parses a simple multipart form streaming', async () => {
    let request = createMultipartRequest(boundary, {
      field1: 'value1',
    })

    let buffering_parts: Promise<BufferedMultipartPart>[] = []
    for await (let part of parseMultipartRequestAsStreams(request)) {
      buffering_parts.push(part.toBuffered())
    }
    let parts = await Promise.all(buffering_parts)

    assert.equal(parts.length, 1)
    assert.equal(parts[0].name, 'field1')
    assert.equal(parts[0].text, 'value1')
  })

  it('parses a simple multipart form buffered', async () => {
    let request = createMultipartRequest(boundary, {
      field1: 'value1',
    })

    let parts = []
    for await (let part of parseMultipartRequest(request)) {
      parts.push(part)
    }

    assert.equal(parts.length, 1)
    assert.equal(parts[0].name, 'field1')
    assert.equal(parts[0].text, 'value1')
  })

  it('parses large file uploads correctly', async () => {
    let maxFileSize = LARGE_FILE_SIZE
    let content = getRandomBytes(maxFileSize)
    let request = createMultipartRequest(boundary, {
      file1: {
        filename: "tesla.jpg",
        mediaType: "image/jpeg",
        content,
      },
    });

    let parts: { name?: string, filename?: string, mediaType?: string, content: Promise<Uint8Array> }[] = []
    for await (let part of parseMultipartRequestAsStreams(request, { maxFileSize })) {
      parts.push({
        name: part.name,
        filename: part.filename,
        mediaType: part.mediaType,
        content: part.toBuffered().then((b) => b.bytes),
      })
    }

    assert.equal(parts.length, 1)
    assert.equal(parts[0].name, 'file1')
    assert.equal(parts[0].filename, 'tesla.jpg')
    assert.equal(parts[0].mediaType, 'image/jpeg')
    assert.deepEqual(await parts[0].content, content)
  })

  it('throws when the number of parts exceeds maxParts', async () => {
    let request = createMultipartRequest(boundary, {
      field1: 'value1',
      field2: 'value2',
      field3: 'value3',
    })

    await assert.rejects(async () => {
      for await (let part of parseMultipartRequestAsStreams(request, { maxParts: 2 })) {
        part.toBuffered()
      }
    }, MaxPartsExceededError)
  })

  it('throws when the aggregate content size exceeds maxTotalSize', async () => {
    let request = createMultipartRequest(boundary, {
      field1: 'hello',
      field2: 'world',
    })

    await assert.rejects(async () => {
      for await (let part of parseMultipartRequestAsStreams(request, { maxTotalSize: 9 })) {
        part.toBuffered().catch(() => {})
      }
    }, MaxTotalSizeExceededError)
  })
})
