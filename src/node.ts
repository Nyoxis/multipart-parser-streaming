// Re-export all core functionality
export type {
  ParseMultipartOptions,
  MultipartParserOptions,
  MultipartHeaders,
} from './lib/multipart.ts'
export {
  MultipartParseError,
  MaxHeaderSizeExceededError,
  MaxFileSizeExceededError,
  MaxPartsExceededError,
  MaxTotalSizeExceededError,
  MultipartParser,
  MultipartPart,
  StreamedMultipartPart,
  BufferedMultipartPart
} from './lib/multipart.ts'

export { getMultipartBoundary } from './lib/multipart-request.ts'

// Export Node.js-specific functionality
export {
  isMultipartRequest,
  parseMultipartRequest,
  parseMultipartRequestAsStreams,
  parseMultipart,
  parseMultipartStream,
} from './lib/multipart.node.ts'
