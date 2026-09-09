declare module "parallel-transform" {
  import type { Transform } from "node:stream";
  function parallelTransform(
    concurrency: number,
    transform: (chunk: any, enc: BufferEncoding, cb: (err: Error | null, data?: any) => void) => void,
  ): Transform;
  function parallelTransform(
    concurrency: number,
    opts: object,
    transform?: (chunk: any, enc: BufferEncoding, cb: (err: Error | null, data?: any) => void) => void,
  ): Transform;
  export default parallelTransform;
}
