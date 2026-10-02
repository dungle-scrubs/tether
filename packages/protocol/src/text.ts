/**
 * Returns the UTF-8 byte length of a string without allocating an encoded
 * buffer. Unpaired surrogates count as the three bytes `TextEncoder` emits for
 * the replacement character, so the result always matches
 * `new TextEncoder().encode(value).byteLength`.
 */
export function utf8ByteLength(value: string): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit <= 0x7f) {
      bytes += 1;
      continue;
    }
    if (unit <= 0x7ff) {
      bytes += 2;
      continue;
    }
    if (unit >= 0xd800 && unit <= 0xdbff && index + 1 < value.length) {
      const trail = value.charCodeAt(index + 1);
      if (trail >= 0xdc00 && trail <= 0xdfff) {
        bytes += 4;
        index += 1;
        continue;
      }
    }
    bytes += 3;
  }
  return bytes;
}
