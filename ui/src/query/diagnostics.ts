import type { Diagnostic as EditorDiagnostic } from '@codemirror/lint';

import type { QueryDiagnostic } from '../api/contracts';

const textEncoder = new TextEncoder();

export function diagnosticsForEditor(
  query: string,
  diagnostics: readonly QueryDiagnostic[],
): readonly EditorDiagnostic[] {
  return diagnostics.flatMap((diagnostic) => {
    if (diagnostic.span === null) {
      return [];
    }
    const from = utf8ByteOffsetToUtf16Index(query, diagnostic.span.startByte);
    const to = utf8ByteOffsetToUtf16Index(query, diagnostic.span.endByte);
    if (from === null || to === null || from > to) {
      return [];
    }
    return [
      {
        from,
        to,
        severity: diagnostic.severity === 'ERROR' ? 'error' : 'warning',
        message: diagnostic.message,
        source: diagnostic.code,
      },
    ];
  });
}

function utf8ByteOffsetToUtf16Index(
  text: string,
  byteOffset: number,
): number | null {
  if (!Number.isSafeInteger(byteOffset) || byteOffset < 0) {
    return null;
  }
  let bytes = 0;
  let utf16Index = 0;
  for (const character of text) {
    if (bytes === byteOffset) {
      return utf16Index;
    }
    bytes += textEncoder.encode(character).byteLength;
    utf16Index += character.length;
    if (bytes > byteOffset) {
      return null;
    }
  }
  return bytes === byteOffset ? utf16Index : null;
}
