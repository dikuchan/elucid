import { describe, expect, test } from 'vitest';

import type { QueryDiagnostic } from '../api/contracts';
import { diagnosticsForEditor } from './diagnostics';

describe('query diagnostic placement', () => {
  test('converts backend UTF-8 byte spans to CodeMirror UTF-16 positions', () => {
    const query = 'source demo | project "сбой"';
    const fieldStart = query.indexOf('сбой');
    const encoder = new TextEncoder();
    const startByte = encoder.encode(query.slice(0, fieldStart)).byteLength;
    const endByte = encoder.encode(query.slice(0, fieldStart + 4)).byteLength;
    const diagnostic: QueryDiagnostic = {
      code: 'QUERY_FIELD_NOT_FOUND',
      severity: 'ERROR',
      message: 'Field was not found',
      span: { startByte, endByte },
      sourceRange: null,
    };

    expect(diagnosticsForEditor(query, [diagnostic])).toEqual([
      {
        from: fieldStart,
        to: fieldStart + 4,
        severity: 'error',
        message: 'Field was not found',
        source: 'QUERY_FIELD_NOT_FOUND',
      },
    ]);
  });
});
