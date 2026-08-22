import { classHighlighter, highlightTree } from '@lezer/highlight';
import { describe, expect, test } from 'vitest';

import { eqlLanguage } from './eql';

interface HighlightedToken {
  readonly text: string;
  readonly classes: string;
}

function highlightedTokens(query: string): readonly HighlightedToken[] {
  const tokens: HighlightedToken[] = [];
  highlightTree(
    eqlLanguage.parser.parse(query),
    classHighlighter,
    (from, to, classes) => {
      tokens.push({ text: query.slice(from, to), classes });
    },
  );
  return tokens;
}

describe('EQL syntax highlighting', () => {
  test('distinguishes contextual command words from field names', () => {
    const query =
      'source source | filter source == 1 | project filter, `sort`, @event_time';

    expect(highlightedTokens(query)).toEqual([
      { text: 'source', classes: 'tok-keyword' },
      { text: 'source', classes: 'tok-variableName' },
      { text: '|', classes: 'tok-punctuation' },
      { text: 'filter', classes: 'tok-keyword' },
      { text: 'source', classes: 'tok-variableName' },
      { text: '==', classes: 'tok-operator' },
      { text: '1', classes: 'tok-number' },
      { text: '|', classes: 'tok-punctuation' },
      { text: 'project', classes: 'tok-keyword' },
      { text: 'filter', classes: 'tok-variableName' },
      { text: ',', classes: 'tok-punctuation' },
      { text: '`sort`', classes: 'tok-variableName' },
      { text: ',', classes: 'tok-punctuation' },
      { text: '@event_time', classes: 'tok-variableName2' },
    ]);
  });
});
