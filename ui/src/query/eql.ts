import {
  HighlightStyle,
  LanguageSupport,
  LRLanguage,
  syntaxHighlighting,
} from '@codemirror/language';
import { styleTags, tags } from '@lezer/highlight';

import { parser } from './eql-parser';

const parserWithHighlighting = parser.configure({
  props: [
    styleTags({
      'SourceExpression/source FilterStage/filter ProjectStage/project SortStage/sort TakeStage/take SummarizeStage/summarize':
        tags.controlKeyword,
      'StartBound/start_inclusive EndBound/end_exclusive TimeExpression/now SortStage/by SummarizeStage/by CastExpression/as':
        tags.keyword,
      'LogicalOr/or LogicalAnd/and Unary/not': tags.logicOperator,
      'TimeUnit! TimeOperation/"@s" TimeOperation/"@m" TimeOperation/"@h" TimeOperation/"@d"':
        tags.keyword,
      'CastExpression/cast CastExpression/try_cast RemainderExpression/rest RemainderExpression/rest_exists DatetimeConstructor/datetime EidConstructor/eid':
        tags.function(tags.variableName),
      'AggregateName!': tags.function(tags.variableName),
      'IntegerType! FloatingType! LogicalType/bool LogicalType/utf8 LogicalType/datetime LogicalType/eid LogicalType/json':
        tags.typeName,
      'Literal/true Literal/false': tags.bool,
      'Literal/null': tags.null,
      'Projection/Identifier! Measure/Identifier!': tags.definition(
        tags.variableName,
      ),
      'Identifier!': tags.variableName,
      SystemIdentifier: tags.special(tags.variableName),
      'Integer Float': tags.number,
      String: tags.string,
      'Plus Minus Multiply Divide Equal NotEqual GreaterEqual Greater LessEqual Less At':
        tags.operator,
      Assign: tags.definitionOperator,
      'Pipe LeftParen RightParen Comma': tags.punctuation,
    }),
  ],
});

export const eqlLanguage = LRLanguage.define({
  name: 'eql',
  parser: parserWithHighlighting,
  languageData: {
    closeBrackets: { brackets: ['(', '"', '`'] },
  },
});

const eqlHighlightStyle = HighlightStyle.define(
  [
    {
      tag: [tags.keyword, tags.controlKeyword],
      color: 'var(--mantine-color-violet-8)',
      fontWeight: '600',
    },
    {
      tag: tags.function(tags.variableName),
      color: 'var(--mantine-color-blue-8)',
    },
    { tag: tags.typeName, color: 'var(--mantine-color-grape-8)' },
    {
      tag: tags.definition(tags.variableName),
      color: 'var(--mantine-color-indigo-8)',
    },
    {
      tag: tags.special(tags.variableName),
      color: 'var(--mantine-color-pink-8)',
    },
    {
      tag: [tags.number, tags.bool, tags.null],
      color: 'var(--mantine-color-orange-9)',
    },
    { tag: tags.string, color: 'var(--mantine-color-green-9)' },
    {
      tag: [tags.operator, tags.logicOperator, tags.definitionOperator],
      color: 'var(--mantine-color-cyan-9)',
    },
    { tag: tags.punctuation, color: 'var(--mantine-color-gray-7)' },
  ],
  { scope: eqlLanguage, themeType: 'light' },
);

export const eqlLanguageSupport = new LanguageSupport(
  eqlLanguage,
  syntaxHighlighting(eqlHighlightStyle),
);
