# Elucid v0 Query Language Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md)

## 1. Model

A query is a source expression followed by an ordered pipeline. Each stage consumes one typed relation and produces one typed relation. Evaluation order is left to right.

A parsed field occurrence is a `FieldReference` containing an identifier and source span. Semantic analysis MUST replace every `FieldReference` with a `Field` containing resolved identity, output name, logical type, nullability, and origin. Typed IR MUST contain no unresolved field reference.

Field origin MUST be a closed variant: schema field, system field, remainder field, or derived field. It MUST NOT be represented by independent boolean flags.

## 2. Lexical grammar

Source and user-field names MUST match `[A-Za-z_][A-Za-z0-9_]*`. An unquoted identifier has the same form. A quoted identifier MUST contain one valid source or user-field name between backticks, MUST support no escape syntax, and MUST be accepted wherever the grammar expects `identifier`. Backticks are not part of the resolved name. System-field identifiers MUST match `@[A-Za-z_][A-Za-z0-9_]*` and MUST NOT be quoted. Keywords are lowercase and case-sensitive.

Quoted identifiers are never keywords. Unquoted keywords have these classifications:

| Classification | Words |
|---|---|
| Contextual commands | `source`, `filter`, `project`, `sort`, `take`, `summarize` |
| Contextual time units | `s`, `m`, `h`, `d` |
| Reserved operators and literals | `and`, `or`, `not`, `true`, `false`, `null`, `now` |
| Reserved clauses | `start_inclusive`, `end_exclusive`, `by`, `as` |
| Reserved functions | `cast`, `try_cast`, `rest`, `rest_exists`, `count`, `sum`, `min`, `max`, `avg` |
| Reserved types and constructors | `bool`, `int32`, `int64`, `uint32`, `uint64`, `float32`, `float64`, `utf8`, `datetime`, `eid`, `json` |

A contextual keyword MUST be interpreted as an identifier where the grammar expects `identifier`. A reserved word MUST be quoted to identify a source, field, or alias with that name.

String literals MUST use double quotes and JSON string escapes. A leading minus MUST always be a separate unary-operator token and MUST NOT belong to an integer or floating-point token; a sign following an exponent marker remains part of that floating-point token.

An integer literal token MUST contain only ASCII decimal digits and have a mathematical value in `[0, 18446744073709551615]`. It remains an exact untyped integer during analysis. When one operand or enclosing construct supplies a unique numeric type, the literal MUST acquire that type if its value is exactly representable and MUST otherwise produce `QUERY_LITERAL_INVALID`. Without a unique numeric context, it MUST acquire `int64` when representable and MUST otherwise produce `QUERY_LITERAL_INVALID`. Unary minus applied to an otherwise uncontextualized integer literal MUST produce the corresponding `int64` value when representable, including `-9223372036854775808`; a negative expression in an unsigned context MUST produce `QUERY_TYPE_MISMATCH`.

A floating-point literal MUST use JSON number syntax without a leading sign, contain a fraction or exponent, produce a finite IEEE 754 binary64 value, and have type `float64`.

## 3. Grammar

The following EBNF is normative; whitespace separates tokens and is otherwise insignificant:

```ebnf
query                 = source_expression, { "|", stage } ;
source_expression     = "source", identifier, [ start_bound ], [ end_bound ] ;
start_bound           = "start_inclusive", "=", time_expression ;
end_bound             = "end_exclusive", "=", time_expression ;
time_expression       = datetime_constructor | "now" | relative_time ;
relative_time         = time_operation, { time_operation } ;
time_operation        = "@", time_unit | ( "+" | "-" ), [ positive_integer ], time_unit ;
time_unit             = "s" | "m" | "h" | "d" ;
stage                 = filter | project | sort | take | summarize ;
filter                = "filter", expression ;
project               = "project", projection, { ",", projection } ;
projection            = [ identifier, "=" ], expression ;
sort                   = "sort", [ "by" ], sort_item, { ",", sort_item } ;
sort_item              = [ "+" | "-" ], field_reference ;
take                   = "take", signed_integer_literal ;
summarize              = "summarize", measure, { ",", measure }, [ "by", field_reference, { ",", field_reference } ] ;
measure                = [ identifier, "=" ], aggregate_call ;
aggregate_call         = aggregate_name, "(", [ field_reference ], ")" ;
expression             = logical_or ;
logical_or             = logical_and, { "or", logical_and } ;
logical_and            = comparison, { "and", comparison } ;
comparison             = additive, [ comparison_operator, additive ] ;
additive               = multiplicative, { ( "+" | "-" ), multiplicative } ;
multiplicative         = unary, { ( "*" | "/" ), unary } ;
unary                  = [ "not" | "-" ], primary ;
primary                = literal | field_reference | constructor | cast_expression | remainder_expression | "(", expression, ")" ;
constructor            = datetime_constructor | eid_constructor ;
datetime_constructor   = "datetime", "(", string_literal, ")" ;
eid_constructor        = "eid", "(", string_literal, ")" ;
cast_expression        = ( "cast" | "try_cast" ), "(", expression, "as", logical_type, ")" ;
remainder_expression   = ( "rest" | "rest_exists" ), "(", string_literal, ")" ;
literal                = integer_literal | floating_point_literal | string_literal | "true" | "false" | "null" ;
field_reference        = identifier | system_identifier ;
identifier             = unquoted_identifier | quoted_identifier ;
unquoted_identifier    = ? non-reserved or contextual [A-Za-z_][A-Za-z0-9_]* ? ;
quoted_identifier      = "`", ? [A-Za-z_][A-Za-z0-9_]* ?, "`" ;
system_identifier      = ? @[A-Za-z_][A-Za-z0-9_]* ? ;
signed_integer_literal = [ "-" ], integer_literal ;
integer_literal        = ? unsigned decimal integer in [0, 18446744073709551615] ? ;
floating_point_literal = ? unsigned JSON number containing a fraction or exponent and producing finite binary64 ? ;
positive_integer       = ? non-zero unsigned decimal integer ? ;
string_literal         = ? JSON string literal ? ;
comparison_operator    = "==" | "!=" | ">" | ">=" | "<" | "<=" ;
aggregate_name         = "count" | "sum" | "min" | "max" | "avg" ;
logical_type           = "bool" | "int32" | "int64" | "uint32" | "uint64" | "float32" | "float64" | "utf8" | "datetime" | "eid" | "json" ;
```

An omitted signed-operation magnitude means `1`. `positive_integer` MUST be greater than zero. A repeated, reversed, or duplicate source bound MUST produce `QUERY_SYNTAX_ERROR`.

## 4. Time expressions

The query engine MUST capture one UTC millisecond `query_reference_time` for an execution. Every relative time expression in that execution MUST start from that exact value.

`now` MUST resolve to the exact `query_reference_time` without truncation or arithmetic.

Relative operations MUST execute from left to right. A signed operation adds the stated fixed duration; `d` means exactly 24 hours. A truncation operation rounds down in UTC to the named second, minute, hour, or calendar day. Therefore `-1d@h` subtracts 24 hours and then truncates to the hour, while `@h` truncates the reference time without a shift.

`datetime("value")` MUST accept one compile-time RFC 3339 string with `Z` or an explicit numeric offset, normalize it to UTC milliseconds, and reject unrepresentable sub-millisecond precision. `eid("value")` MUST accept exactly 32 lowercase hexadecimal characters and produce one `eid` value. Constructor arity and argument constancy are fixed by grammar.

Each source expression owns its bounds. An explicit `start_inclusive` replaces the execution request's start; an explicit `end_exclusive` replaces the request's end. Each absent bound independently inherits the corresponding request value. The resolved interval denotes `[start_inclusive, end_exclusive)` and MUST satisfy `start_inclusive < end_exclusive`; a violation after applying source bounds MUST produce `QUERY_TIME_RANGE_INVALID`.

AST time values MUST be structured absolute or relative expressions. Typed IR MUST contain structured time operations or resolved UTC instants, never unvalidated strings.

## 5. Field resolution

The source relation MUST expose the active schema from the catalog snapshot. System identifiers MUST resolve only to declared system fields; an unknown system identifier MUST produce `QUERY_FIELD_NOT_FOUND`.

A user identifier MUST resolve in this order:

1. A field in the current relation schema.
2. A top-level key read from the current relation's `@rest` field.

A remainder field MUST have logical type `json`, MUST be nullable, and MUST evaluate to logical null when the key is absent or `@rest` is null. A present key containing JSON null remains a non-absent `json` value. Resolution MUST use the identifier as the exact top-level JSON object key and emit `QUERY_FIELD_RESOLVED_FROM_REMAINDER` with severity `WARNING` for each implicitly resolved field occurrence. A projection that omits `@rest` removes remainder resolution from its output relation; an already projected remainder field remains available under its output name.

`rest("key")` MUST read the exact top-level key from the current relation's `@rest`, return nullable `json`, and preserve the same absent and JSON-null distinction without emitting an implicit-resolution diagnostic. `rest_exists("key")` MUST return non-null `bool`, return `true` when the key is present including with JSON null, and return `false` when the key is absent or `@rest` is null. Both functions MUST require one compile-time JSON string and an `@rest` field in the current relation. Their string argument MAY contain any Unicode scalar sequence representable by a JSON string.

Schema-field resolution takes precedence over implicit remainder resolution. Promoting a remainder key to a schema field therefore changes a bare reference from nullable `json` to the schema field's declared type and removes the warning. Explicit `rest("key")` continues to address the remainder value.

Each stage MUST be analyzed against the preceding stage's output schema. A duplicate output name MUST produce `QUERY_DUPLICATE_OUTPUT_FIELD`.

## 6. Values and nulls

Expressions use catalog logical types plus the polymorphic literal type `null`. Nullability is tracked independently from logical type. A null literal MUST acquire one logical type from context; a null literal without a unique contextual type MUST produce `QUERY_TYPE_MISMATCH`.

Arithmetic requires numeric operands. Exact integer literals MUST acquire a contextual type before ordinary numeric coercion. Other numeric operands MUST coerce to the unique least type reachable from both operand types through zero or more catalog lossless-widening edges; absence of such a type is incompatible. In particular, a typed `int64` or `uint64` operand has no common type with a floating-point operand. Addition, subtraction, multiplication, and division MUST return the common type. Integer division MUST truncate toward zero. Unary minus MUST require a signed integer or floating-point operand. Every arithmetic operator MUST propagate null.

In `int64_field > 1000`, the exact integer literal acquires type `int64`. `int64_field > 1000.5`, `int64_field > float64_field`, and `int64_field > uint64_field` have no lossless common type and MUST produce `QUERY_TYPE_MISMATCH`; the query author MUST choose an explicit `cast` or `try_cast` and its failure semantics.

Ordered comparisons require compatible numeric operands or two operands of the same `utf8`, `datetime`, or `eid` type. Equality and inequality additionally permit `bool`. `json` has no implicit scalar conversion. `expression == null` and `expression != null` lower to null predicates.

`and`, `or`, and `not` MUST require `bool` operands and use SQL three-valued logic. `filter` retains only rows whose predicate is `true`; `false` and `null` are removed.

Constant arithmetic overflow and division by zero MUST produce `QUERY_CONSTANT_EVALUATION_FAILED`. Row-dependent overflow or division by zero MUST produce `QUERY_EVALUATION_FAILED` unless the containing operation explicitly defines null-on-failure behavior.

## 7. Casts

`cast(expression as type)` and `try_cast(expression as type)` MUST perform the same conversion. Null input MUST produce null. An invalid non-null conversion MUST terminate execution with `QUERY_CAST_FAILED` for `cast` and MUST produce null for `try_cast`.

The result of `try_cast` MUST be nullable. The result of `cast` MUST preserve input nullability, and casting from `json` MUST additionally be nullable because a JSON null produces a logical null.

Scalar conversions MUST obey these rules:

- Numeric-to-numeric conversion MUST reject overflow, non-finite values, fractional loss, and integer-to-float values that are not represented exactly.
- `utf8` to integer MUST accept only an optional leading minus for signed targets followed by one or more ASCII decimal digits, with no whitespace or separators.
- `utf8` to floating point MUST accept JSON number syntax and a finite result.
- `utf8` to `bool` MUST accept exactly `true` or `false`.
- `utf8` to `datetime` MUST use the `datetime` constructor's RFC 3339 contract.
- `utf8` to `eid` MUST require exactly 32 lowercase hexadecimal characters.
- Scalar-to-`utf8` conversion MUST use the canonical JSON spelling for numbers and booleans, RFC 3339 UTC with millisecond precision for `datetime`, and lowercase hexadecimal for `eid`.

Casting from `json` to another type MUST inspect its runtime kind. JSON null produces logical null. A boolean or string MUST unwrap to the corresponding scalar and then use the scalar rules above. A number cast to a numeric type MUST parse its preserved JSON token directly into the target type; cast to `utf8` MUST return that exact token. An array or object MUST fail conversion to every non-`json` type.

Casting to `json` MUST preserve a `json` input and encode a scalar as the corresponding canonical JSON scalar. `datetime` and `eid` MUST become JSON strings using their canonical `utf8` encodings.

## 8. Pipeline stages

### 8.1 Filter

`filter` MUST require a nullable or non-null `bool` expression and MUST preserve the input schema.

### 8.2 Project

A bare field projection MUST retain its name, type, nullability, and value. A computed projection MUST use `name = expression`; an unaliased computed expression MUST produce `QUERY_PROJECTION_ALIAS_REQUIRED`.

`project` MUST preserve declaration order, reject duplicate output names, and produce exactly the declared fields. A computed field's type and nullability MUST be inferred from its expression and become available to subsequent stages.

### 8.3 Sort

An unsigned or `+` field sorts ascending; `-` sorts descending. Nulls MUST sort last in both directions. User-visible order is guaranteed only by an explicit `sort` stage.

Sorting MUST reject `json`. Boolean order is `false` before `true`; numeric order is mathematical; `utf8` order is binary lexicographic order over valid UTF-8 bytes; `datetime` order is chronological; and `eid` order is unsigned lexicographic order over its 16 bytes.

### 8.4 Take

`take` MUST accept a non-negative signed 64-bit integer literal and limit the relation at its pipeline position. `take 0` MUST produce a typed empty relation. The value MUST NOT be compared with HTTP output-row limits because later stages can reduce the relation further.

### 8.5 Summarize

Every aggregate measure MUST use `alias = function(argument)`. An unaliased aggregate MUST produce `QUERY_AGGREGATE_ALIAS_REQUIRED`. Measure aliases MUST be unique and MUST NOT collide with group keys.

Aggregate typing MUST follow this table:

| Function | Argument | Result | Null behavior |
|---|---|---|---|
| `count()` | None | Non-null `int64` | Counts rows |
| `count(field)` | Any | Non-null `int64` | Counts non-null values |
| `sum(field)` | `int32`, `int64` | Nullable `int64` | Ignores nulls; checked overflow |
| `sum(field)` | `uint32`, `uint64` | Nullable `uint64` | Ignores nulls; checked overflow |
| `sum(field)` | `float32`, `float64` | Nullable `float64` | Ignores nulls; finite result required |
| `avg(field)` | Numeric | Nullable `float64` | Ignores nulls; null for no values |
| `min(field)`, `max(field)` | Numeric, `utf8`, `datetime`, `eid` | Nullable argument type | Ignores nulls |

The output schema MUST contain group keys in declared order followed by measures in declared order. A global aggregation over an empty relation MUST emit one row; `count` is zero and every other measure is null. A grouped aggregation over an empty relation MUST emit no rows.

An aggregate count or integer sum that exceeds its result domain, or a floating aggregate that produces a non-finite result, MUST terminate execution with `QUERY_EVALUATION_FAILED`.

Group keys MUST reject `json`. Their equality semantics MUST match language equality for the key type.

Only `sort` and `take` MAY follow `summarize`. Their field references MUST resolve against the aggregation output schema.

## 9. Diagnostics

The top-level failure code MUST be `QUERY_SYNTAX_ERROR` when parsing fails and `QUERY_SEMANTIC_ERROR` when semantic analysis, including source resolution, fails. Every diagnostic MUST contain stable severity, stable code, message, and the original UTF-8 byte span when it refers to query text. Successful analysis MAY contain warning diagnostics.

Spans MUST use zero-based half-open byte offsets. Display coordinates MUST use one-based line and Unicode-scalar columns. API-supplied values MUST be identified by their request field path and MUST NOT receive invented query spans.

The diagnostic registry is closed:

| Code | Severity | Condition |
|---|---|---|
| `QUERY_SYNTAX_ERROR` | `ERROR` | Tokenization or grammar failure |
| `QUERY_SOURCE_NOT_FOUND` | `ERROR` | Source name is absent from the catalog snapshot |
| `QUERY_FIELD_NOT_FOUND` | `ERROR` | System field is unknown, or a user field is absent when remainder resolution is unavailable |
| `QUERY_TIME_EXPRESSION_INVALID` | `ERROR` | Relative or absolute time expression is invalid or unrepresentable |
| `QUERY_TIME_BOUND_UNRESOLVED` | `ERROR` | A required source bound cannot inherit or resolve a value |
| `QUERY_TIME_RANGE_INVALID` | `ERROR` | Resolved `start_inclusive` is not before `end_exclusive` |
| `QUERY_LITERAL_INVALID` | `ERROR` | Literal or constructor value is malformed, out of range, or not exactly representable |
| `QUERY_FUNCTION_ARITY_INVALID` | `ERROR` | Function or aggregate arity is invalid |
| `QUERY_FUNCTION_ARGUMENT_TYPE_INVALID` | `ERROR` | Function or aggregate argument type is invalid |
| `QUERY_CAST_INVALID` | `ERROR` | Cast source and target types have no defined conversion |
| `QUERY_PROJECTION_ALIAS_REQUIRED` | `ERROR` | Computed projection omits its alias |
| `QUERY_AGGREGATE_ALIAS_REQUIRED` | `ERROR` | Aggregate measure omits its alias |
| `QUERY_DUPLICATE_OUTPUT_FIELD` | `ERROR` | Relation output contains a duplicate field name |
| `QUERY_STAGE_ORDER_INVALID` | `ERROR` | A stage is forbidden after the preceding stage |
| `QUERY_TAKE_INVALID` | `ERROR` | `take` value is negative or outside signed 64-bit range |
| `QUERY_TYPE_MISMATCH` | `ERROR` | Operator operands, null context, or expression result type is incompatible |
| `QUERY_CONSTANT_EVALUATION_FAILED` | `ERROR` | Compile-time arithmetic overflows or divides by zero |
| `QUERY_FIELD_RESOLVED_FROM_REMAINDER` | `WARNING` | Bare user identifier resolves implicitly from `@rest` |

An implementation MUST NOT emit another query diagnostic code under v0. Multiple diagnostics MAY describe independent occurrences and MUST be ordered by start byte, end byte, severity with `ERROR` before `WARNING`, and code.

The closed registry governs syntax and semantic diagnostics. `QUERY_CAST_FAILED` and `QUERY_EVALUATION_FAILED` are runtime execution error codes carried by the service error envelope and MUST NOT appear as diagnostics.
