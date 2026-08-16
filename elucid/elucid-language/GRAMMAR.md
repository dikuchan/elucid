# Grammar Reference

The formal grammar is defined in [`grammar.ebnf`](grammar.ebnf).

## Examples

```elucid
source logs

source logs | filter status >= 400

source logs | filter method == "POST" and status != 200

source logs | sort by -count, +status

source logs | take 100

source logs | summarize total = sum(bytes), count() by method, status

source logs
  | filter status >= 500
  | summarize count() by path
  | sort by -count
  | take 10
```
