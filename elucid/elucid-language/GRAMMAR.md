# Grammar Reference

The formal grammar is defined in [`grammar.ebnf`](grammar.ebnf).

## Examples

```elucid
dataset logs

dataset logs | where status >= 400

dataset logs | where method == "POST" and status != 200

dataset logs | sort by -count, +status

dataset logs | head 100

dataset logs | aggr total = sum(bytes), count() by method, status

dataset logs
  | where status >= 500
  | aggr count() by path
  | sort by -count
  | head 10
```
