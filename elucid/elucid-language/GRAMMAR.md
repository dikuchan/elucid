# Grammar Reference

The formal grammar is defined in [`grammar.ebnf`](grammar.ebnf).

## Examples

```elucid
source logs

source logs | filter status >= 400

source logs | filter method == "POST" and status != 200

source logs | sort by -count, +status

source logs | take 100

source logs | summarize total = sum(bytes), event_count = count() by method, status

source logs start_inclusive=-1d@h end_exclusive=@h | filter status >= uint32(400)

source logs | project status, parsed = try_cast(rest("status") as uint32)

source logs
  | filter status >= 500
  | summarize event_count = count() by path
  | sort by -event_count
  | take 10
```
