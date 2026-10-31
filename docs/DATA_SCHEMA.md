# Public Data Schema

The production snapshot contained a populated SQLite database and live flow history. Those records are not published.

## `flow_items`

| Column | Meaning |
|---|---|
| `identity` | Stable event identity (`code:` when available, SHA-256 fallback) |
| `symbol` | Underlying symbol |
| `strategy` | Normalized strategy text |
| `strike` | One or more normalized strikes |
| `option_type` | `call`, `put`, `mixed`, `stock`, or `unknown` |
| `expiration` | Normalized expiration date(s) |
| `flow_date` | Flow date converted to configured display timezone |
| `time` | Flow time converted to configured display timezone |
| `premium` | Premium value |
| `inserted_at` | UTC persistence timestamp |

## `insider_flow`

| Column | Meaning |
|---|---|
| `identity` | Stable insider-flow identity |
| `report_date` | Normalized filing/report time |
| `trade` | Normalized buy/sell/acquire/dispose text |
| `member` | Insider/member display name |
| `premium` | Absolute `price × shares` value |
| `inserted_at` | UTC persistence timestamp |
