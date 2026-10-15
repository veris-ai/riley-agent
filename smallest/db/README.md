# db

The card-ops data the agent's tools read and write: `users`, `cards`, and
`replacements` (see `app/db.py` for the access layer).

## `init.sql`

Schema (tables, `CHECK` constraints, `COMMENT`s) plus the seed users and cards —
everything needed to stand up the database.

Veris runs a real Postgres and seeds it from this file. The path is set in
`.veris/veris.yaml`:

```yaml
services:
  - name: postgres
    config:
      SCHEMA_PATH: /agent/db/init.sql
```

`replacements` starts empty; rows are created at runtime by the
`request_card_replacement` tool.
