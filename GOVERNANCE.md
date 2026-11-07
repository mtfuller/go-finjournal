# Governance

## Project Model

`go-finjournal` is released under the Apache-2.0 License. Contributions require a Developer Certificate of Origin (`Signed-off-by` on commits) rather than a CLA.

## RFC Process

Any breaking or additive change to:
- Public Go APIs (`pkg/ledger`, `pkg/money`, `pkg/storage`, `pkg/module`)
- The REST `/v1` OpenAPI contract (`server/openapi.yaml`)
- Database schemas (`pkg/storage/postgres/migrations`)

goes through an RFC in `rfcs/`.
