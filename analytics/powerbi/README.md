# Power BI query set

One file per import target. Paste into Power BI's SQL box after connecting through the
`signal-athena` DSN — setup, credentials, and the case for Import over DirectQuery are in
[`docs/powerbi.md`](../../docs/powerbi.md); the decision to add a BI reader at all is
ADR-0012.

Every query is projected rather than `SELECT *`, and filtered on the partition column
where the table has one. Each file's header says what its NULLs mean — several of these
columns use NULL for "not measured", which is not zero.

Verified against the deployed lake (account 481879233905) on 2026-08-26: 789 rows,
0.29 MB scanned, $0.000336 for the full set.
