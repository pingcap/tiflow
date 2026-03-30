# Replace `round()` with `CAST(... AS DECIMAL(65, 30))` in sync-diff-inspector

**Goal:** Replace `round()` with `CAST(... AS DECIMAL(65, 30))` for float/double columns in sync-diff-inspector to enable TiKV coprocessor pushdown and fix the zero-value NULL bug.

**Architecture:** Two code sites in `sync_diff_inspector/utils/utils.go` generate SQL that wraps float/double columns with `round()`. Replace both with `CAST(... AS DECIMAL(65, 30))`, update the stale comments, and fix the unit test assertion.

**Tech Stack:** Go, MySQL/TiDB SQL

Issue: https://github.com/pingcap/tidb-tools/issues/859

## Background

sync-diff-inspector compares data between an upstream database (MySQL or TiDB) and a
downstream database (TiDB) using a two-phase strategy:

1. **Checksum phase** (`GetCountAndMD5Checksum`): For each chunk of rows, compute a
   `BIT_XOR` of per-row MD5 hashes on both sides. If the checksums and counts match, the
   chunk is identical -- no further work needed.
2. **Row-by-row phase** (`GetTableRowsQueryFormat`): If the checksums differ, fetch the
   actual rows from both sides, iterate through them in primary-key order, and emit
   INSERT/DELETE/REPLACE fix-SQL for each mismatch.

Both phases must normalize `FLOAT`/`DOUBLE` columns before comparison because the IEEE 754
binary representation of a floating-point value can produce slightly different decimal
strings across database engines or versions. Without normalization, two databases holding
identical data could yield different checksums.

## Problem

The current normalization wraps float/double columns with `round()`:

```sql
-- For FLOAT (~7 significant digits):
round(`col`, 5 - floor(log10(abs(`col`))))

-- For DOUBLE (~15 significant digits):
round(`col`, 14 - floor(log10(abs(`col`))))
```

This has two problems:

### 1. `round()` cannot be pushed down to TiKV

TiDB's expression pushdown blocklist
([infer_pushdown.go](https://github.com/pingcap/tidb/blob/07f4eda04057923b0588eb8d62be45962fa7658b/pkg/expression/infer_pushdown.go#L160))
includes `round`. When a checksum query contains `round`, the entire aggregation
(`BIT_XOR`, `MD5`, `CONCAT_WS`, ...) must execute on the TiDB node rather than being
distributed across TiKV coprocessors.

Verified by `EXPLAIN` on TiDB v8.5:

```
-- With round(): aggregation stays on root (TiDB)
StreamAgg_8    root      funcs:bit_xor(...)
└─Projection   root      cast(round(...))
  └─TableReader root
    └─TableFullScan cop[tikv]

-- With CAST(... AS DECIMAL): aggregation pushed to cop[tikv]
HashAgg_11     root      funcs:bit_xor(Column#5)
└─TableReader  root      data:HashAgg_5
  └─HashAgg_5  cop[tikv]  funcs:bit_xor(cast(conv(substring(md5(concat_ws(...))))))
    └─TableFullScan cop[tikv]
```

For large tables this is a significant performance bottleneck: every row must be sent from
TiKV to TiDB before the checksum can be computed, instead of computing partial checksums
locally on each TiKV region.

### 2. `round()` returns NULL when the column value is zero

`log10(abs(0))` is mathematically undefined (`-inf`), so the expression
`round(0, 5 - floor(-inf))` evaluates to `NULL`:

```sql
mysql> SELECT round(0.0, 5-floor(log10(abs(0.0))));
+--------------------------------------+
| round(0.0, 5-floor(log10(abs(0.0)))) |
+--------------------------------------+
|                                 NULL |
+--------------------------------------+
```

The code comments acknowledge this:

```go
// When col value is 0, the result is NULL.
// But we can use ISNULL to distinguish between null and 0.
```

The checksum query appends `ISNULL(col)` columns to the `CONCAT_WS` so that a genuine
NULL and a zero produce different hash inputs. This works but is a workaround for a problem
that should not exist.

## Solution

Replace `round(col, N - floor(log10(abs(col))))` with `CAST(col AS DECIMAL(65, 30))`.

### Why `CAST(... AS DECIMAL)` works

**Pushdown-compatible.** `CAST` to `DECIMAL` is not on TiDB's pushdown blocklist. The
`EXPLAIN` output above confirms the full checksum expression is pushed to `cop[tikv]`.

**Deterministic.** For any given IEEE 754 float/double bit pattern, `CAST(col AS
DECIMAL(65, 30))` always produces the same decimal string. This guarantees that the same
stored value yields the same checksum on both upstream and downstream, which is the only
correctness requirement for sync-diff-inspector.

**Handles zero correctly.** `CAST(0 AS DECIMAL(65, 30))` returns
`0.000000000000000000000000000000`, not NULL. The existing `ISNULL` columns in the
checksum query continue to work, and the zero-value edge case is no longer special.

### Precision analysis

`DECIMAL(65, 30)` allows up to 35 integer digits and 30 fractional digits. This is the
maximum scale permitted by MySQL/TiDB.

**FLOAT (single precision, ~7 significant digits):**

| Value magnitude | Significant digits preserved by DECIMAL(65,30) | Sufficient? |
|---|---|---|
| > 1e35 | All significant digits in integer part (up to 35 digits) | Float only has ~7 sig digits, 35 integer digits is more than enough |
| 1e-24 to 1e35 | All ~7 significant digits | Yes |
| < 1e-24 | Fewer than 7 sig digits (fractional part truncated at 30 places) | Edge case -- values this small are extremely rare in practice |

**DOUBLE (double precision, ~15 significant digits):**

| Value magnitude | Significant digits preserved by DECIMAL(65,30) | Sufficient? |
|---|---|---|
| > 1e35 | Up to 35 integer digits; double only has ~15 sig digits | Yes |
| 1e-16 to 1e35 | All ~15 significant digits | Yes |
| < 1e-16 | Fewer than 15 sig digits (e.g., 1.23e-16 keeps 14 sig digits) | Minor edge case |

Tested on MySQL 8.0:

```sql
-- Double value at 1e-15: full precision preserved
mysql> SELECT cast(cast(1.234567890123456e-15 as double) as decimal(65,30));
  → 0.000000000000001234567890123456  -- all 15 sig digits preserved

-- Double value at 1e-16: one sig digit lost
mysql> SELECT cast(cast(1.234567890123456e-16 as double) as decimal(65,30));
  → 0.000000000000000123456789012346  -- 14 sig digits (last digit rounded)
```

In practice, float/double columns rarely store values with magnitude below 1e-16. The
current `round()` approach also has precision limitations -- it keeps only 6 or 15
significant digits and produces NULL for zero. `CAST(... AS DECIMAL(65, 30))` is strictly
better for all practical values.

### Why use `DECIMAL(65, 30)` specifically?

- **65** is the maximum precision (total digits) for DECIMAL in MySQL/TiDB. This ensures
  we never overflow for any representable float/double value.
- **30** is the maximum scale (fractional digits) for DECIMAL in MySQL/TiDB. This
  maximizes the precision preserved for small values.
- Using a single `(65, 30)` for both FLOAT and DOUBLE simplifies the code -- no need for
  separate format strings per type.

### Alternatives considered

| Approach | Verdict |
|---|---|
| `CAST(col AS DECIMAL(65, 30))` | **Chosen.** Pushdown-compatible, deterministic, fixes zero bug, sufficient precision. |
| `CAST(col AS CHAR)` | Preserves exact representation but may produce different string formats (scientific notation) across MySQL vs TiDB. Pushdown status unverified. |
| Remove wrapper entirely | Not viable. `CONCAT_WS` on raw float produces inconsistent formats (e.g., `1.23457e-6` on one side vs `0.00000123457` on the other). |
| Remove `round` from TiDB pushdown blocklist | Fixes pushdown but not the `log10(0) → NULL` bug. Also introduces upstream dependency — sync-diff-inspector cannot ship the fix independently, and `round` may be on the blocklist for correctness reasons (edge-case behavior differences between TiDB and TiKV). |
| `HEX(col)` (bit-exact comparison) | Zero precision loss and no NULL issue, but **not viable for cross-engine comparison**. The same logical value (e.g., `3.14`) may have slightly different IEEE 754 bit patterns after binlog replication or string-to-float conversion, causing false mismatches. |
| `ROUND(col, fixed_N)` without `log10` | Fixes the zero-value NULL bug by removing `log10(abs(col))`, but `round` is still on TiDB's pushdown blocklist — the core performance problem remains unsolved. |
| `FORMAT(col, 30)` | Forces fixed-point string output, but `FORMAT()` is locale-dependent (inserts thousands separators like `1,234.56`), making results inconsistent across environments. Pushdown status unverified. |
| Hybrid: CAST for checksum, keep `round()` for row-level | Only changes the performance-critical checksum path. However, using different normalization in the two phases risks correctness: if `CAST` and `round` produce different values for the same input, a checksum mismatch would trigger the row-level phase, which could then report spurious diffs or miss real ones. |
| Application-side normalization (Go `strconv.FormatFloat`) | Fully controllable, but **cannot apply to the checksum phase** since checksums are computed server-side via `BIT_XOR(MD5(CONCAT_WS(...)))`. Only useful for the row-level phase, which is not the performance bottleneck. |