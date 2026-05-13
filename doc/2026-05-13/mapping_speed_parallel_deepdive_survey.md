# Mapping Speed Parallel Deepdive Survey

Date: 2026-05-13

## Scope

Survey target: Streamlit `어댑터 정규화 및 S2 매핑 실행` path when many settlement files are uploaded at once.

Parallel survey split:

- App orchestration: `app.py` processing, result materialization, ZIP/CSV generation
- Excel I/O and adapters: `settlement_adapters.py`, `mapping_core.export_mapping`, `s2_transfer.export_s2_transfer`
- Matching core: `mapping_core.py`, `matching_rules.py`, `s2_reference_guards.py`
- Parallel/runtime: `parallel_mapping.py`, Streamlit progress, Cloud constraints

No code changes were made by the survey agents.

## Current Execution Path

1. `app.py` loads S2 basis and guard lookup files.
2. `process_settlement_files()` resolves worker count. Current default and max are both 2.
3. Each file runs in a `ThreadPoolExecutor` worker:
   - `normalize_settlement()`
   - `filter_s2_by_sales_channel()`
   - `build_mapping()`
   - `annotate_mapping_result()`
   - `build_s2_transfer()`
   - `export_mapping()`
   - optional `export_s2_transfer()`
4. After all files complete, `build_mapping_session_state()` creates:
   - summary frame
   - PD work order CSV
   - combined mapping CSV
   - full ZIP bytes
5. Download buttons render already materialized bytes.

## Highest Confidence Bottlenecks

### 1. Per-file S2 and Guard Index Rebuild

`build_mapping()` and `annotate_mapping_result()` rebuild expensive S2-derived structures for every uploaded file.

Key repeated work:

- clean S2 titles with `clean_title`
- filter 124,755 S2 rows by channel
- group S2 rows by clean key
- parse auto-select date columns inside candidate groups
- rebuild missing/billing/service/payment/master guard indexes
- rebuild full `s2_all_frame` payment index in `annotate_mapping_result`

Observed by explorer on a synthetic Yes24 case:

- `filter_s2_by_sales_channel`: about 0.043s
- `build_mapping(filtered_s2, 1000-row settlement)`: about 6.845s
- `annotate_mapping_result(..., full S2 frame)`: about 8.665s

Likely fix:

- Build batch-level S2/guard runtime context once.
- Precompute `_정제키`, normalized channel, parsed auto-select date, channel-filtered frames, and guard indexes.
- Reuse across all files in the same run.

Expected effect: high for multi-file batches.

Risk: medium. Must preserve duplicate candidate latest-date selection and guard annotation output exactly.

### 2. Immediate Per-file XLSX Export

Each successful file immediately creates mapping XLSX bytes via openpyxl.

Expensive behavior:

- writes 5 sheets
- styles headers
- colors every review row
- scans worksheet columns/cells to auto-size width

Explorer synthetic benchmark:

- `export_mapping()` on 30,000 rows x 36 columns: about 32.38s.

Likely fix:

- Limit column-width auto sizing to header + top N rows.
- Consider a fast export mode.
- Eventually make file artifacts lazy: map first, generate XLSX/ZIP when requested.

Expected effect: high for large files.

Risk: low to medium for width-sampling; medium for lazy artifact design.

### 3. Workbook Load and Whole-sheet Materialization

`normalize_settlement()` uses `load_workbook(..., read_only=False)` and `_sheet_rows()` materializes whole sheets.

Explorer measured 51 fixture normalizations:

- total: about 44.17s
- 리디북스: about 14.84s, workbook load about 11.20s
- 미스터블루: about 9.15s, workbook load about 6.03s
- 원스토어: about 8.91s, workbook load about 6.01s

Likely fix:

- Use `read_only=True` streaming path where merged-cell handling is not needed.
- Keep existing full workbook path only for platforms that depend on merged cells.
- Longer term: header scan first, then parse needed rows/columns only.

Expected effect: high for large source Excel files.

Risk: medium to high. Platform fixtures are essential.

### 4. Duplicate Combined Report Work

`build_mapping_session_state()` calls:

- `build_pd_work_order_report_frame(results)`, which internally builds combined report
- `build_combined_mapping_report_frame(results)` again

This duplicates copies, column normalization, concat, and sort across all successful mapping rows.

Likely fix:

- Build combined report once.
- Add `build_pd_work_order_report_frame_from_combined(combined)` or optional `combined` parameter.

Expected effect: medium, easy quick win.

Risk: low.

### 5. Immediate CSV/ZIP Materialization

Even if the user does not download everything, the app immediately creates:

- work order CSV bytes
- combined CSV bytes
- full ZIP bytes

ZIP also recomputes CSV bytes and compresses already-compressed XLSX.

Likely fix:

- Reuse CSV bytes in ZIP.
- Store `.xlsx` entries with `ZIP_STORED` instead of deflating again.
- Add progress step for post-processing.
- Longer term: lazy ZIP generation.

Expected effect: medium to high for 38-file batches.

Risk: low for byte reuse and `ZIP_STORED`; medium for lazy generation.

## Parallelism Findings

Current structure is conservative and mostly safe:

- file-level parallelism
- `ThreadPoolExecutor`
- default workers: 2
- max workers: 2
- uploaded files are snapshotted only when submitted
- worker threads do not call Streamlit UI directly

Do not make Cloud default 3+ workers yet.

Reason:

- openpyxl export and Python loops are memory-heavy
- `ProcessPoolExecutor` would pickle large DataFrames/bytes and likely increase memory pressure
- Streamlit Cloud resource limits make 2 workers a safer default

Potential opt-in:

- Add separate local-only upper bound such as `MAPPING_PARALLEL_WORKERS_MAX`.
- Keep Cloud default at 2 and allow `MAPPING_PARALLEL_WORKERS=1` rollback.

## Recommended Implementation Order

### Phase 0: Instrumentation

Add timing logs to:

- normalize
- S2 filter
- build mapping
- guard annotation
- transfer validation
- XLSX export
- batch report build
- ZIP build

Goal: show per-stage seconds in summary without changing behavior.

### Phase 1: Low-risk Quick Wins

1. Reuse combined report when building PD work order.
2. Avoid duplicate CSV encoding between standalone downloads and ZIP.
3. Use `ZIP_STORED` for `.xlsx` files.
4. Replace `summarize_normalization()` default-feed copy with mask/count logic.
5. Add progress text for post-processing: report build, ZIP build.

### Phase 2: Medium-risk High-value Work

1. Precompute batch-level guard indexes.
2. Precompute S2 clean key, normalized channel, and parsed date.
3. Cache channel-filtered S2 frames or channel candidate indexes.
4. Optimize `_candidate_index` to parse dates once per frame, not per group.
5. Use `itertuples()` or vectorized masks for review/guard row loops.

### Phase 3: Larger Structural Work

1. Streaming workbook reader for platforms that do not need merged-cell expansion.
2. Lazy artifact generation for individual XLSX and full ZIP.
3. Optional fast export mode with reduced styling/auto-width.
4. Local-only worker count experiments above 2.

## Test Requirements

Before changing matching/index logic:

- duplicate S2 key latest-registration selection remains identical
- invalid/blank date fallback order remains identical
- disabled marker filtering remains identical
- guard annotation output columns remain identical
- same S2/guard context reused across multiple files returns identical results
- existing `test_mapping_core.py`, `test_s2_reference_guards.py`, `test_matching_rules.py` all pass

Before changing report/ZIP generation:

- combined report row count and sort are unchanged
- PD work order grouping is unchanged
- ZIP entries and download file names are unchanged except compression method
- failure files still produce error txt entries

Before changing adapter I/O:

- fixture normalize row counts stay identical
- sheet audit statuses stay identical
- merged-cell dependent platforms are explicitly covered

## Recommended Next Move

Start with Phase 0 + Phase 1. They are low-risk and will tell us whether the dominant live bottleneck is export, guard annotation, or workbook loading.

The first actual patch should probably be:

1. add stage timing fields to `process_settlement_batch_item`
2. reuse combined report in `build_mapping_session_state`
3. reuse CSV bytes and store XLSX uncompressed inside ZIP

This gives immediate user-visible insight and small speed wins without changing matching behavior.
