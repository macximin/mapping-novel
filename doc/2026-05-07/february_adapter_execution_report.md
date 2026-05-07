# 2026-02 adapter execution report

## Scope
- Adapter registry was moved into code in `settlement_adapters.py`.
- The Streamlit app now routes uploaded settlement files through the adapter before `build_mapping()`.
- Latest-source fixture tests cover 53 known source files across 37 platforms.
- 2026-02 source files were used as an independent execution run.

## Registry / Fixture Result
- Registry platforms: 37
- Default-feed blocked by design: 2
  - `보인&국립장애인도서관`: non-S2 purchase/selection material
  - `알라딘 종이책`: non-S2 paper-book ledger material
- Latest fixture files tested by unittest: 53
- Latest fixture parse result: all passed
- Test command: `python -m unittest discover -s tests`

## 2026-02 Materialization Result
- 2026-02 source files found: 35
- Platforms with 2026-02 source: 22
- Parse errors: 0
- Materialized default-feed platforms: 22
- Platforms without 2026-02 source in the shared folder: 15

Mapping smoke status:
- `pass_after_substitution`: 7
- `pass_with_review`: 15
- `skipped_no_default_feed`: 15

Interpretation:
- The 2026-02 adapters can create system-feed material for every platform that has a 2026-02 source file.
- `pass_with_review` is mainly a current title-cleaning/matching risk, not a parser crash. The smoke creates synthetic S2/IPS rows from source titles; current `clean_title()` can collapse volume/episode/title variants into the same key.

## Amount Policy Gate
- S2 projection candidate after policy lock: 11 platforms
- Still blocked by amount/policy: 24 platforms
- Blocked non-S2: 2 platforms

Legacy `_매핑.xlsx` amount comparison for 2026-02:
- `sale_settlement_offset_match`: 2
- `settlement_match_only`: 9
- `mismatch_needs_policy_review`: 10
- `no_legacy_mapping`: 16

Immediate amount-policy target:
- Use `settlement_match_only` platforms to decide whether legacy S2 used settlement amount as the stable anchor while sale/offset remained derived or policy-specific.
- Review `mismatch_needs_policy_review` platforms before enabling S2 4-column export.
- Do not emit final S2 upload rows from platforms whose `s2_projection_status` is blocked.

## Outputs
- `february_adapter_materialization_files.csv`
- `february_adapter_materialization_platforms.csv`
- `february_adapter_materialization_amount_policy_audit.csv`
- `february_adapter_materialization_summary.json`
- `february_adapter_materialization_report.md`
- `legacy_s2_transfer_candidate_scan.csv`
- `february_legacy_mapping_amount_reconcile.csv`
