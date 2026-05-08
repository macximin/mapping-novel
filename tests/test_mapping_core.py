from __future__ import annotations

import unittest

import pandas as pd

from mapping_core import (
    MATCH_NONE,
    MATCH_OK,
    build_mapping,
    clean_master_title,
    drop_disabled_rows,
    extract_master_work_title,
)


class MappingCoreTest(unittest.TestCase):
    def test_angle_bracket_master_title_is_extracted(self) -> None:
        raw = "<그 남자의 비밀>_홍길동_1001234_2002345_확정"

        self.assertEqual(extract_master_work_title(raw), "그 남자의 비밀")
        self.assertEqual(clean_master_title(raw), "그남자의비밀")

    def test_legacy_confirmed_master_title_drops_suffix_metadata(self) -> None:
        raw = "너_그리고_나_홍길동_1001234_2002345_확정"

        self.assertEqual(extract_master_work_title(raw), "너_그리고_나")
        self.assertEqual(clean_master_title(raw), "너그리고나")

    def test_unique_match_keeps_ids_in_id_columns(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["그 남자의 비밀"], "판매채널콘텐츠ID": ["S2-1"], "콘텐츠ID": ["CID-1"]})
        settlement = pd.DataFrame({"작품명": ["그 남자의 비밀 1화"], "금액": [1000]})
        master = pd.DataFrame({"콘텐츠명": ["<그 남자의 비밀>_홍길동_100_200_확정"], "콘텐츠ID": ["CID-1"]})

        rows = build_mapping(s2, settlement, master).rows

        self.assertEqual(rows.loc[0, "S2_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[0, "S2_판매채널콘텐츠ID"], "S2-1")
        self.assertEqual(rows.loc[0, "S2_콘텐츠ID"], "CID-1")
        self.assertEqual(rows.loc[0, "IPS_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[0, "IPS_콘텐츠ID"], "CID-1")
        self.assertEqual(rows.loc[0, "검토필요(Y/N)"], "N")

    def test_ips_master_is_optional_for_s2_mapping(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["그 남자의 비밀"], "판매채널콘텐츠ID": ["S2-1"], "콘텐츠ID": ["CID-1"]})
        settlement = pd.DataFrame({"작품명": ["그 남자의 비밀"], "금액": [1000]})

        mapping = build_mapping(s2, settlement, None)
        rows = mapping.rows

        self.assertEqual(rows.loc[0, "S2_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[0, "S2_판매채널콘텐츠ID"], "S2-1")
        self.assertEqual(rows.loc[0, "S2_콘텐츠ID"], "CID-1")
        self.assertEqual(rows.loc[0, "IPS_매칭상태"], "skipped")
        self.assertEqual(rows.loc[0, "검토필요(Y/N)"], "N")

    def test_no_match_does_not_fallback_to_normalized_title_as_id(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["다른 작품"], "판매채널콘텐츠ID": ["S2-1"]})
        settlement = pd.DataFrame({"작품명": ["그 남자의 비밀"], "금액": [1000]})
        master = pd.DataFrame({"콘텐츠명": ["다른 작품"], "콘텐츠ID": ["CID-1"]})

        rows = build_mapping(s2, settlement, master).rows

        self.assertEqual(rows.loc[0, "S2_매칭상태"], MATCH_NONE)
        self.assertEqual(rows.loc[0, "S2_판매채널콘텐츠ID"], "")
        self.assertEqual(rows.loc[0, "IPS_매칭상태"], MATCH_NONE)
        self.assertEqual(rows.loc[0, "IPS_콘텐츠ID"], "")
        self.assertEqual(rows.loc[0, "검토필요(Y/N)"], "Y")

    def test_disabled_marker_rows_are_not_used_as_reference_candidates(self) -> None:
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["정상 작품", "[사용안함]_삭제 작품"],
                "판매채널콘텐츠ID": ["S2-1", "S2-2"],
                "콘텐츠ID": ["CID-1", "CID-2"],
            }
        )
        settlement = pd.DataFrame({"작품명": ["삭제 작품", "정상 작품"]})

        rows = build_mapping(s2, settlement, None).rows

        self.assertEqual(rows.loc[0, "S2_매칭상태"], MATCH_NONE)
        self.assertEqual(rows.loc[0, "S2_판매채널콘텐츠ID"], "")
        self.assertEqual(rows.loc[1, "S2_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[1, "S2_판매채널콘텐츠ID"], "S2-1")

    def test_drop_disabled_rows_checks_all_reference_cells(self) -> None:
        frame = pd.DataFrame(
            {
                "콘텐츠명": ["정상 작품", "삭제 작품", "다른 삭제 작품"],
                "메모": ["", "(사용금지)", ""],
                "상태": ["", "", "[사용안함]"],
            }
        )

        filtered = drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품"])

    def test_duplicate_master_key_auto_selects_first_candidate(self) -> None:
        s2 = pd.DataFrame({"콘텐츠명": ["그 남자의 비밀"], "판매채널콘텐츠ID": ["S2-1"]})
        settlement = pd.DataFrame({"작품명": ["그 남자의 비밀"], "금액": [1000]})
        master = pd.DataFrame(
            {
                "콘텐츠명": [
                    "<그 남자의 비밀>_홍길동_100_200_확정",
                    "<그 남자의 비밀>_김철수_101_201_확정",
                ],
                "콘텐츠ID": ["CID-1", "CID-2"],
            }
        )

        mapping = build_mapping(s2, settlement, master)
        rows = mapping.rows

        self.assertEqual(rows.loc[0, "IPS_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[0, "IPS_콘텐츠ID"], "CID-1")
        self.assertEqual(rows.loc[0, "IPS_후보수"], "2")
        self.assertEqual(rows.loc[0, "IPS_후보ID목록"], "CID-1 | CID-2")
        self.assertEqual(rows.loc[0, "검토필요(Y/N)"], "N")
        self.assertEqual(len(mapping.duplicate_candidates), 1)

    def test_duplicate_s2_key_auto_selects_latest_registration_date(self) -> None:
        s2 = pd.DataFrame(
            {
                "콘텐츠명": ["그 남자의 비밀", "그 남자의 비밀"],
                "판매채널콘텐츠ID": ["S2-old", "S2-new"],
                "콘텐츠ID": ["CID-old", "CID-new"],
                "지급정산마스터_등록일자": ["2025-01-01T00:00:00", "2026-01-01T00:00:00"],
            }
        )
        settlement = pd.DataFrame({"작품명": ["그 남자의 비밀"], "금액": [1000]})

        mapping = build_mapping(s2, settlement, None)
        rows = mapping.rows
        duplicate = mapping.duplicate_candidates.iloc[0]

        self.assertEqual(rows.loc[0, "S2_매칭상태"], MATCH_OK)
        self.assertEqual(rows.loc[0, "S2_판매채널콘텐츠ID"], "S2-new")
        self.assertEqual(rows.loc[0, "S2_콘텐츠ID"], "CID-new")
        self.assertEqual(rows.loc[0, "S2_후보ID목록"], "S2-new | S2-old")
        self.assertEqual(duplicate["자동선택기준"], "지급정산마스터_등록일자 최신순")
        self.assertEqual(duplicate["자동선택기준값"], "2026-01-01T00:00:00")


if __name__ == "__main__":
    unittest.main()
