from __future__ import annotations

import unittest

import pandas as pd

from cleaning_rules import CleaningPolicy


class CleaningRulesTest(unittest.TestCase):
    def test_policy_extracts_and_cleans_master_title(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.extract_master_work_title("<그 남자의 비밀>_홍길동_100_200_확정"), "그 남자의 비밀")
        self.assertEqual(policy.clean_master_title("너_그리고_나_홍길동_100_200_확정"), "너그리고나")

    def test_policy_extracts_confirmed_master_suffix_variants(self) -> None:
        policy = CleaningPolicy()
        cases = {
            "재벌가 차남은 먼치킨_말리브해적_1003258_472_확정": "재벌가 차남은 먼치킨",
            "재벌가 차남은 먼치킨_말리브해적_1003258_미연결_확정": "재벌가 차남은 먼치킨",
            "재벌가 차남은 먼치킨_말리브해적_1003258_선인세없음_확정": "재벌가 차남은 먼치킨",
            "너_그리고_나_홍길동_1001234_2002345_확정": "너_그리고_나",
        }

        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(policy.extract_confirmed_master_title(raw), expected)
                self.assertEqual(policy.extract_master_work_title(raw), expected)

    def test_default_disabled_markers_drop_rows_before_mapping(self) -> None:
        policy = CleaningPolicy()
        frame = pd.DataFrame(
            {
                "콘텐츠명": [
                    "정상 작품",
                    "[사용안함] 삭제 작품",
                    "(사용안함) 삭제 작품",
                    "[사용금지] 삭제 작품",
                    "(사용금지) 삭제 작품",
                    "[정산정보없음] 삭제 작품",
                    "(정산정보없음) 삭제 작품",
                    "[사용안함]_[정산정보없음] 이중 차단 작품",
                    "[정산정보없음]_[사용안함] 역순 이중 차단 작품",
                    "(사용안함)_(정산정보없음) 괄호 이중 차단 작품",
                ]
            }
        )

        filtered = policy.drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품"])

    def test_policy_can_override_disabled_markers(self) -> None:
        policy = CleaningPolicy(disabled_row_markers=("DROP",))
        frame = pd.DataFrame({"콘텐츠명": ["정상 작품", "DROP 삭제 작품", "[사용안함]_기본표식"]})

        filtered = policy.drop_disabled_rows(frame)

        self.assertEqual(filtered["콘텐츠명"].tolist(), ["정상 작품", "[사용안함]_기본표식"])

    def test_policy_preserves_whole_square_wrapped_titles(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.clean_title("[효종]"), "효종")
        self.assertEqual(policy.clean_title("[BL] 효종"), "효종")
        self.assertEqual(policy.clean_title("[BL]"), "")
        self.assertEqual(policy.clean_title("[사용안함]"), "")

    def test_policy_preserves_title_final_je_before_episode_numbers(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.clean_title("파공검제 30권"), "파공검제")
        self.assertEqual(policy.clean_title("마교육제 242화"), "마교육제")
        self.assertEqual(policy.clean_title("수라검제 2권"), "수라검제")
        self.assertEqual(policy.clean_title("파공검 제30권"), "파공검")
        self.assertEqual(policy.clean_title("검(劍) 1권"), "검")

    def test_policy_removes_episode_markers_with_inner_spaces(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.clean_title("평화로운 먼치킨 영지 5 화"), "평화로운먼치킨영지")
        self.assertEqual(policy.clean_title("평화로운 먼치킨 영지 10 화"), "평화로운먼치킨영지")

    def test_policy_extracts_structured_s2_title_segment(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.clean_title("0_야한규칙으로 다 따먹음_파워레인젖_일반"), "야한규칙으로다따먹음")
        self.assertEqual(policy.clean_title("12_조금은 야한 우리 회사_파워레인젖_일반"), "조금은야한우리회사")
        self.assertEqual(policy.clean_title("0_야한규칙으로 다 따먹음_파워레인젖_비일반"), "야한규칙으로다따먹음파워레인젖비일반")

    def test_policy_extracts_contract_parenthesized_title(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(
            policy.clean_title("배타적 발행권 설정계약서_김현준 작가_신작 1종 (EX급 만능사제가 되었다)"),
            "ex급만능사제가되었다",
        )
        self.assertEqual(policy.clean_title("악작(현대AU 외전)"), "악작")

    def test_policy_applies_only_confirmed_exact_title_aliases(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(
            policy.clean_title("늙은 경비에게 조교당하는 스튜어디스의 이야기 15화"),
            "늙은경비에게조교당하는스튜어디스",
        )
        self.assertEqual(
            policy.clean_title("[연재] 늙은 경비에게 조교 당하는 스튜어디스 이야기"),
            "늙은경비에게조교당하는스튜어디스",
        )
        self.assertEqual(policy.clean_title("천대받는 F급 힐러라 좋았는데요?"), "천대받는f급힐러라서좋았는데요")
        self.assertEqual(policy.clean_title("던전에서 성자가 하는 일 [단행본]"), "던전에서성자性者가하는일")
        self.assertEqual(policy.clean_title("백치 공주 시리즈"), "백치공주")
        self.assertEqual(policy.clean_title("악작 현대AU"), "악작")
        self.assertEqual(policy.clean_title("보수적이라며 팬티는 왜 벗어?"), "보수적인데팬티는왜벗어")
        self.assertEqual(policy.clean_title("스피크 오브 더 데블"), "speakofthedevil")
        self.assertEqual(policy.clean_title("중력술사 아스께기맨"), "중력술사아스께끼맨")
        self.assertEqual(policy.clean_title("내가 키운 용사가 나에게 집착한다"), "내가키운용사가나한테집착한다")
        self.assertEqual(policy.clean_title("[연재]망겜의 고인물로 살아남기 프롤로그"), "망겜의고인물로살아남기")
        self.assertEqual(policy.clean_title("플레이싱 (Plaything) : 어느 대공 각하의 장난감 1"), "plaything어느대공각하의장난감")
        self.assertEqual(policy.clean_title("[e북]보이즈 돈 크라이 (Boys don't cry) 1권"), "boysdontcry")
        self.assertEqual(policy.clean_title("SM클럽 -암캐가 된 여자들- 268화"), "sm클럽")
        self.assertEqual(policy.clean_title("고인물, 무림에 가다 1화"), "고인물무림에가다갈드창작지원금")
        self.assertEqual(policy.clean_title("메소드 로맨스 증보판"), "메소드로맨스")
        self.assertEqual(policy.clean_title("독식하는 재벌 3세"), "독식하는재벌세")
        self.assertEqual(policy.clean_title("1980 독식하는 재벌!"), "독식하는재벌")
        self.assertEqual(policy.clean_title("다른 작품의 이야기"), "다른작품의이야기")

    def test_policy_removes_commas_after_unicode_normalization(self) -> None:
        policy = CleaningPolicy()

        self.assertEqual(policy.clean_title("추억， 순간의 바람이 되어 1권"), "추억순간의바람이되어")
        self.assertEqual(policy.clean_title("추억, 순간의 바람이 되어"), "추억순간의바람이되어")
        self.assertEqual(
            policy.clean_title("[19금] 서큐버스와 인큐버스， 그들의 수행기 1권"),
            "서큐버스와인큐버스그들의수행기",
        )
        self.assertEqual(
            policy.clean_title("[19금]서큐버스와인큐버스,그들의수행기"),
            "서큐버스와인큐버스그들의수행기",
        )


if __name__ == "__main__":
    unittest.main()
