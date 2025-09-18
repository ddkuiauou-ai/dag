import math
import re
from collections import defaultdict

import dagster as dg
import pandas as pd

from .partitions import daily_exchange_category_partition
from .resources import PostgresResource

# Optimized mapping for _eng2kor, defined at module level for efficiency
_ENG_KOR_MAP = {
    "a": "에이",
    "A": "에이",
    "b": "비",
    "B": "비",
    "c": "씨",
    "C": "씨",
    "d": "디",
    "D": "디",
    "e": "이",
    "E": "이",
    "f": "에프",
    "F": "에프",
    "g": "지",
    "G": "지",
    "h": "에이치",
    "H": "에이치",
    "i": "아이",
    "I": "아이",
    "j": "제이",
    "J": "제이",
    "k": "케이",
    "K": "케이",
    "l": "엘",
    "L": "엘",
    "m": "엠",
    "M": "엠",
    "n": "엔",
    "N": "엔",
    "o": "오",
    "O": "오",
    "p": "피",
    "P": "피",
    "q": "큐",
    "Q": "큐",
    "r": "알",
    "R": "알",
    "s": "에스",
    "S": "에스",
    "t": "티",
    "T": "티",
    "u": "유",
    "U": "유",
    "v": "브이",
    "V": "브이",
    "w": "더블유",
    "W": "더블유",
    "x": "엑스",
    "X": "엑스",
    "y": "와이",
    "Y": "와이",
    "z": "제트",
    "Z": "제트",
    "&": "앤",
}


def _eng2kor(input_text):
    # Optimized implementation using the precomputed map and a list comprehension
    # The .get(char, char) pattern provides a direct lookup or defaults to the original character
    return "".join([_ENG_KOR_MAP.get(char, char) for char in input_text])


class Inko:
    영어 = "rRseEfaqQtTdwWczxvgASDFGZXCVkoiOjpuPhynbmlYUIHJKLBNM"
    한글 = "ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎㅁㄴㅇㄹㅎㅋㅌㅊㅍㅏㅐㅑㅒㅓㅔㅕㅖㅗㅛㅜㅠㅡㅣㅛㅕㅑㅗㅓㅏㅣㅠㅜㅡ"
    초성 = "ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ"
    중성 = "ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ"
    종성 = "ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ"
    첫모음 = 28
    가 = 44032
    힣 = 55203
    ㄱ = 12593
    ㅣ = 12643

    영어index = (lambda en: {en[i]: i for i in range(len(en))})(영어)

    한글index = (lambda kr: {i: w for i, w in enumerate(kr)})(한글)

    connectableConsonant = {
        "ㄱㅅ": "ㄳ",
        "ㄴㅈ": "ㄵ",
        "ㄴㅎ": "ㄶ",
        "ㄹㄱ": "ㄺ",
        "ㄹㅁ": "ㄻ",
        "ㄹㅂ": "ㄼ",
        "ㄹㅅ": "ㄽ",
        "ㄹㅌ": "ㄾ",
        "ㄹㅍ": "ㄿ",
        "ㄹㅎ": "ㅀ",
        "ㅂㅅ": "ㅄ",
    }

    connectableVowel = {
        "ㅗㅏ": "ㅘ",
        "ㅗㅐ": "ㅙ",
        "ㅗㅣ": "ㅚ",
        "ㅜㅓ": "ㅝ",
        "ㅜㅔ": "ㅞ",
        "ㅜㅣ": "ㅟ",
        "ㅡㅣ": "ㅢ",
    }

    isVowel = (
        lambda self, e: [k for k, v in self.한글index.items() if v == e][0]
        >= self.첫모음
    )

    한글생성 = lambda self, *args: chr(
        44032 + args[0][0] * 588 + args[0][1] * 28 + args[0][2] + 1
    )

    def indexOf(self, val, _list):
        try:
            return _list.index(val)
        except ValueError:
            return -1

    def __init__(self, **_option):
        option = {} if _option is None else _option
        self._allowDoubleConsonant = (
            option["allowDoubleConsonant"]
            if "allowDoubleConsonant" in option
            else False
        )

    def config(self, **_option):
        option = {} if _option is None else _option
        self._allowDoubleConsonant = (
            option["allowDoubleConsonant"]
            if "allowDoubleConsonant" in option
            else self._allowDoubleConsonant
        )

    def en2ko(self, _input, **_option):
        option = {} if _option is None else _option
        self._allowDoubleConsonant = (
            option["allowDoubleConsonant"]
            if "allowDoubleConsonant" in option
            else self._allowDoubleConsonant
        )
        stateLength = [0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5]
        transitions = [
            [1, 1, 2, 2],  # 0, EMPTY
            [3, 1, 4, 4],  # 1, 자
            [1, 1, 5, 2],  # 2, 모
            [3, 1, 4, -1],  # 3, 자자
            [6, 1, 7, 2],  # 4, 자모
            [1, 1, 2, 2],  # 5, 모모
            [9, 1, 4, 4],  # 6, 자모자
            [9, 1, 2, 2],  # 7, 자모모
            [1, 1, 4, 4],  # 8, 자모자자
            [10, 1, 4, 4],  # 9, 자모모자
            [1, 1, 4, 4],  # 10, 자모모자자
        ]

        last = lambda _list: _list[len(_list) - 1]

        def combine(arr):
            group = []
            for i in range(len(arr)):
                h = self.한글[arr[i]]
                if i == 0 or self.isVowel(last(group)[0]) != self.isVowel(h):
                    group.append([])
                last(group).append(h)

            def connect(e):
                w = "".join(e)
                if w in self.connectableConsonant:
                    return self.connectableConsonant[w]
                elif w in self.connectableVowel:
                    return self.connectableVowel[w]
                else:
                    return w

            group = [connect(e) for e in group]

            if len(group) == 1:
                return group[0]

            charSet = [self.초성, self.중성, self.종성]
            try:
                code = [self.indexOf(w, charSet[i]) for i, w in enumerate(group)]
            except IndexError:
                pass

            if len(code) < 3:
                code.append(-1)

            return self.한글생성(code)

        def _():
            length = len(_input)
            last = -1
            result = []
            state = 0
            _vars = {"tmp": []}

            def flush():
                if len(_vars["tmp"]) > 0:
                    result.append(combine(_vars["tmp"]))
                _vars["tmp"] = []

            for i in range(length):
                char = _input[i]
                if char not in self.영어index:
                    state = 0
                    flush()
                    result.append(char)
                else:
                    curr = self.영어index[char]

                    def transition():
                        c = (self.한글[last] if last > -1 else "") + self.한글[curr]
                        lastIsVowel = self.isVowel(self.한글[last])
                        currIsVowel = self.isVowel(self.한글[curr])
                        if not currIsVowel:
                            if lastIsVowel:
                                return (
                                    0
                                    if self.indexOf("ㄸㅃㅉ", self.한글[curr]) == -1
                                    else 1
                                )
                            if state == 1 and not self._allowDoubleConsonant:
                                return 1
                            return 0 if c in self.connectableConsonant else 1
                        elif lastIsVowel:
                            return 2 if c in self.connectableVowel else 3

                        return 2

                    _transition = transition()
                    nextState = transitions[state][_transition]
                    _vars["tmp"].append(curr)
                    diff = len(_vars["tmp"]) - stateLength[nextState]
                    if diff > 0:
                        result.append(combine(_vars["tmp"][0:diff]))
                        _vars["tmp"] = _vars["tmp"][diff:]
                    state = nextState
                    last = curr

            flush()

            return "".join(result)

        return _()

    def ko2en(self, _input):
        result = ""
        if _input == "" or _input is None:
            return result
        _분리 = [-1, -1, -1, -1, -1]

        for i in range(len(_input)):
            _한글 = _input[i]
            _코드 = ord(_한글)
            # 가 ~ 힣 사이에 있는 한글이라면
            if (_코드 >= self.가 and _코드 <= self.힣) or (
                _코드 >= self.ㄱ and _코드 <= self.ㅣ
            ):
                _분리 = self.한글분리(_한글)
            # 한글이 아니라면
            else:
                result += _한글
                _분리 = [-1, -1, -1, -1, -1]

            for j in range(len(_분리)):
                if _분리[j] != -1:
                    result += self.영어[_분리[j]]

        return result

    def 한글분리(self, _한글):
        코드 = ord(_한글)

        if 코드 >= self.가 and 코드 <= self.힣:
            초 = math.floor((코드 - self.가) / 588)
            중 = math.floor((코드 - self.가 - 초 * 588) / 28)
            종 = 코드 - self.가 - 초 * 588 - 중 * 28 - 1
            중1, 중2, 종1, 종2 = 중, -1, 종, -1

            if 중 == self.indexOf("ㅘ", self.중성):
                중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf("ㅏ", self.한글)
            elif 중 == self.indexOf("ㅙ", self.중성):
                중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf("ㅐ", self.한글)
            elif 중 == self.indexOf("ㅚ", self.중성):
                중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf("ㅣ", self.한글)
            elif 중 == self.indexOf("ㅝ", self.중성):
                중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf("ㅓ", self.한글)
            elif 중 == self.indexOf("ㅞ", self.중성):
                중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf("ㅔ", self.한글)
            elif 중 == self.indexOf("ㅟ", self.중성):
                중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf("ㅣ", self.한글)
            elif 중 == self.indexOf("ㅢ", self.중성):
                중1, 중2 = self.indexOf("ㅡ", self.한글), self.indexOf("ㅣ", self.한글)

            if 종 == self.indexOf("ㄳ", self.종성):
                종1, 종2 = self.indexOf("ㄱ", self.한글), self.indexOf("ㅅ", self.한글)
            elif 종 == self.indexOf("ㄵ", self.종성):
                종1, 종2 = self.indexOf("ㄴ", self.한글), self.indexOf("ㅈ", self.한글)
            elif 종 == self.indexOf("ㄶ", self.종성):
                종1, 종2 = self.indexOf("ㄴ", self.한글), self.indexOf("ㅎ", self.한글)
            elif 종 == self.indexOf("ㄺ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㄱ", self.한글)
            elif 종 == self.indexOf("ㄻ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅁ", self.한글)
            elif 종 == self.indexOf("ㄼ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅂ", self.한글)
            elif 종 == self.indexOf("ㄽ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅅ", self.한글)
            elif 종 == self.indexOf("ㄾ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅌ", self.한글)
            elif 종 == self.indexOf("ㄿ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅍ", self.한글)
            elif 종 == self.indexOf("ㅀ", self.종성):
                종1, 종2 = self.indexOf("ㄹ", self.한글), self.indexOf("ㅎ", self.한글)
            elif 종 == self.indexOf("ㅄ", self.종성):
                종1, 종2 = self.indexOf("ㅂ", self.한글), self.indexOf("ㅅ", self.한글)

            if 중2 == -1 and 중 != -1:
                중1 = self.indexOf(self.중성[중], self.한글)
            if 종2 == -1 and 종 != -1:
                종1 = self.indexOf(self.종성[종], self.한글)

            return [초, 중1, 중2, 종1, 종2]
        elif 코드 >= self.ㄱ and 코드 <= self.ㅣ:
            if self.indexOf(_한글, self.초성) > -1:
                초 = self.indexOf(_한글, self.한글)
                return [초, -1, -1, -1, -1]
            elif self.indexOf(_한글, self.중성) > -1:
                중 = self.indexOf(_한글, self.중성)
                중1, 중2 = 중, -1
                if 중 == self.indexOf("ㅘ", self.중성):
                    중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf(
                        "ㅏ", self.한글
                    )
                elif 중 == self.indexOf("ㅙ", self.중성):
                    중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf(
                        "ㅐ", self.한글
                    )
                elif 중 == self.indexOf("ㅚ", self.중성):
                    중1, 중2 = self.indexOf("ㅗ", self.한글), self.indexOf(
                        "ㅣ", self.한글
                    )
                elif 중 == self.indexOf("ㅝ", self.중성):
                    중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf(
                        "ㅓ", self.한글
                    )
                elif 중 == self.indexOf("ㅞ", self.중성):
                    중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf(
                        "ㅔ", self.한글
                    )
                elif 중 == self.indexOf("ㅟ", self.중성):
                    중1, 중2 = self.indexOf("ㅜ", self.한글), self.indexOf(
                        "ㅣ", self.한글
                    )
                elif 중 == self.indexOf("ㅢ", self.중성):
                    중1, 중2 = self.indexOf("ㅡ", self.한글), self.indexOf(
                        "ㅣ", self.한글
                    )

                if 중2 == -1:
                    중1 = self.indexOf(self.중성[중], self.한글)

                return [-1, 중1, 중2, -1, -1]

        return [-1, -1, -1, -1, -1]

    def is한글(self, char):
        if len(char) > 1:
            raise Exception("한 글자가 아닙니다.")
        return re.match("[ㄱ-ㅎ|ㅏ-ㅣ|가-힣]", char) is not None


def _kor2eng_for_keyboard(input_text):
    myInko = Inko()
    return myInko.ko2en(input_text)


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=["cd_upsert_company_by_security"],
)
def cd_sync_display_name_search_name(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    회사의 kor_name이 기존 display_name에 없는 경우,
    "display_name"과 "search_name" 테이블에 신규 레코드를 생성하여 업데이트합니다.
    """
    partition_info = context.partition_key.keys_by_dimension
    date = partition_info["date"].replace("-", "")
    exchange = partition_info["exchange"]

    # 제외할 security type (상수)
    EXCLUDES = ("스팩", "리츠", "펀드")
    excludes_tuple = "(" + ",".join(f"'{e}'" for e in EXCLUDES) + ")"

    update_records = []  # 업데이트 내역 기록

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. 해당 exchange에서 조건에 맞는 회사를 조회
            company_query = f"""
                SELECT c.company_id, c.name, c.kor_name
                FROM company c
                WHERE EXISTS (
                    SELECT 1 FROM security s
                    WHERE s.company_id = c.company_id
                      AND s.exchange = %s
                      AND (s.type IS NULL OR s.type NOT IN {excludes_tuple})
                )
            """
            cur.execute(company_query, (exchange,))
            companies = cur.fetchall()  # 각 tuple: (companyId, name, korName)

            # 2. 모든 회사의 DisplayName을 한 번에 조회하여 딕셔너리로 그룹화
            if companies:
                company_ids = tuple(company[0] for company in companies)
                display_query = """
                    SELECT company_id, value
                    FROM display_name
                    WHERE company_id IN %s
                    ORDER BY "order"
                """
                cur.execute(display_query, (company_ids,))
                display_rows = cur.fetchall()
                display_dict = {}
                for cid, value in display_rows:
                    display_dict.setdefault(cid, []).append(value)
            else:
                display_dict = {}

            # 3. 각 회사별로 display_name과 search_name 업데이트 여부 확인 및 삽입
            for company in companies:
                company_id, company_name, company_kor_name = company
                current_display_names = display_dict.get(company_id, [])

                # 회사의 kor_name이 등록되어 있지 않으면 업데이트 진행
                if not current_display_names or (
                    company_kor_name not in current_display_names
                ):
                    update_type = "New" if not current_display_names else "Add"
                    new_order = len(current_display_names)

                    # display_name 신규 등록
                    insert_display = """
                        INSERT INTO display_name (company_id, company_name, value, "order")
                        VALUES (%s, %s, %s, %s)
                    """
                    cur.execute(
                        insert_display,
                        (company_id, company_name, company_kor_name, new_order),
                    )

                    # search_name 신규 등록
                    insert_search = """
                        INSERT INTO search_name (company_id, company_name, value, "order")
                        VALUES (%s, %s, %s, %s)
                    """
                    cur.execute(
                        insert_search,
                        (company_id, company_name, company_kor_name, new_order),
                    )

                    update_records.append(
                        [date, exchange, company_id, company_name, update_type]
                    )
            conn.commit()

    # DataFrame 생성 및 메타데이터 구성
    if update_records:
        odf = pd.DataFrame(
            update_records, columns=["date", "exchange", "company_id", "name", "type"]
        )
        new_count = sum(1 for record in update_records if record[4] == "New")
        add_count = sum(1 for record in update_records if record[4] == "Add")
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "# of New": dg.MetadataValue.int(new_count),
            "# of Add": dg.MetadataValue.int(add_count),
            "Preview": dg.MetadataValue.md(odf.to_markdown()),
        }
    else:
        odf = pd.DataFrame(columns=["date", "exchange", "company_id", "name", "type"])
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text(
                "모든 회사의 display_name와 search_name가 이미 업데이트 되었습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=[cd_sync_display_name_search_name],
)
def cd_post_eng2kor_search_name(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """
    한국 시장만을 대상으로, 회사 name에 영어가 포함된 경우
    _eng2kor 함수를 통해 한글로 번역한 후 search_name 테이블에 신규 레코드를 추가합니다.
    """

    # partition key에서 날짜와 exchange 값 추출 (날짜는 하이픈 제거)
    partition_info = context.partition_key.keys_by_dimension
    date = partition_info["date"].replace("-", "")
    exchange = partition_info["exchange"]

    update_records = []  # 업데이트 내역 기록

    # 정규표현식 컴파일 (영어 포함 여부 판별)
    eng_regex = re.compile(r"[a-zA-Z]")

    # INSERT 쿼리문 미리 상수화
    insert_query = """
        INSERT INTO search_name (company_id, company_name, value, "order")
        VALUES (%s, %s, %s, %s)
    """

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. 조건에 맞는 회사를 조회: 대한민국 소재이며, 해당 exchange에 속한 Security가 존재하는 회사
            company_query = """
                SELECT c.company_id, c.name
                FROM company c
                WHERE c.country = '대한민국'
                  AND EXISTS (
                      SELECT 1 FROM security s
                      WHERE s.company_id = c.company_id
                        AND s.exchange = %s
                  )
            """
            cur.execute(company_query, (exchange,))
            companies = cur.fetchall()  # 각 row: (companyId, name)

            # 2. 각 회사별 기존 search_name을 조회하여 딕셔너리 구성
            search_dict = defaultdict(list)
            if companies:
                company_ids = tuple(company[0] for company in companies)
                search_query = """
                    SELECT company_id, value
                    FROM search_name
                    WHERE company_id IN %s
                    ORDER BY "order"
                """
                cur.execute(search_query, (company_ids,))
                for cid, value in cur.fetchall():
                    search_dict[cid].append(value)

            # 3. 각 회사의 name에 영어가 포함되어 있으면 번역 후, 기존 search_name에 없을 경우 신규 삽입
            for company_id, company_name in companies:
                if eng_regex.search(company_name):  # 영어 포함 여부 확인
                    translated_name = _eng2kor(
                        company_name
                    )  # 영어 → 한글 번역 (함수 구현 필요)
                    existing_names = search_dict.get(company_id, [])
                    if translated_name not in existing_names:
                        order = len(existing_names)
                        cur.execute(
                            insert_query,
                            (company_id, company_name, translated_name, order),
                        )
                        update_records.append(
                            [date, exchange, company_id, company_name, translated_name]
                        )
            conn.commit()

    # 업데이트 내역에 따라 metadata 구성 및 MaterializeResult 반환
    if update_records:
        odf = pd.DataFrame(
            update_records,
            columns=["date", "exchange", "company_id", "name", "search_name"],
        )
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "# of New": dg.MetadataValue.int(len(update_records)),
            "Preview": dg.MetadataValue.md(odf.to_markdown()),
        }
    else:
        odf = pd.DataFrame(
            columns=["date", "exchange", "company_id", "name", "search_name"]
        )
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text(
                "모든 회사의 search_name이 이미 업데이트 되었습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    partitions_def=daily_exchange_category_partition,
    group_name="CD",
    kinds={"postgres"},
    tags={"data_tier": "silver", "domain": "finance", "source": "pykrx"},
    deps=[cd_post_eng2kor_search_name],
)
def cd_post_kor2eng_search_name_for_keyboard(
    context: dg.AssetExecutionContext, cd_postgres: PostgresResource
) -> dg.MaterializeResult:
    """한국 시장만을 대상으로 한글 이름의 경우 search_name을 전부 영문 키보드로 변경해서 추가"""

    from collections import defaultdict

    # partition key에서 날짜와 exchange 값 추출 (날짜는 하이픈 제거)
    partition_info = context.partition_key.keys_by_dimension
    date = partition_info["date"].replace("-", "")
    exchange = partition_info["exchange"]

    update_records = []  # metadata 미리보기용 업데이트 내역 기록
    new_inserts = []  # bulk insert를 위한 신규 레코드 목록

    # 한글 포함 여부 판별 정규표현식 컴파일
    hangul_regex = re.compile(r"[가-힣]")

    # SQL 쿼리 상수화
    company_query = """
        SELECT c.company_id, c.name
        FROM company c
        WHERE c.country = '대한민국'
          AND EXISTS (
              SELECT 1 FROM security s
              WHERE s.company_id = c.company_id
                AND s.exchange = %s
          )
    """
    search_query = """
        SELECT company_id, value
        FROM search_name
        WHERE company_id IN %s
        ORDER BY "order"
    """
    insert_query = """
        INSERT INTO search_name (company_id, company_name, value, "order")
        VALUES (%s, %s, %s, %s)
    """

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. 조건에 맞는 회사 조회
            cur.execute(company_query, (exchange,))
            companies = cur.fetchall()  # 각 row: (company_id, name)

            # 2. 각 회사별 기존 SearchName 조회 및 딕셔너리 구성
            search_dict = defaultdict(list)
            if companies:
                company_ids = tuple(company[0] for company in companies)
                cur.execute(search_query, (company_ids,))
                for cid, value in cur.fetchall():
                    search_dict[cid].append(value)

            # 3. 각 회사의 name에 한글이 포함된 경우, 영문 키보드로 변환하여 SearchName에 추가
            for company_id, company_name in companies:
                if hangul_regex.search(company_name):
                    eng_name = _kor2eng_for_keyboard(company_name)
                    existing_names = search_dict.get(company_id, [])
                    if eng_name not in existing_names:
                        order = len(existing_names)
                        new_inserts.append((company_id, company_name, eng_name, order))
                        update_records.append([date, exchange, company_id, company_name, eng_name])

            # 신규 레코드가 있으면 정렬 후 배치 처리로 bulk insert 실행
            if new_inserts:
                # Sort by company_id for consistent lock ordering
                new_inserts.sort(key=lambda x: x[0])  # company_id is at index 0
                # Process in smaller batches
                batch_size = 500  # Reduced batch size to minimize lock contention
                for i in range(0, len(new_inserts), batch_size):
                    batch = new_inserts[i:i + batch_size]
                    cur.executemany(insert_query, batch)
                    conn.commit()  # Commit each batch separately

    # 업데이트 내역에 따라 DataFrame 및 metadata 구성
    if update_records:
        odf = pd.DataFrame(
            update_records,
            columns=["date", "exchange", "company_id", "name", "search_name"],
        )
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "# of New": dg.MetadataValue.int(len(update_records)),
            "Preview": dg.MetadataValue.md(odf.to_markdown()),
        }
    else:
        odf = pd.DataFrame(
            columns=["date", "exchange", "company_id", "name", "search_name"]
        )
        metadata = {
            "Date": dg.MetadataValue.text(date),
            "Exchange": dg.MetadataValue.text(exchange),
            "Result": dg.MetadataValue.text(
                "모든 회사의 searchName이 이미 업데이트 되었습니다"
            ),
        }

    return dg.MaterializeResult(metadata=metadata)
