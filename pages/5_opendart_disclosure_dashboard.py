import datetime as dt
import time
from typing import Optional
import xml.etree.ElementTree as ET

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


DART_API_BASE = "https://opendart.fss.or.kr/api"


def _to_yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _get_api_key_from_secrets() -> str:
    # st.secrets에 키가 없을 수도 있어 예외를 흡수합니다.
    try:
        return str(st.secrets.get("DART_API_KEY", "")).strip()
    except Exception:
        return ""


def _dart_get_json(api_key: str, endpoint: str, params: dict) -> dict:
    # OpenDART는 crtfc_key 파라미터를 요구합니다.
    url = f"{DART_API_BASE}/{endpoint}"
    query = dict(params)
    query["crtfc_key"] = api_key
    headers = {"User-Agent": "Mozilla/5.0 (Streamlit OpenDART Dashboard)"}
    resp = requests.get(url, params=query, timeout=30, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    # OpenDART 에러는 status/message로도 전달되는 경우가 있습니다.
    if str(data.get("status")) != "000":
        raise RuntimeError(f"OpenDART API error: {data.get('message')}")
    return data


def _dart_get_xml(api_key: str, endpoint: str, params: Optional[dict] = None) -> ET.Element:
    url = f"{DART_API_BASE}/{endpoint}"
    query = dict(params or {})
    query["crtfc_key"] = api_key
    headers = {"User-Agent": "Mozilla/5.0 (Streamlit OpenDART Dashboard)"}
    resp = requests.get(url, params=query, timeout=60, headers=headers)
    resp.raise_for_status()
    txt = resp.text or ""
    stripped = txt.lstrip()
    if not stripped:
        raise RuntimeError(f"OpenDART returned empty body for {endpoint}.")

    # OpenDART가 에러를 JSON으로 주는 경우가 있어(키/권한/제한), 그 경우를 먼저 처리합니다.
    if stripped.startswith("{"):
        try:
            data = resp.json()
            raise RuntimeError(f"OpenDART returned JSON (not XML) for {endpoint}: {data.get('message')}")
        except Exception:
            preview = stripped[:200].replace("\n", " ")
            raise RuntimeError(f"OpenDART returned JSON-like body for {endpoint}. Preview: {preview}")

    # XML이 아니면(HTML 차단 페이지 등) 파싱하지 않고 미리보기로 원인을 노출합니다.
    if not stripped.startswith("<"):
        preview = stripped[:200].replace("\n", " ")
        raise RuntimeError(f"OpenDART returned non-XML body for {endpoint}. Preview: {preview}")

    try:
        return ET.fromstring(txt)
    except ET.ParseError:
        preview = stripped[:300].replace("\n", " ")
        raise RuntimeError(
            f"OpenDART XML parse failed for {endpoint}. " f"Response preview: {preview}"
        )


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def fetch_corp_code_list(api_key: str) -> pd.DataFrame:
    """
    corpCode.xml은 전체 상장/등록 회사를 반환합니다.
    대시보드에서 회사명 -> corp_code 매핑을 위해 사용합니다.
    """
    root = _dart_get_xml(api_key=api_key, endpoint="corpCode.xml")

    rows: list[dict] = []
    # 응답 예시: <list>...</list>가 반복
    for node in root.findall(".//list"):
        rows.append(
            {
                "corp_code": (node.findtext("corp_code") or "").strip(),
                "corp_name": (node.findtext("corp_name") or "").strip(),
                "stock_code": (node.findtext("stock_code") or "").strip(),
                "modify_date": (node.findtext("modify_date") or "").strip(),
            }
        )

    df = pd.DataFrame(rows)
    # corp_code 기준 중복 제거
    if not df.empty and "corp_code" in df.columns:
        df = df.drop_duplicates(subset=["corp_code"]).reset_index(drop=True)
    return df


@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_disclosures(
    api_key: str,
    corp_code: str,
    bgn_de: str,
    end_de: str,
    pblntf_ty: str = "",
    pblntf_detail_ty: str = "",
    last_reprt_at: str = "N",
    page_count: int = 100,
    max_pages: int = 2,
) -> tuple[list[dict], pd.DataFrame]:
    page_no = 1
    items: list[dict] = []
    total_page: Optional[int] = None

    while page_no <= max_pages:
        params = {
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count,
            "last_reprt_at": last_reprt_at,
        }
        if pblntf_ty.strip():
            params["pblntf_ty"] = pblntf_ty.strip()
        if pblntf_detail_ty.strip():
            params["pblntf_detail_ty"] = pblntf_detail_ty.strip()

        data = _dart_get_json(api_key=api_key, endpoint="list.json", params=params)
        if total_page is None:
            total_page = int(data.get("total_page") or 0)

        batch = data.get("list") or []
        items.extend(batch)

        # 모두 다 받았으면 종료
        if total_page is not None and page_no >= total_page:
            break

        page_no += 1
        time.sleep(0.25)  # 호출 간 짧은 템포 조절(레이트 제한 대비)

    df = pd.DataFrame(items)
    if not df.empty:
        df = df.reset_index(drop=True)
        df["_item_idx"] = range(len(df))
    return items, df


def main() -> None:
    st.set_page_config(
        page_title="OpenDART 공시 대시보드",
        page_icon="🧾",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
            .stApp { background: linear-gradient(180deg, #e3f2fd 0%, #f5f9ff 45%, #eef6ff 100%); }
            [data-testid="stSidebar"] { background-color: rgba(225, 245, 254, 0.65); }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("🧾 OpenDART 공시 데이터 대시보드")
    st.caption("OpenDART API로 `list.json` 공시 목록을 조회하고 요약/표/원본(JSON)을 확인합니다.")

    default_api_key = _get_api_key_from_secrets()
    with st.sidebar:
        st.header("연동 설정")
        api_key = st.text_input(
            "OpenDART API Key",
            type="password",
            value=default_api_key,
            help="DART 발급 키. 공유가 어려우면 st.secrets에 `DART_API_KEY`로 등록하세요.",
        )

        st.divider()
        st.header("회사 선택")
        corp_mode = st.radio(
            "선택 방식",
            options=["회사코드(corp_code)", "회사명(검색)"],
            index=1,
            horizontal=True,
        )

        corp_code = ""
        corp_name_query = ""

        corp_df: Optional[pd.DataFrame] = None
        if api_key.strip() and corp_mode == "회사명(검색)":
            with st.spinner("회사 목록 불러오는 중..."):
                corp_df = fetch_corp_code_list(api_key=api_key.strip())

            corp_name_query = st.text_input("회사명 검색(부분 입력)", value="", placeholder="예: 삼성전자, 현대차")
            if corp_name_query.strip():
                filtered = corp_df[
                    corp_df["corp_name"].str.contains(corp_name_query.strip(), case=False, na=False)
                ].copy()
            else:
                filtered = corp_df.head(200).copy()

            # 표시용 라벨 생성
            filtered = filtered.sort_values(["corp_name", "corp_code"], ascending=[True, True])
            filtered = filtered.head(200)
            labels = [f'{r["corp_name"]} ({r["corp_code"]})' for _, r in filtered.iterrows()]
            label_to_code = {lab: code for lab, code in zip(labels, filtered["corp_code"].tolist())}

            if labels:
                selected_label = st.selectbox("검색 결과", options=labels, index=0)
                corp_code = label_to_code[selected_label]
        elif corp_mode == "회사코드(corp_code)":
            corp_code = st.text_input("corp_code", value="", placeholder="예: 00126380")
        else:
            st.info("API Key를 입력하면 회사 목록을 불러올 수 있습니다.")

        st.divider()
        st.header("조회 조건")
        today = dt.date.today()
        default_end = today
        default_start = today - dt.timedelta(days=90)

        date_range = st.date_input(
            "기간",
            value=(default_start, default_end),
            help="OpenDART list.json은 시작/종료일(YYYYMMDD)로 조회합니다.",
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = default_start, default_end

        pblntf_ty = st.text_input("pblntf_ty (선택, 코드)", value="", placeholder="예: A (사업보고서) 등")
        pblntf_detail_ty = st.text_input("pblntf_detail_ty (선택)", value="", placeholder="상세 분류 코드")
        last_reprt_at = st.selectbox("last_reprt_at", options=["전체", "Y", "N"], index=2)

        if last_reprt_at == "전체":
            last_reprt_at_val = "N"
        else:
            last_reprt_at_val = last_reprt_at

        st.divider()
        st.header("성능/호출 제한")
        page_count = st.slider("page_count (1~100)", min_value=10, max_value=100, value=100, step=10)
        max_pages = st.slider("max_pages (호출 횟수 제한)", min_value=1, max_value=5, value=2, step=1)

        st.divider()
        run = st.button("조회", type="primary", disabled=not api_key.strip() or not corp_code.strip())

    if not api_key.strip():
        st.warning("OpenDART API Key를 입력해 주세요.")
        return
    if not corp_code.strip():
        st.warning("조회할 회사(`corp_code`)를 선택해 주세요.")
        return

    if not run:
        st.info("왼쪽에서 조건을 설정한 뒤 `조회`를 눌러 주세요.")
        return

    bgn_de = _to_yyyymmdd(start_date)
    end_de = _to_yyyymmdd(end_date)

    with st.spinner("OpenDART 공시 목록 조회 중..."):
        try:
            items, df = fetch_disclosures(
                api_key=api_key.strip(),
                corp_code=corp_code.strip(),
                bgn_de=bgn_de,
                end_de=end_de,
                pblntf_ty=pblntf_ty,
                pblntf_detail_ty=pblntf_detail_ty,
                last_reprt_at=last_reprt_at_val,
                page_count=page_count,
                max_pages=max_pages,
            )
        except Exception as e:
            st.error(str(e))
            return

    if df.empty:
        st.warning("해당 조건에서 조회된 공시가 없습니다.")
        return

    # 좌상단 요약 지표
    st.divider()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 공시 수", f"{len(df):,}")
    report_nm_col = "report_nm" if "report_nm" in df.columns else df.columns[0]
    c2.metric("서류 종류 수", f"{df[report_nm_col].nunique():,}")
    # bgn_de/end_de는 문자열이라도 정렬/최소최대 계산 가능
    if "bgn_de" in df.columns:
        c3.metric("기간 내 첫 공시일", str(df["bgn_de"].min()))
        c4.metric("기간 내 마지막 공시일", str(df["bgn_de"].max()))
    else:
        c3.metric("기간 시작", bgn_de)
        c4.metric("기간 종료", end_de)

    st.subheader("공시 종류 TOP")
    top_n = st.slider("Top N", min_value=5, max_value=20, value=10, step=1)
    group_col = report_nm_col
    top_df = (
        df.groupby(group_col, as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        .head(top_n)
    )
    fig = px.bar(
        top_df,
        x=group_col,
        y="count",
        text="count",
        color="count",
        color_continuous_scale="Blues",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(height=420, xaxis_title="공시명", yaxis_title="건수", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # 목록 테이블
    preferred_cols = [
        "rcept_no",
        "corp_name",
        "report_nm",
        "flr_nm",
        "bgn_de",
        "end_de",
        "rm",
        "fs_nm",
        "url",
        "dcmNo",
        "dcmn_url",
    ]
    display_cols = [c for c in preferred_cols if c in df.columns]
    if not display_cols:
        display_cols = list(df.columns[: min(10, len(df.columns))])

    # 테이블에 보기 좋게 정렬 (가능한 경우 bgn_de 내림차순)
    sort_cols = []
    if "bgn_de" in df.columns:
        sort_cols.append("bgn_de")
    if "end_de" in df.columns:
        sort_cols.append("end_de")
    if sort_cols:
        df_show = df.sort_values(sort_cols, ascending=[False] * len(sort_cols)).copy()
    else:
        df_show = df.copy()

    st.subheader("공시 목록")
    st.dataframe(
        df_show[display_cols],
        use_container_width=True,
        height=360,
    )

    # 선택 항목 JSON 표시
    st.subheader("선택 공시 원본(JSON)")
    # _item_idx를 기준으로 items[]와 매칭합니다.
    df_show_reset = df_show.reset_index(drop=True)
    if "_item_idx" in df_show_reset.columns:
        idxs = df_show_reset["_item_idx"].astype(int).tolist()
        options = []
        for item_idx in idxs[: min(500, len(idxs))]:
            row = df.iloc[item_idx]
            item_date = str(row.get("bgn_de", ""))
            item_name = str(row.get("report_nm", ""))
            options.append((item_idx, f"{item_date} | {item_name}"))

        option_map = {label: code for code, label in options}
        selected_label = st.selectbox("항목", options=[lab for _, lab in options])
        selected_item_idx = option_map[selected_label]

        selected = items[int(selected_item_idx)]
        with st.expander("원본 JSON 보기", expanded=True):
            st.json(selected)

    st.divider()
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="CSV 다운로드",
        data=csv_bytes,
        file_name=f"opendart_{corp_code}_{bgn_de}_{end_de}.csv",
        mime="text/csv",
    )

    st.caption("주의: 본 대시보드는 API 응답 기반의 조회 도구이며, 법적 효력/투자 판단의 근거로 사용하지 마세요.")


main()

