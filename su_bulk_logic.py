"""
수불(입고/사용/재고) 집계 및 Plotly 차트용 데이터 생성.
엑셀 시트명·컬럼명은 키워드 매칭으로 유연하게 인식한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from typing import Any, Literal

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re

GRADE_HIGH = frozenset({"ADS01"})
GRADE_LOW = frozenset({"ADS02", "ADS04"})
GRADE_MID = frozenset({"ADS15"})


def _coerce_biz_date_to_python_date(s: pd.Series) -> pd.Series:
    """datetime64 / Timestamp / date 혼재 → python `date`로 통일 (필터 비교 오류 방지)."""
    dt = pd.to_datetime(s, errors="coerce").dt.normalize()
    return dt.dt.date


def normalize_supply_label(text: str | float | None) -> str:
    """엑셀 공급구분 문자열 → 유통 / MOU / 회수 / 기타. 회수·MOU를 유통보다 먼저 판별."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return "기타"
    s = str(text).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return "기타"
    su = s.upper().replace(" ", "")
    if "회수" in s:
        return "회수"
    if "MOU" in su or "M.O.U" in su:
        return "MOU"
    if "유통" in s:
        return "유통"
    return "기타"


def _norm_supplier_name(x: Any) -> str:
    return (
        str(x)
        .strip()
        .replace(" ", "")
        .replace("\u00a0", "")
        .replace("(주)", "")
        .replace("㈜", "")
        .upper()
    )


def build_supply_maps(ref_supply: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    """
    기준정보에서
    - 공급사명 -> 공급사구분
    - ITEM -> 공급사구분
    매핑을 동시에 만든다.
    """
    by_supplier: dict[str, str] = {}
    by_item: dict[str, str] = {}
    if ref_supply.empty:
        return by_supplier, by_item

    for _, row in ref_supply.iterrows():
        sup_raw = row.get("공급사명", "")
        if pd.notna(sup_raw) and str(sup_raw).strip():
            by_supplier[_norm_supplier_name(sup_raw)] = str(row.get("공급사구분", "")).strip()

        item_raw = row.get("ITEM", "")
        if pd.notna(item_raw) and str(item_raw).strip():
            by_item[normalize_item(item_raw)] = str(row.get("공급사구분", "")).strip()

    return by_supplier, by_item


def classify_supply_for_row(
    row: pd.Series,
    by_supplier: dict[str, str],
    by_item: dict[str, str],
) -> str:
    """입고 행: 입고 시트 공급구분 → 공급사명 매칭 → ITEM 매칭."""
    if "supply_type" in row.index:
        v = row["supply_type"]
        if pd.notna(v) and str(v).strip() and str(v).strip().lower() != "nan":
            c = normalize_supply_label(str(v))
            if c != "기타":
                return c

    if "supplier_name" in row.index:
        sn = row["supplier_name"]
        if pd.notna(sn) and str(sn).strip() and str(sn).strip().lower() != "nan":
            raw = by_supplier.get(_norm_supplier_name(sn), "")
            if raw:
                return normalize_supply_label(raw)

    item = normalize_item(row.get("ITEM", ""))
    raw2 = by_item.get(item, "")
    if raw2:
        return normalize_supply_label(raw2)
    return "기타"


def _norm_col(c: str) -> str:
    return str(c).strip().replace(" ", "").lower()


def find_col(df: pd.DataFrame, keywords: list[str], fallback: str | None = None) -> str | None:
    if df is None or df.empty:
        return fallback
    cols = {_norm_col(c): c for c in df.columns}
    for kw in keywords:
        k = _norm_col(kw)
        for nc, orig in cols.items():
            if k in nc or nc in k:
                return orig
    return fallback


def coerce_number_series(s: pd.Series) -> pd.Series:
    """콤마·공백·문자 혼입 숫자 열 → float."""
    if s.dtype.kind in "iuf":
        return pd.to_numeric(s, errors="coerce").fillna(0.0)
    t = (
        s.astype(str)
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.replace("，", "", regex=False)
        .str.replace("\u00a0", "", regex=False)
    )
    t = t.replace({"nan": "", "None": "", "<NA>": ""})
    return pd.to_numeric(t, errors="coerce").fillna(0.0)


def find_qty_col(df: pd.DataFrame) -> str | None:
    best_c = None
    best_s = -1
    for c in df.columns:
        nc = _norm_col(str(c))
        score = 0
        if "입하량" in nc or ("입하" in nc and "량" in nc):
            score += 5
        if "net" in nc:
            score += 3
        if nc.endswith("량") and "입하" not in nc and "사용" not in nc:
            score += 1
        if "사용" in nc and "량" in nc:
            score += 4
        if "수량" in nc or "물량" in nc or nc == "qty":
            score += 2
        if score > best_s:
            best_s = score
            best_c = c
    return best_c if best_s > 0 else None


def find_site_col(df: pd.DataFrame) -> str | None:
    c = find_col(df, ["사소명", "사소", "소속", "공장", "plant", "site"], None)
    if c:
        return c
    for col in df.columns:
        nc = _norm_col(str(col))
        if "사소" in nc or nc in ("포항소", "광양소"):
            return col
    return None


def pick_sheet(xl: pd.ExcelFile, candidates: list[str]) -> str | None:
    names = {str(n).strip(): n for n in xl.sheet_names}
    for c in candidates:
        if c in names:
            return names[c]
        for k, v in names.items():
            if c.lower() in k.lower():
                return v
    return None


def business_date_from_ts(ts: pd.Timestamp) -> date | None:
    """엑셀 날짜만(자정)은 달력 그대로. 시각이 있는 경우만 07시 미만 → 전일 입하일."""
    if pd.isna(ts):
        return None
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0 and ts.microsecond == 0:
        return ts.normalize().date()
    if ts.hour < 7:
        return (ts.normalize() - pd.Timedelta(days=1)).date()
    return ts.normalize().date()


def apply_business_date(series_dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series_dt, errors="coerce")
    return dt.map(lambda x: business_date_from_ts(x) if pd.notna(x) else pd.NaT)


def normalize_item(s: Any) -> str:
    return str(s).strip().upper() if pd.notna(s) else ""


@dataclass
class LoadedData:
    usage: pd.DataFrame
    receipt: pd.DataFrame
    expected_usage: pd.DataFrame
    expected_receipt: pd.DataFrame
    opening: pd.DataFrame
    ref_supply: pd.DataFrame
    ref_site: pd.DataFrame


def load_workbooks(data_bytes: bytes, ref_bytes: bytes) -> LoadedData:
    dbuf = BytesIO(data_bytes)
    rbuf = BytesIO(ref_bytes)

    def read_d(sheet_cands: list[str]) -> pd.DataFrame:
        dbuf.seek(0)
        dxl = pd.ExcelFile(dbuf)
        sn = pick_sheet(dxl, sheet_cands)
        if not sn:
            return pd.DataFrame()
        dbuf.seek(0)
        return pd.read_excel(dbuf, sheet_name=sn)

    def read_r(sheet_cands: list[str]) -> pd.DataFrame:
        rbuf.seek(0)
        rxl = pd.ExcelFile(rbuf)
        sn = pick_sheet(rxl, sheet_cands)
        if not sn:
            return pd.DataFrame()
        rbuf.seek(0)
        return pd.read_excel(rbuf, sheet_name=sn)

    return LoadedData(
        usage=read_d(["사용"]),
        receipt=read_d(["입고"]),
        expected_usage=read_d(["예상사용량"]),
        expected_receipt=read_d(["예상입고량"]),
        opening=read_d(["기초재고"]),
        ref_supply=read_r(["ITEM공급사", "공급사기준", "공급사", "기준정보"]),
        ref_site=read_r(["사소지역", "사소기준", "지역기준"]),
    )


def standardize_movement_df(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["사소", "ITEM", "biz_date", "qty", "kind"])
    c_site = find_site_col(df)
    c_item = find_col(
        df,
        ["구매item", "구매 item", "item", "품목", "품목코드", "구매품목", "material"],
        None,
    )
    c_date = find_col(
        df,
        ["입하일자", "일자", "날짜", "date", "기준일", "입하일", "발생일"],
        None,
    )
    c_qty = find_qty_col(df) or find_col(
        df, ["입하량", "입하량(net)", "net", "수량", "물량", "중량"], None
    )
    if not all([c_site, c_item, c_date, c_qty]):
        return pd.DataFrame(columns=["사소", "ITEM", "biz_date", "qty", "kind"])
    c_sup = None
    c_supplier = None
    if kind == "receipt":
        c_sup = find_col(
            df,
            [
                "공급사구분",
                "공급유형",
                "유통구분",
                "구매구분",
                "supply",
                "공급",
                "입고구분",
            ],
            None,
        )
        c_supplier = find_col(
            df,
            ["공급사명", "공급사", "업체", "거래처", "vendor", "supplier"],
            None,
        )
    use_cols = [c_site, c_item, c_date, c_qty]
    if c_sup:
        use_cols.append(c_sup)
    if c_supplier:
        use_cols.append(c_supplier)
    tmp = df[use_cols].copy()
    ncols = ["사소", "ITEM", "dt_raw", "qty"]
    if c_sup:
        ncols.append("supply_type")
    if c_supplier:
        ncols.append("supplier_name")
    tmp.columns = ncols
    tmp["qty"] = coerce_number_series(tmp["qty"])
    tmp["ITEM"] = tmp["ITEM"].map(normalize_item)
    if c_sup:
        tmp["supply_type"] = tmp["supply_type"].map(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip().lower() != "nan" else ""
        )
    if c_supplier:
        tmp["supplier_name"] = tmp["supplier_name"].map(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip().lower() != "nan" else ""
        )
    tmp["biz_date"] = apply_business_date(tmp["dt_raw"])
    tmp["kind"] = kind
    base_cols = ["사소", "ITEM", "biz_date", "qty", "kind"]
    if c_sup:
        base_cols.append("supply_type")
    if c_supplier:
        base_cols.append("supplier_name")
    out = tmp[base_cols].dropna(subset=["biz_date"])
    out["biz_date"] = pd.to_datetime(out["biz_date"]).dt.date
    return out


def standardize_expected_df(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["사소", "ITEM", "biz_date", "qty", "kind"])
    def _parse_calendar_date(v: Any) -> date | None:
        """
        예상 탭은 07시 기준일 룰을 적용하지 않고, '표에 적힌 날짜' 그대로(달력 날짜) 사용한다.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, date) and not isinstance(v, pd.Timestamp):
            return v
        ts = pd.to_datetime(v, errors="coerce")
        if pd.notna(ts):
            return ts.date()
        s = str(v).strip()
        m = re.search(r"(?:(\d{4})[./-])?\s*(\d{1,2})[./-]\s*(\d{1,2})", s)
        if m:
            yy = int(m.group(1)) if m.group(1) else df_date_year_hint
            mm = int(m.group(2))
            dd = int(m.group(3))
            try:
                return date(yy, mm, dd)
            except ValueError:
                return None
        return None

    def _date_cols_from_header(frame: pd.DataFrame) -> list[Any]:
        out: list[Any] = []
        for c in frame.columns:
            if isinstance(c, (pd.Timestamp,)):
                out.append(c)
                continue
            if _parse_calendar_date(c) is not None:
                out.append(c)
        return out

    def _item_cols(frame: pd.DataFrame) -> list[str]:
        cols: list[str] = []
        for c in frame.columns:
            s = normalize_item(c)
            if s.startswith("ADS") and len(s) >= 5:
                cols.append(str(c))
        return cols

    c_site = find_site_col(df)
    c_item = find_col(df, ["item", "구매", "품목", "구매item", "구매 item", "구매품목"], None)
    c_date = find_col(df, ["일자", "날짜", "입하", "기준일", "예상일", "입하일자"], None)
    c_qty = (
        find_col(df, ["예상사용량", "예상입고량", "예상사용", "예상입고", "예상수량", "예상"], None)
        or find_col(df, ["수량", "물량", "사용량", "입고량", "입하량"], None)
        or find_qty_col(df)
    )

    # 헤더/셀에 연도가 없는 '3/19' 같은 표기 대비 (조회월 기준)
    df_date_year_hint = pd.Timestamp.today().year
    try:
        df_date_year_hint = int(pd.to_datetime(df[c_date].dropna().iloc[0], errors="coerce").year) if c_date and not df.empty and len(df[c_date].dropna()) else df_date_year_hint
    except Exception:
        pass

    # Case A) Long format: site + item + date + qty
    if all([c_site, c_item, c_date, c_qty]):
        t = df[[c_site, c_item, c_date, c_qty]].copy()
        t.columns = ["사소", "ITEM", "dt_raw", "qty"]
        t["qty"] = coerce_number_series(t["qty"])
        t["ITEM"] = t["ITEM"].map(normalize_item)
        t["biz_date"] = t["dt_raw"].map(_parse_calendar_date)
        t["kind"] = kind
        t["biz_date"] = _coerce_biz_date_to_python_date(t["biz_date"])
        return t[["사소", "ITEM", "biz_date", "qty", "kind"]].dropna(subset=["biz_date"])

    # Case B) Wide format (dates are columns): site(+item) + 여러 날짜열
    date_cols = _date_cols_from_header(df)
    if c_site and date_cols:
        id_vars = [c_site]
        if c_item:
            id_vars.append(c_item)
        t = df.copy()
        t = t[[*id_vars, *date_cols]].copy()
        t = t.melt(id_vars=id_vars, var_name="dt_raw", value_name="qty")
        t["qty"] = coerce_number_series(t["qty"])
        if c_item:
            t = t.rename(columns={c_site: "사소", c_item: "ITEM"})
            t["ITEM"] = t["ITEM"].map(normalize_item)
        else:
            t = t.rename(columns={c_site: "사소"})
            t["ITEM"] = ""
        t["biz_date"] = t["dt_raw"].map(_parse_calendar_date)
        t["kind"] = kind
        t["biz_date"] = _coerce_biz_date_to_python_date(t["biz_date"])
        return t[["사소", "ITEM", "biz_date", "qty", "kind"]].dropna(subset=["biz_date"])

    # Case C) Date column exists, but ITEM is spread across ADS columns
    item_cols = _item_cols(df)
    if c_site and c_date and item_cols:
        t = df[[c_site, c_date, *item_cols]].copy()
        t = t.melt(id_vars=[c_site, c_date], var_name="ITEM", value_name="qty")
        t = t.rename(columns={c_site: "사소", c_date: "dt_raw"})
        t["qty"] = coerce_number_series(t["qty"])
        t["ITEM"] = t["ITEM"].map(normalize_item)
        t["biz_date"] = t["dt_raw"].map(_parse_calendar_date)
        t["kind"] = kind
        t["biz_date"] = _coerce_biz_date_to_python_date(t["biz_date"])
        return t[["사소", "ITEM", "biz_date", "qty", "kind"]].dropna(subset=["biz_date"])

    return pd.DataFrame(columns=["사소", "ITEM", "biz_date", "qty", "kind"])


def standardize_opening_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["사소", "ITEM", "qty"])
    c_site = find_site_col(df)
    c_item = find_col(df, ["구매item", "구매 item", "item", "품목", "품목코드"], None)
    c_qty = (
        find_col(df, ["기초재고", "재고량", "현재고", "재고", "기초", "수량"], None)
        or find_qty_col(df)
    )
    if not all([c_site, c_item, c_qty]):
        return pd.DataFrame(columns=["사소", "ITEM", "qty"])
    t = df[[c_site, c_item, c_qty]].copy()
    t.columns = ["사소", "ITEM", "qty"]
    t["qty"] = coerce_number_series(t["qty"])
    t["ITEM"] = t["ITEM"].map(normalize_item)
    return t


def standardize_ref_supply(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["공급사명", "ITEM", "공급사구분"])
    c_item = find_col(
        df,
        ["item", "품목", "구매item", "구매 item", "품목코드", "구매품목"],
        df.columns[0],
    )
    c_supplier = find_col(
        df,
        ["공급사명", "공급사", "업체명", "거래처", "vendor", "supplier"],
        None,
    )
    c_sup = find_col(
        df,
        [
            "공급사구분",
            "공급유형",
            "유통구분",
            "구매구분",
            "공급구분",
            "구분",
            "유형",
            "공급",
        ],
        None,
    )
    if not c_sup:
        return pd.DataFrame(columns=["공급사명", "ITEM", "공급사구분"])

    cols = []
    if c_supplier:
        cols.append(c_supplier)
    if c_item:
        cols.append(c_item)
    cols.append(c_sup)

    t = df[cols].copy()
    new_cols = []
    if c_supplier:
        new_cols.append("공급사명")
    if c_item:
        new_cols.append("ITEM")
    new_cols.append("공급사구분")
    t.columns = new_cols

    if "공급사명" not in t.columns:
        t["공급사명"] = ""
    if "ITEM" not in t.columns:
        t["ITEM"] = ""
    t["공급사명"] = t["공급사명"].astype(str).str.strip()
    t["ITEM"] = t["ITEM"].map(normalize_item)
    t["공급사구분"] = t["공급사구분"].astype(str).str.strip()
    return t[["공급사명", "ITEM", "공급사구분"]]


def standardize_ref_site(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["사소", "지역"])
    c_site = find_col(df, ["사소", "소", "코드"], df.columns[0])
    c_reg = find_col(df, ["지역", "권역"], None)
    if not c_site or not c_reg:
        return pd.DataFrame(columns=["사소", "지역"])
    t = df[[c_site, c_reg]].copy()
    t.columns = ["사소", "지역"]
    t["사소"] = t["사소"].astype(str).str.strip()
    t["지역"] = t["지역"].astype(str).str.strip()
    return t


def _norm_site_key(x: str) -> str:
    return str(x).strip().replace(" ", "").replace("\u00a0", "")


def infer_region(site: str, ref_site: pd.DataFrame) -> str:
    """사소 값(포항소·광양소 등) → 포항/광양. 기준정보는 부분 일치로 매핑."""
    s = str(site).strip()
    sn = _norm_site_key(s)
    if not ref_site.empty and "사소" in ref_site.columns and "지역" in ref_site.columns:
        for _, row in ref_site.iterrows():
            key = _norm_site_key(str(row["사소"]))
            if not key:
                continue
            if sn == key or key in sn or sn in key:
                reg = str(row["지역"])
                if "광양" in reg:
                    return "광양"
                if "포항" in reg:
                    return "포항"
    if "광양소" in s or "광양" in s or "GWANGYANG" in s.upper():
        return "광양"
    if "포항소" in s or "포항" in s or "POHANG" in s.upper():
        return "포항"
    return "기타"


def attach_region(df: pd.DataFrame, ref_site: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(지역=pd.Series(dtype=object))
    out = df.copy()
    out["지역"] = out["사소"].astype(str).map(lambda x: infer_region(x, ref_site))
    return out


def month_bounds(q: date) -> tuple[date, date]:
    start = date(q.year, q.month, 1)
    if q.month == 12:
        end = date(q.year, 12, 31)
    else:
        end = date(q.year, q.month + 1, 1) - timedelta(days=1)
    return start, end


def month_week_block_index(d: date) -> int:
    """해당 월 기준 주차: 1~7일=1주차, 8~14=2주차, 15~21=3주차, 22~28=4주차, 29~말=5주차."""
    return (d.day - 1) // 7 + 1


def block_date_bounds(year: int, month: int, block: int, month_end: date) -> tuple[date, date]:
    d_first = 7 * (block - 1) + 1
    d_last = min(7 * block, month_end.day)
    return date(year, month, d_first), date(year, month, d_last)


def build_x_axis_periods(query_date: date, month_start: date, month_end: date) -> list[dict[str, Any]]:
    """
    조회월의 **모든 일자**마다 1개 포인트(3월이면 31개).
    Plotly에서 X가 겹치면 막대/선이 합쳐지므로 `x_key`는 날짜 ISO 문자열로 **항상 유일**하게 둔다.
    틱 라벨은 조회일만 `3/18` 형식으로 표시.
    (주차 브라켓/주차명 표시는 차트에서 shape+annotation으로 별도 렌더링)
    """
    ms, me = month_start, month_end
    y, m = ms.year, ms.month
    periods: list[dict[str, Any]] = []
    d = ms
    while d <= me:
        b = month_week_block_index(d)
        d0, d1 = block_date_bounds(y, m, b, me)
        x_key = d.isoformat()

        # 기준일(조회일) 날짜 표시는 그래프 상단 점선 라벨로만 표시
        x_tick = ""

        periods.append(
            {
                "type": "day",
                "start": d,
                "end": d,
                "x_key": x_key,
                "x_label": x_tick,
                "sort_key": (0, d.toordinal()),
            }
        )
        d += timedelta(days=1)

    return periods


def filter_item_grade(df: pd.DataFrame, grade: Literal["all", "high", "low"]) -> pd.DataFrame:
    if df.empty:
        return df
    if grade == "all":
        return df
    if grade == "high":
        return df[df["ITEM"].isin(GRADE_HIGH)]
    if grade == "low":
        return df[df["ITEM"].isin(GRADE_LOW)]
    return df


def sum_qty(df: pd.DataFrame, d0: date, d1: date, region: str | None) -> float:
    if df.empty:
        return 0.0
    m = (df["biz_date"] >= d0) & (df["biz_date"] <= d1)
    if region:
        m &= df["지역"] == region
    sub = df.loc[m, "qty"]
    return float(sub.sum()) if len(sub) else 0.0


def opening_sum_fixed(
    opening: pd.DataFrame,
    ref_site: pd.DataFrame,
    region: str | None,
    grade: Literal["all", "high", "low"],
) -> float:
    if opening.empty:
        return 0.0
    o = opening.copy()
    o["ITEM"] = o["ITEM"].map(normalize_item)
    o["지역"] = o["사소"].astype(str).map(lambda s: infer_region(s, ref_site))
    if region:
        o = o[o["지역"] == region]
    o = filter_item_grade(o, grade)
    return float(o["qty"].sum())


def prepare_frames(
    raw: LoadedData,
    query_date: date,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    date,
    date,
    list[dict[str, Any]],
]:
    month_start, month_end = month_bounds(query_date)
    periods = build_x_axis_periods(query_date, month_start, month_end)

    usage = standardize_movement_df(raw.usage, "usage")
    receipt = standardize_movement_df(raw.receipt, "receipt")
    exp_u = standardize_expected_df(raw.expected_usage, "exp_use")
    exp_r = standardize_expected_df(raw.expected_receipt, "exp_rec")
    if not exp_u.empty:
        exp_u = exp_u.copy()
        exp_u["biz_date"] = _coerce_biz_date_to_python_date(exp_u["biz_date"])
    if not exp_r.empty:
        exp_r = exp_r.copy()
        exp_r["biz_date"] = _coerce_biz_date_to_python_date(exp_r["biz_date"])
    opening = standardize_opening_df(raw.opening)
    ref_site = standardize_ref_site(raw.ref_site)

    u_a = usage[(usage["biz_date"] >= month_start) & (usage["biz_date"] <= month_end)].copy()
    r_a = receipt[(receipt["biz_date"] >= month_start) & (receipt["biz_date"] <= month_end)].copy()
    u_a = attach_region(u_a, ref_site)
    r_a = attach_region(r_a, ref_site)

    eu = exp_u[(exp_u["biz_date"] >= month_start) & (exp_u["biz_date"] <= month_end)].copy()
    er = exp_r[(exp_r["biz_date"] >= month_start) & (exp_r["biz_date"] <= month_end)].copy()
    eu = attach_region(eu, ref_site)
    er = attach_region(er, ref_site)

    u_f = eu[eu["biz_date"] > query_date].copy()
    r_f = er[er["biz_date"] > query_date].copy()

    u_a_act = u_a[u_a["biz_date"] <= query_date].copy()
    r_a_act = r_a[r_a["biz_date"] <= query_date].copy()

    return (
        u_a_act,
        r_a_act,
        u_f,
        r_f,
        opening,
        ref_site,
        standardize_ref_supply(raw.ref_supply),
        month_start,
        month_end,
        periods,
    )


def daily_recv_use_inv(
    month_start: date,
    month_end: date,
    query_date: date,
    ra: pd.DataFrame,
    ua: pd.DataFrame,
    rf: pd.DataFrame,
    uf: pd.DataFrame,
    region: str | None,
    grade: Literal["all", "high", "low"],
    opening: pd.DataFrame,
    ref_site: pd.DataFrame,
) -> tuple[dict[date, float], dict[date, float], dict[date, float], dict[date, bool]]:
    """일자별 입고·사용·기말재고. 조회일 이하는 실적, 초과는 예상 탭."""
    base = opening_sum_fixed(opening, ref_site, region, grade)
    ra = filter_item_grade(ra, grade)
    ua = filter_item_grade(ua, grade)
    rf = filter_item_grade(rf, grade)
    uf = filter_item_grade(uf, grade)

    recv_d: dict[date, float] = {}
    use_d: dict[date, float] = {}
    fc_flag: dict[date, bool] = {}
    d = month_start
    while d <= month_end:
        if d <= query_date:
            recv_d[d] = sum_qty(ra, d, d, region)
            use_d[d] = sum_qty(ua, d, d, region)
            fc_flag[d] = False
        else:
            recv_d[d] = sum_qty(rf, d, d, region)
            use_d[d] = sum_qty(uf, d, d, region)
            fc_flag[d] = True
        d += timedelta(days=1)

    cum_r = 0.0
    cum_u = 0.0
    inv_d: dict[date, float] = {}
    d = month_start
    while d <= month_end:
        cum_r += recv_d[d]
        cum_u += use_d[d]
        inv_d[d] = base + cum_r - cum_u
        d += timedelta(days=1)

    return recv_d, use_d, inv_d, fc_flag


def bucket_metrics(
    u_act: pd.DataFrame,
    r_act: pd.DataFrame,
    u_fc: pd.DataFrame,
    r_fc: pd.DataFrame,
    opening: pd.DataFrame,
    ref_site: pd.DataFrame,
    periods: list[dict[str, Any]],
    region: str | None,
    grade: Literal["all", "high", "low"],
    query_date: date,
    month_start: date,
    month_end: date,
) -> tuple[
    list[str],
    list[str],
    list[float],
    list[float],
    list[float],
    list[float],
    list[float],
    list[float],
    list[bool],
]:
    recv_d, use_d, inv_d, fc_flag = daily_recv_use_inv(
        month_start, month_end, query_date, r_act, u_act, r_fc, u_fc, region, grade, opening, ref_site
    )

    x_keys: list[str] = []
    x_ticktext: list[str] = []
    recv_act: list[float] = []
    use_act: list[float] = []
    inv_act: list[float] = []
    recv_fc: list[float] = []
    use_fc: list[float] = []
    inv_fc: list[float] = []
    is_fc: list[bool] = []

    for p in periods:
        d0, d1 = p["start"], p["end"]
        xk = str(p.get("x_key", ""))
        label = str(p.get("x_label", ""))
        days = []
        cur = d0
        while cur <= d1:
            if month_start <= cur <= month_end:
                days.append(cur)
            cur += timedelta(days=1)

        r_a = r_f = u_a = u_f = 0.0
        bucket_fc = False

        if not days:
            x_keys.append(xk or label)
            x_ticktext.append(label)
            recv_act.append(0.0)
            recv_fc.append(0.0)
            use_act.append(0.0)
            use_fc.append(0.0)
            inv_act.append(0.0)
            inv_fc.append(0.0)
            is_fc.append(False)
            continue

        for dd in days:
            if fc_flag[dd]:
                bucket_fc = True
                r_f += recv_d[dd]
                u_f += use_d[dd]
            else:
                r_a += recv_d[dd]
                u_a += use_d[dd]

        last = days[-1]
        inv_total = inv_d[last]
        if bucket_fc:
            recv_act.append(r_a)
            recv_fc.append(r_f)
            use_act.append(u_a)
            use_fc.append(u_f)
            inv_act.append(0.0)
            inv_fc.append(inv_total)
        else:
            recv_act.append(r_a)
            recv_fc.append(0.0)
            use_act.append(u_a)
            use_fc.append(0.0)
            inv_act.append(inv_total)
            inv_fc.append(0.0)
        is_fc.append(bucket_fc)
        x_keys.append(xk or d0.isoformat())
        x_ticktext.append(label)

    return x_keys, x_ticktext, recv_act, use_act, inv_act, recv_fc, use_fc, inv_fc, is_fc


def make_mixed_chart(
    title: str,
    x_keys: list[str],
    x_ticktext: list[str],
    recv_act: list[float],
    use_act: list[float],
    inv_act: list[float],
    recv_fc: list[float],
    use_fc: list[float],
    inv_fc: list[float],
    is_fc: list[bool],
    highlight_x_key: str | None = None,
) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    n = len(x_keys)
    r_tot = [float(recv_act[i] + recv_fc[i]) for i in range(n)]
    u_tot = [float(use_act[i] + use_fc[i]) for i in range(n)]
    r_fc_line = [r_tot[i] if is_fc[i] else None for i in range(n)]
    u_fc_line = [u_tot[i] if is_fc[i] else None for i in range(n)]
    hover_dates: list[str] = []
    for x in x_keys:
        try:
            dlab = pd.to_datetime(x, errors="coerce")
            hover_dates.append(f"{int(dlab.month)}/{int(dlab.day)}" if pd.notna(dlab) else str(x))
        except Exception:
            hover_dates.append(str(x))

    fig.add_trace(
        go.Scatter(
            x=x_keys,
            y=r_tot,
            name="입고량",
            mode="lines+markers",
            line=dict(color="#2E7D32", width=2.5),
            connectgaps=True,
            hoverinfo="skip",
            hovertemplate=None,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=x_keys,
            y=u_tot,
            name="사용량",
            mode="lines+markers",
            line=dict(color="#C62828", width=2.5),
            connectgaps=True,
            hoverinfo="skip",
            hovertemplate=None,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=x_keys,
            y=r_fc_line,
            name="입고(예상·점선)",
            mode="lines+markers",
            line=dict(color="#43A047", width=2, dash="dash"),
            connectgaps=True,
            hoverinfo="skip",
            hovertemplate=None,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=x_keys,
            y=u_fc_line,
            name="사용(예상·점선)",
            mode="lines+markers",
            line=dict(color="#E53935", width=2, dash="dash"),
            connectgaps=True,
            hoverinfo="skip",
            hovertemplate=None,
        ),
        secondary_y=False,
    )

    inv_y = [a + f for a, f in zip(inv_act, inv_fc)]
    inv_colors = ["#5C6BC0" if fc else "#1565C0" for fc in is_fc]
    bar_customdata = list(zip(hover_dates, r_tot, u_tot, inv_y))
    fig.add_trace(
        go.Bar(
            x=x_keys,
            y=inv_y,
            name="재고(예상 구간은 밝은 색)",
            marker_color=inv_colors,
            opacity=0.75,
            customdata=bar_customdata,
            hovertemplate=(
                "날짜 %{customdata[0]}<br>"
                "입고량 %{customdata[1]:.0f}<br>"
                "출고량 %{customdata[2]:.0f}<br>"
                "재고량 %{customdata[3]:.0f}"
                "<extra></extra>"
            ),
        ),
        secondary_y=True,
    )

    # 주차 구간을 한눈에 보이도록: 하단 브라켓 선 + 가운데 주차 라벨
    # (x축 ticktext에 주차를 직접 넣으면 31개 포인트가 합쳐져 보이거나 hover가 어색해질 수 있어 분리 렌더링)
    try:
        dates = [pd.to_datetime(x, errors="coerce").date() for x in x_keys]
        if dates and all(isinstance(d, date) for d in dates):
            ms = min(dates)
            me = max(dates)
            y, m = ms.year, ms.month
            max_b = month_week_block_index(me)
            for b in range(1, max_b + 1):
                d0, d1 = block_date_bounds(y, m, b, me)
                if d1 < ms or d0 > me:
                    continue
                # clamp to available range
                d0c = max(d0, ms)
                d1c = min(d1, me)
                x0 = d0c.isoformat()
                x1 = d1c.isoformat()

                # 연한 배경 (교차 음영)
                fig.add_vrect(
                    x0=x0,
                    x1=x1,
                    fillcolor=("rgba(0,0,0,0.03)" if b % 2 == 1 else "rgba(0,0,0,0.00)"),
                    opacity=1.0,
                    layer="below",
                    line_width=0,
                )

                # 브라켓(선으로 이어짐): 그래프 바로 하단
                yb = -0.045
                yt = -0.01
                fig.add_shape(
                    type="line",
                    xref="x",
                    yref="paper",
                    x0=x0,
                    x1=x1,
                    y0=yb,
                    y1=yb,
                    line=dict(color="rgba(0,0,0,0.55)", width=1),
                )
                fig.add_shape(
                    type="line",
                    xref="x",
                    yref="paper",
                    x0=x0,
                    x1=x0,
                    y0=yb,
                    y1=yt,
                    line=dict(color="rgba(0,0,0,0.55)", width=1),
                )
                fig.add_shape(
                    type="line",
                    xref="x",
                    yref="paper",
                    x0=x1,
                    x1=x1,
                    y0=yb,
                    y1=yt,
                    line=dict(color="rgba(0,0,0,0.55)", width=1),
                )

                mid = date.fromordinal((d0c.toordinal() + d1c.toordinal()) // 2).isoformat()
                fig.add_annotation(
                    x=mid,
                    xref="x",
                    y=-0.16,
                    yref="paper",
                    text=f"{m}-{b}주차<br>({m}/{d0.day}~{m}/{d1.day})",
                    showarrow=False,
                    xanchor="center",
                    align="center",
                    font=dict(color="rgba(0,0,0,0.75)", size=11),
                )
    except Exception:
        # 브라켓은 시각적 장식이므로 실패해도 본 차트는 렌더링
        pass

    if highlight_x_key and highlight_x_key in set(x_keys):
        # category x-axis에서는 add_vline의 annotation 계산이 문자열 평균을 내다 오류가 날 수 있어,
        # shape + annotation을 분리해서 추가한다.
        try:
            dlab = pd.to_datetime(highlight_x_key, errors="coerce")
            label = f"{int(dlab.month)}/{int(dlab.day)}" if pd.notna(dlab) else str(highlight_x_key)
        except Exception:
            label = str(highlight_x_key)

        fig.add_shape(
            type="line",
            xref="x",
            yref="paper",
            x0=highlight_x_key,
            x1=highlight_x_key,
            y0=0,
            y1=1,
            line=dict(color="#D32F2F", width=2, dash="dash"),
        )
        fig.add_annotation(
            x=highlight_x_key,
            xref="x",
            y=1.02,
            yref="paper",
            text=f"<b>{label}</b>",
            showarrow=False,
            font=dict(color="#D32F2F", size=14),
            bgcolor="rgba(255,255,255,0.65)",
        )

    title_text = title or ""
    fig.update_layout(
        title=dict(text=title_text, x=0.01, y=0.99, xanchor="left", yanchor="top"),
        xaxis=dict(
            title="",
            type="category",
            tickmode="array",
            tickvals=x_keys,
            ticktext=x_ticktext,
            tickangle=0,
            categoryorder="array",
            categoryarray=x_keys,
            automargin=False,
        ),
        barmode="overlay",
        hovermode="closest",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,  # paper coords: place legend above plotting area
            xanchor="right",
            x=1,
        ),
        height=520,
        margin=dict(t=(120 if title_text else 90), b=140),
    )
    fig.update_yaxes(
        title_text="",
        secondary_y=False,
        rangemode="tozero",
        tickformat=",.0f",
    )
    # 보조축(재고): 변화를 보기 쉽도록
    # - 최솟값이 그래프 높이의 약 50% 위치
    # - 최댓값이 그래프 높이의 약 90% 위치
    # 선형 축에서 p=(v-low)/(high-low) 이므로
    # low = vmin - 1.25*(vmax-vmin), high = vmax + 0.25*(vmax-vmin)
    inv_min = float(min(inv_y)) if inv_y else 0.0
    inv_max = float(max(inv_y)) if inv_y else 0.0
    d = inv_max - inv_min
    if d > 0:
        inv_low = inv_min - 1.25 * d
        inv_high = inv_max + 0.25 * d
    else:
        # 변화가 거의 없을 때는 주변 여유만 확보
        pad = max(abs(inv_max) * 0.1, 1.0)
        inv_low = inv_min - pad
        inv_high = inv_max + pad

    fig.update_yaxes(
        title_text="",
        secondary_y=True,
        showgrid=False,
        range=[inv_low, inv_high],
        tickformat=",.0f",
    )

    # 기본축/보조축 타이틀을 "축이 시작하는 아래" 쪽으로 내리기 위해
    # 기존 yaxis title 대신 annotation을 사용한다.
    # - 왼쪽 라벨: 3-1주차의 왼쪽(월 첫 날짜 x축 위치)에 붙임
    # - 오른쪽 라벨: 3-5주차의 오른쪽(월 마지막 날짜 x축 위치)에 붙임
    if x_keys:
        # y를 paper 좌표로 조정: x축(날짜) tick 라벨과 같은 높이 근처
        label_y = -0.05

        fig.add_annotation(
            text="물량(톤)",
            # x축 범주(domain) 안쪽에 고정: 바깥으로 밀지 않아서
            # annotation 때문에 plot 폭이 줄어드는 현상을 최소화한다.
            xref="x domain",
            yref="paper",
            x=0.015,
            y=label_y,
            # x=0(왼쪽 경계) 기준으로 텍스트가 더 왼쪽으로 뻗게
            xanchor="right",
            yanchor="top",
            textangle=0,
            showarrow=False,
            font=dict(color="rgba(0,0,0,0.75)", size=12),
        )
        fig.add_annotation(
            text="재고(톤)",
            xref="x domain",
            yref="paper",
            x=1.0,
            y=label_y,
            # x=1(오른쪽 경계) 기준으로 텍스트가 더 오른쪽으로 뻗게
            xanchor="left",
            yanchor="top",
            textangle=0,
            showarrow=False,
            font=dict(color="rgba(0,0,0,0.75)", size=12),
        )

    return fig


def site_bar_figure_v2(
    query_date: date,
    site_label: str,
    receipt_day: pd.DataFrame,
    usage_day: pd.DataFrame,
    ref_supply: pd.DataFrame,
) -> go.Figure:
    # 상단 Plotly title은 제거 (Streamlit에서 `##### 포항`/`##### 광양`로 구분)
    title = ""
    categories = ["총계", "ADS01", "ADS02", "ADS04", "ADS15"]

    by_supplier, by_item = build_supply_maps(ref_supply)

    items_sets: dict[str, frozenset[str] | None] = {
        "총계": None,
        "ADS01": GRADE_HIGH,
        "ADS02": frozenset({"ADS02"}),
        "ADS04": frozenset({"ADS04"}),
        "ADS15": GRADE_MID,
    }

    r_day = receipt_day[receipt_day["biz_date"] == query_date].copy()
    u_day = usage_day[usage_day["biz_date"] == query_date].copy()

    ut_flow = {c: 0.0 for c in categories}
    ut_mou = {c: 0.0 for c in categories}
    ut_recv = {c: 0.0 for c in categories}
    ut_misc = {c: 0.0 for c in categories}
    usage_by = {c: 0.0 for c in categories}

    for cat in categories:
        it = items_sets[cat]
        sub_r = r_day if it is None else r_day[r_day["ITEM"].isin(it)]
        sub_u = u_day if it is None else u_day[u_day["ITEM"].isin(it)]
        for _, row in sub_r.iterrows():
            b = classify_supply_for_row(row, by_supplier, by_item)
            q = float(row["qty"])
            if b == "유통":
                ut_flow[cat] += q
            elif b == "MOU":
                ut_mou[cat] += q
            elif b == "회수":
                ut_recv[cat] += q
            else:
                ut_misc[cat] += q
        usage_by[cat] += float(sub_u["qty"].sum()) if not sub_u.empty else 0.0

    def _fmt_ton(x: float) -> str:
        return f"{int(round(float(x))):,}톤"

    def _receipt_label(flow: float, mou: float, recv: float, misc: float) -> str:
        parts: list[str] = []
        if flow:
            parts.append(f"유통 {int(round(float(flow))):,}")
        if mou:
            parts.append(f"MOU {int(round(float(mou))):,}")
        if recv:
            parts.append(f"회수 {int(round(float(recv))):,}")
        if misc:
            parts.append(f"기타 {int(round(float(misc))):,}")
        total = flow + mou + recv + misc
        if not parts or total == 0:
            return ""
        # 종(세로)로 표시 + 톤은 총에만 표기
        return "<br>".join([f"<b>총 {_fmt_ton(total)}</b>", *parts])

    fig = go.Figure()
    x_base = list(range(len(categories)))
    offset_in = -0.22
    offset_out = 0.22
    bar_w = 0.38

    fig.add_trace(
        go.Bar(
            x=[i + offset_in for i in x_base],
            y=[ut_flow[c] for c in categories],
            width=bar_w,
            name="입고-유통",
            marker_color="#2E7D32",
        )
    )
    fig.add_trace(
        go.Bar(
            x=[i + offset_in for i in x_base],
            y=[ut_mou[c] for c in categories],
            width=bar_w,
            name="입고-MOU",
            marker_color="#F9A825",
        )
    )
    fig.add_trace(
        go.Bar(
            x=[i + offset_in for i in x_base],
            y=[ut_recv[c] for c in categories],
            width=bar_w,
            name="입고-회수",
            marker_color="#6A1B9A",
        )
    )
    fig.add_trace(
        go.Bar(
            x=[i + offset_in for i in x_base],
            y=[ut_misc[c] for c in categories],
            width=bar_w,
            name="입고-기타(미분류)",
            marker_color="#90A4AE",
        )
    )
    fig.add_trace(
        go.Bar(
            x=[i + offset_out for i in x_base],
            y=[usage_by[c] for c in categories],
            width=bar_w,
            name="출고(사용)",
            marker_color="#C62828",
        )
    )

    # 입고 스택 상단에 합계/구성 텍스트 표시 (가독성: 2줄 + 배경 박스)
    receipt_totals = [ut_flow[c] + ut_mou[c] + ut_recv[c] + ut_misc[c] for c in categories]
    receipt_labels = [
        _receipt_label(ut_flow[c], ut_mou[c], ut_recv[c], ut_misc[c]) for c in categories
    ]
    for i, cat in enumerate(categories):
        if not receipt_labels[i]:
            continue
        x_pos = i + offset_in
        y_pos = receipt_totals[i]
        fig.add_annotation(
            x=x_pos,
            y=y_pos,
            xref="x",
            yref="y",
            text=receipt_labels[i],
            showarrow=False,
            xanchor="center",
            yanchor="bottom",
            yshift=6,
            align="center",
            font=dict(size=11, color="rgba(0,0,0,0.85)"),
            bgcolor="rgba(255,255,255,0.82)",
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1,
        )

    # 출고(사용) 막대 상단 숫자 표시
    def _fmt_num(x: float) -> str:
        return f"{int(round(float(x))):,}"

    for i, cat in enumerate(categories):
        out = float(usage_by[cat])
        if out == 0:
            continue
        x_pos = i + offset_out
        y_pos = out
        fig.add_annotation(
            x=x_pos,
            y=y_pos,
            xref="x",
            yref="y",
            text=f"<b>출고 { _fmt_num(out) }</b>",
            showarrow=False,
            xanchor="center",
            yanchor="bottom",
            yshift=6,
            align="center",
            font=dict(size=11, color="rgba(0,0,0,0.85)"),
            bgcolor="rgba(255,255,255,0.82)",
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1,
        )

    fig.update_layout(
        title=dict(text=title),
        barmode="stack",
        xaxis=dict(tickmode="array", tickvals=x_base, ticktext=categories),
        yaxis=dict(title="입하량(톤)", tickformat=",.0f"),
        height=560,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        # title이 없으므로 상단 여백을 크게 줄여 차트를 더 크게/빨리 배치
        margin=dict(t=40, b=80),
    )
    return fig


def export_workbook_bytes(
    summary_rows: list[dict[str, Any]],
    daily_detail: pd.DataFrame,
) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(summary_rows).to_excel(w, sheet_name="요약", index=False)
        if not daily_detail.empty:
            daily_detail.to_excel(w, sheet_name="일자별집계", index=False)
    return buf.getvalue()


def validate_minimum(raw: LoadedData) -> list[str]:
    errs: list[str] = []
    if raw.usage.empty:
        errs.append("더미데이터에 `사용` 시트를 찾을 수 없거나 비어 있습니다.")
    if raw.receipt.empty:
        errs.append("더미데이터에 `입고` 시트를 찾을 수 없거나 비어 있습니다.")
    if raw.opening.empty:
        errs.append("더미데이터에 `기초재고` 시트가 필요합니다.")
    return errs


def full_movement_month(
    raw: LoadedData,
    ref_site: pd.DataFrame,
    month_start: date,
    month_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """하단 사소별 막대용: 해당월 전체 입고·사용(실적)."""
    u = standardize_movement_df(raw.usage, "usage")
    r = standardize_movement_df(raw.receipt, "receipt")
    if not u.empty:
        u = u[(u["biz_date"] >= month_start) & (u["biz_date"] <= month_end)]
        u = attach_region(u, ref_site)
    if not r.empty:
        r = r[(r["biz_date"] >= month_start) & (r["biz_date"] <= month_end)]
        r = attach_region(r, ref_site)
    return u, r
