"""
yfinance 기반 글로벌(미국+한국) 주요 주식 대시보드
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf


st.markdown(
    """
    <style>
        .stApp { background: linear-gradient(180deg, #e3f2fd 0%, #f5f9ff 45%, #eef6ff 100%); }
        [data-testid="stSidebar"] { background-color: rgba(225, 245, 254, 0.65); }
    </style>
    """,
    unsafe_allow_html=True,
)


STOCKS = [
    {"name": "애플", "ticker": "AAPL"},
    {"name": "테슬라", "ticker": "TSLA"},
    {"name": "MS", "ticker": "MSFT"},
    {"name": "엔비디아", "ticker": "NVDA"},
    {"name": "SK하이닉스", "ticker": "000660.KS"},
    {"name": "삼성전자", "ticker": "005930.KS"},
]


@st.cache_data(ttl=60 * 60)
def load_ohlcv_single(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    # plotly가 datetime 인덱스를 안정적으로 처리하도록 timezone을 제거
    if hasattr(df.index, "tz") and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # yfinance가 멀티인덱스를 반환하는 경우가 있어 컬럼을 정규화
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def latest_metrics(df: pd.DataFrame) -> dict | None:
    if df is None or df.empty or "Close" not in df.columns:
        return None

    df = df.dropna(subset=["Close"]).copy()
    if len(df) < 2:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    last_close = float(last["Close"])
    prev_close = float(prev["Close"])
    delta = last_close - prev_close
    delta_pct = (delta / prev_close * 100.0) if prev_close != 0 else 0.0

    return {
        "Date": str(df.index[-1]),
        "Close": last_close,
        "Change": delta,
        "ChangePct": delta_pct,
        "High": float(df["High"].max()) if "High" in df.columns else float(df["Close"].max()),
        "Low": float(df["Low"].min()) if "Low" in df.columns else float(df["Close"].min()),
        "Volume": float(last["Volume"]) if "Volume" in df.columns else None,
    }


def apply_number_format(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    styler = df.style
    for col in df.columns:
        if col == "Date":
            continue
        if col == "Volume":
            styler = styler.format({col: "{:,.0f}"})
        else:
            styler = styler.format({col: "{:,.2f}"})
    return styler


def main() -> None:
    st.title("📈 애플/테슬라/MS/엔비디아 & 한국(대) 종목 대시보드")

    with st.sidebar:
        st.header("데이터 설정")

        name_to_ticker = {s["name"]: s["ticker"] for s in STOCKS}
        ticker_to_name = {s["ticker"]: s["name"] for s in STOCKS}

        selected_names = st.multiselect(
            "종목 선택",
            options=[s["name"] for s in STOCKS],
            default=[s["name"] for s in STOCKS],
        )
        selected_tickers = [name_to_ticker[n] for n in selected_names]

        period = st.selectbox(
            "조회 기간",
            options=["1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"],
            index=3,
        )
        interval = st.selectbox("데이터 간격", options=["1d", "1wk", "1mo"], index=0)

        st.divider()
        chart_type = st.radio("차트 종류", options=["라인(종가)", "캔들(OHLC)"], index=0)

        st.divider()
        st.header("이동평균(SMA)")
        show_sma = st.checkbox("SMA 표시", value=True)
        sma_fast = st.slider("SMA 단기", min_value=5, max_value=60, value=20, step=1)
        sma_slow = st.slider("SMA 장기", min_value=10, max_value=120, value=60, step=1)

    if not selected_tickers:
        st.warning("종목을 선택해 주세요.")
        return

    if chart_type == "캔들(OHLC)" and len(selected_tickers) != 1:
        st.error("캔들 차트는 1개 종목만 선택할 수 있습니다.")
        return

    # 종목별 데이터 불러오기
    series_by_ticker: dict[str, pd.DataFrame] = {}
    for t in selected_tickers:
        df = load_ohlcv_single(t, period, interval)
        if df.empty:
            st.warning(f"{t} 데이터가 없습니다.")
            continue
        series_by_ticker[t] = df

    if not series_by_ticker:
        st.warning("선택한 종목 중 데이터를 불러오지 못했습니다.")
        return

    # 최근 지표 테이블
    rows = []
    for s in STOCKS:
        if s["ticker"] not in series_by_ticker:
            continue
        m = latest_metrics(series_by_ticker[s["ticker"]])
        if not m:
            continue
        m["종목"] = s["name"]
        m["티커"] = s["ticker"]
        rows.append(m)

    metrics_df = pd.DataFrame(rows)
    if not metrics_df.empty:
        # 표시 순서
        cols = ["종목", "티커", "Date", "Close", "Change", "ChangePct", "High", "Low", "Volume"]
        metrics_df = metrics_df[[c for c in cols if c in metrics_df.columns]]
        st.subheader("최근 데이터")
        st.dataframe(apply_number_format(metrics_df), use_container_width=True, height=280)

    # 차트
    st.divider()
    if chart_type == "라인(종가)":
        us_tickers = [t for t in selected_tickers if not t.endswith(".KS")]
        kr_tickers = [t for t in selected_tickers if t.endswith(".KS")]

        def render_group(group_name: str, tickers: list[str]) -> None:
            if not tickers:
                return
            fig = go.Figure()
            for t in tickers:
                if t not in series_by_ticker:
                    continue
                df = series_by_ticker[t].copy()
                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue

                name = ticker_to_name.get(t, t)
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df["Close"],
                        mode="lines",
                        name=f"{name} ({t})",
                        line=dict(width=2),
                    )
                )

                if show_sma:
                    df["SMA_fast"] = df["Close"].rolling(window=sma_fast).mean()
                    df["SMA_slow"] = df["Close"].rolling(window=sma_slow).mean()
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df["SMA_fast"],
                            mode="lines",
                            name=f"{name} SMA({sma_fast})",
                            line=dict(width=1.2, dash="dash"),
                            opacity=0.9,
                        )
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df["SMA_slow"],
                            mode="lines",
                            name=f"{name} SMA({sma_slow})",
                            line=dict(width=1.2, dash="dot"),
                            opacity=0.85,
                        )
                    )

            fig.update_layout(
                height=520,
                template="plotly_white",
                xaxis_title="날짜",
                yaxis_title="가격(종가)",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0,
                ),
                xaxis_rangeslider_visible=False,
            )
            st.subheader(group_name)
            st.plotly_chart(fig, use_container_width=True)

        render_group("미국 종목", us_tickers)
        render_group("한국(원화) 종목", kr_tickers)

    else:
        # 캔들 차트: 1개 종목만
        t = selected_tickers[0]
        s_name = ticker_to_name.get(t, t)
        df = series_by_ticker[t].copy()
        df = df.dropna(subset=["Close"])
        required = {"Open", "High", "Low", "Close"}
        if not required.issubset(set(df.columns)):
            st.error("캔들 차트에 필요한 OHLC 컬럼(Open/High/Low/Close)이 없습니다.")
            return

        fig = go.Figure()
        fig.add_trace(
            go.Candlestick(
                x=df.index,
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"],
                name=f"{s_name} ({t})",
            )
        )

        if show_sma:
            df["SMA_fast"] = df["Close"].rolling(window=sma_fast).mean()
            df["SMA_slow"] = df["Close"].rolling(window=sma_slow).mean()
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["SMA_fast"],
                    mode="lines",
                    name=f"SMA({sma_fast})",
                    line=dict(width=1.5, dash="dash"),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["SMA_slow"],
                    mode="lines",
                    name=f"SMA({sma_slow})",
                    line=dict(width=1.5, dash="dot"),
                )
            )

        fig.update_layout(
            height=560,
            template="plotly_white",
            xaxis_title="날짜",
            yaxis_title="가격(OHLC)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_rangeslider_visible=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.caption("데이터 출처: `yfinance` (공개/집계 데이터). 투자 판단의 근거로만 사용하지 마세요.")


main()

