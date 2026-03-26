"""
yfinance 기반 포스코 홀딩스 주가 데이터 대시보드
"""

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


@st.cache_data(ttl=60 * 60)
def load_ohlcv(ticker: str, period: str, interval: str) -> pd.DataFrame:
    # yfinance는 에러/빈 결과를 조용히 반환할 때가 있어, 다운 후 비어있는지 확인합니다.
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

    # plotly가 datetime 인덱스 타입을 안정적으로 처리하도록 timezone을 제거합니다.
    if hasattr(df.index, "tz") and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # 컬럼 정규화 (yfinance가 간헐적으로 멀티인덱스를 반환할 수 있음)
    if isinstance(df.columns, pd.MultiIndex):
        # (예: ('Open', '005490.KS') 형태) 첫 레벨을 컬럼명으로 사용
        df.columns = df.columns.get_level_values(0)

    return df


def main() -> None:
    st.title("📈 포스코 홀딩스 주가 대시보드")

    with st.sidebar:
        st.header("데이터 설정")

        ticker = st.text_input("티커", value="005490.KS", help="예: 포스코홀딩스 005490.KS")
        period = st.selectbox(
            "조회 기간",
            options=["1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"],
            index=3,
        )
        interval = st.selectbox("데이터 간격", options=["1d", "1wk", "1mo"], index=0)

        st.divider()
        chart_type = st.radio("차트 종류", options=["라인(종가)", "캔들(OHLC)"], index=0)
        show_volume = st.checkbox("거래량 표시(캔들)", value=False)

        st.divider()
        st.header("보조 지표")
        show_sma = st.checkbox("이동평균(SMA) 표시", value=True)
        sma_fast = st.slider("SMA 단기", min_value=5, max_value=60, value=20, step=1)
        sma_slow = st.slider("SMA 장기", min_value=10, max_value=120, value=60, step=1)

    df = load_ohlcv(ticker=ticker.strip(), period=period, interval=interval)
    if df.empty:
        st.warning("데이터를 가져오지 못했습니다. 티커/기간/간격을 확인해 주세요.")
        return

    # 종가 기준으로 마지막/전날 변화율 계산
    close = df["Close"].dropna()
    if len(close) < 2:
        st.warning("데이터가 너무 짧습니다. 기간을 늘려 다시 시도해 주세요.")
        return

    last_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    delta = last_close - prev_close
    delta_pct = (delta / prev_close) * 100 if prev_close != 0 else 0.0

    hi = float(df["High"].max()) if "High" in df.columns else float(close.max())
    lo = float(df["Low"].min()) if "Low" in df.columns else float(close.min())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재가(종가)", f"{last_close:,.0f}")
    c2.metric("전일 대비", f"{delta:+,.0f} ({delta_pct:+.2f}%)")
    c3.metric("기간 최고", f"{hi:,.0f}")
    c4.metric("기간 최저", f"{lo:,.0f}")

    df_plot = df.copy()
    df_plot = df_plot.dropna(subset=["Close"])

    if show_sma:
        df_plot["SMA_fast"] = df_plot["Close"].rolling(window=sma_fast).mean()
        df_plot["SMA_slow"] = df_plot["Close"].rolling(window=sma_slow).mean()

    st.divider()

    if chart_type == "라인(종가)":
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_plot.index,
                y=df_plot["Close"],
                mode="lines",
                name="Close",
                line=dict(color="#1f77b4", width=2),
            )
        )

        if show_sma:
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=df_plot["SMA_fast"],
                    mode="lines",
                    name=f"SMA({sma_fast})",
                    line=dict(width=1.5, dash="dash"),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=df_plot["SMA_slow"],
                    mode="lines",
                    name=f"SMA({sma_slow})",
                    line=dict(width=1.5, dash="dot"),
                )
            )

        fig.update_layout(
            height=480,
            template="plotly_white",
            xaxis_title="날짜",
            yaxis_title="가격",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        # 캔들(OHLC)
        required = {"Open", "High", "Low", "Close"}
        if not required.issubset(set(df_plot.columns)):
            st.error("캔들 차트에 필요한 OHLC 컬럼이 없습니다.")
            return

        fig = go.Figure()
        fig.add_trace(
            go.Candlestick(
                x=df_plot.index,
                open=df_plot["Open"],
                high=df_plot["High"],
                low=df_plot["Low"],
                close=df_plot["Close"],
                name="OHLC",
            )
        )

        if show_sma:
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=df_plot["SMA_fast"],
                    mode="lines",
                    name=f"SMA({sma_fast})",
                    line=dict(width=1.5, dash="dash"),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df_plot.index,
                    y=df_plot["SMA_slow"],
                    mode="lines",
                    name=f"SMA({sma_slow})",
                    line=dict(width=1.5, dash="dot"),
                )
            )

        if show_volume and "Volume" in df_plot.columns:
            # 간단히 거래량을 같은 차트에 오버레이(높이 조절은 생략)
            fig.add_trace(
                go.Bar(
                    x=df_plot.index,
                    y=df_plot["Volume"],
                    name="Volume",
                    opacity=0.25,
                )
            )

        fig.update_layout(
            height=520,
            template="plotly_white",
            xaxis_title="날짜",
            yaxis_title="가격",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_rangeslider_visible=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("최근 데이터")
    out = df_plot.tail(10).reset_index()
    # reset_index()로 생기는 날짜 컬럼명이 'index'일 수도, 'Datetime'(등)일 수도 있어 Date로 표준화합니다.
    first_col = out.columns[0]
    if first_col != "Date":
        out = out.rename(columns={first_col: "Date"})
    # 최근 데이터 숫자 표시: 천 단위 콤마(,) 적용
    styler = out.style
    for col in out.columns:
        if col == "Date":
            continue
        if col == "Volume":
            styler = styler.format({col: "{:,.0f}"})
        else:
            styler = styler.format({col: "{:,.2f}"})
    st.dataframe(styler, use_container_width=True, height=260)

    st.caption("데이터 출처: `yfinance` (공개/집계 데이터). 투자 판단의 근거로만 사용하지 마세요.")

    csv_text = df_plot.to_csv(index=True)
    st.download_button(
        label="CSV 다운로드",
        data=csv_text,
        file_name=f"{ticker}_ohlcv_{period}_{interval}.csv",
        mime="text/csv",
    )


main()

