"""
포스코 홀딩스 주가 대시보드 — 홈 (멀티페이지 진입점)
"""

import streamlit as st

st.set_page_config(
    page_title="포스코 홀딩스 주가 대시보드",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 포스코 홀딩스 주가 데이터 대시보드")
st.markdown(
    "왼쪽 사이드바에서 **포스코 홀딩스 주가 대시보드** 페이지로 이동하면 "
    "`yfinance`로 주가 데이터를 불러와 차트/지표/표를 볼 수 있습니다."
)
