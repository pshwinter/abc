"""
타이타닉 데이터 탐색 대시보드
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns

# 대시보드 페이지만 연한 파란색 배경
st.markdown(
    """
    <style>
        .stApp {
            background: linear-gradient(180deg, #e3f2fd 0%, #f5f9ff 45%, #eef6ff 100%);
        }
        [data-testid="stHeader"] {
            background-color: rgba(227, 242, 253, 0.92);
        }
        [data-testid="stSidebar"] {
            background-color: rgba(225, 245, 254, 0.65);
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def load_data():
    return sns.load_dataset("titanic")


def main():
    df = load_data()

    st.title("🚢 타이타닉 생존 분석 대시보드")
    st.caption("Seaborn 내장 `titanic` 데이터셋 기반")

    with st.sidebar:
        st.header("필터")
        sex_opts = ["전체"] + sorted(df["sex"].dropna().unique().tolist())
        sex_sel = st.selectbox("성별", sex_opts)
        embarked_opts = ["전체"] + sorted(df["embarked"].dropna().unique().tolist())
        embarked_sel = st.selectbox("승선 항구", embarked_opts)
        pclass_opts = ["전체"] + sorted(df["pclass"].dropna().unique().tolist())
        pclass_sel = st.selectbox("객실 등급", pclass_opts)

    filt = df.copy()
    if sex_sel != "전체":
        filt = filt[filt["sex"] == sex_sel]
    if embarked_sel != "전체":
        filt = filt[filt["embarked"] == embarked_sel]
    if pclass_sel != "전체":
        filt = filt[filt["pclass"] == pclass_sel]

    n = len(filt)
    survived = int(filt["survived"].sum()) if n else 0
    rate = (survived / n * 100) if n else 0.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("표본 수", f"{n:,}")
    with c2:
        st.metric("생존자 수", f"{survived:,}")
    with c3:
        st.metric("생존률", f"{rate:.1f}%")
    with c4:
        avg_age = filt["age"].mean()
        st.metric("평균 연령", f"{avg_age:.1f}세" if pd.notna(avg_age) else "—")

    st.divider()

    row1_left, row1_right = st.columns(2)

    with row1_left:
        st.subheader("객실 등급별 생존")
        ct = filt.groupby("pclass", as_index=False).agg(
            생존=("survived", "sum"), 전체=("survived", "count")
        )
        ct["생존률_%"] = (ct["생존"] / ct["전체"] * 100).round(1)
        ct["pclass"] = ct["pclass"].astype(str) + "등급"
        fig_p = px.bar(
            ct,
            x="pclass",
            y="생존률_%",
            text="생존률_%",
            color="pclass",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_p.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_p.update_layout(
            showlegend=False,
            yaxis_title="생존률 (%)",
            xaxis_title="객실 등급",
            height=400,
        )
        st.plotly_chart(fig_p, use_container_width=True)

    with row1_right:
        st.subheader("성별 생존 비율")
        sex_ct = filt.groupby("sex", as_index=False).agg(
            생존=("survived", "sum"), 전체=("survived", "count")
        )
        sex_ct["생존률_%"] = (sex_ct["생존"] / sex_ct["전체"] * 100).round(1)
        fig_s = px.bar(
            sex_ct,
            x="sex",
            y="생존률_%",
            text="생존률_%",
            color="sex",
            color_discrete_map={"male": "#5DADE2", "female": "#F5B7B1"},
        )
        fig_s.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_s.update_layout(
            showlegend=False,
            yaxis_title="생존률 (%)",
            xaxis_title="성별",
            height=400,
        )
        st.plotly_chart(fig_s, use_container_width=True)

    row2_left, row2_right = st.columns(2)

    with row2_left:
        st.subheader("연령 분포 (생존 여부)")
        age_df = filt.dropna(subset=["age"]).copy()
        age_df["생존여부"] = age_df["survived"].map({0: "사망", 1: "생존"})
        fig_h = px.histogram(
            age_df,
            x="age",
            color="생존여부",
            nbins=30,
            barmode="overlay",
            opacity=0.7,
            color_discrete_map={"사망": "#E74C3C", "생존": "#27AE60"},
            labels={"age": "연령"},
        )
        fig_h.update_layout(height=400, legend_title_text="구분")
        st.plotly_chart(fig_h, use_container_width=True)

    with row2_right:
        st.subheader("요금 분포")
        fare_df = filt.dropna(subset=["fare"])
        fig_f = px.box(
            fare_df,
            x="pclass",
            y="fare",
            color="pclass",
            points="outliers",
            labels={"pclass": "객실 등급", "fare": "요금 (£)"},
        )
        fig_f.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_f, use_container_width=True)

    st.subheader("승선 항구 × 객실 등급 (생존자 수)")
    heat = filt.pivot_table(
        index="embarked",
        columns="pclass",
        values="survived",
        aggfunc="sum",
        fill_value=0,
    )
    fig_heat = px.imshow(
        heat.values,
        labels=dict(x="객실 등급", y="승선 항구", color="생존자 수"),
        x=[str(c) for c in heat.columns],
        y=[str(i) for i in heat.index],
        color_continuous_scale="Blues",
        aspect="auto",
    )
    fig_heat.update_layout(height=350)
    st.plotly_chart(fig_heat, use_container_width=True)

    with st.expander("원본 데이터 미리보기"):
        st.dataframe(filt, use_container_width=True, height=280)


main()
