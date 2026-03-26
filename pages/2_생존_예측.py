"""
성별·객실 등급·나이 입력 → 생존 확률 예측 (로지스틱 회귀, 타이타닉 데이터 학습)
"""

import numpy as np
import streamlit as st
import seaborn as sns
from sklearn.linear_model import LogisticRegression


@st.cache_resource
def train_survival_model():
    df = sns.load_dataset("titanic").dropna(subset=["age"])
    X = np.column_stack(
        [
            df["pclass"].values,
            (df["sex"] == "female").astype(int).values,
            df["age"].values,
        ]
    )
    y = df["survived"].values
    model = LogisticRegression(max_iter=500)
    model.fit(X, y)
    return model


st.title("🎯 생존 여부 예측")
st.caption(
    "동일 타이타닉 데이터로 학습한 로지스틱 회귀 모델입니다. 참고용이며 실제 의미의 의료·안전 예측이 아닙니다."
)

model = train_survival_model()

with st.form("predict_form"):
    col1, col2, col3 = st.columns(3)
    with col1:
        sex = st.selectbox("성별", ["여성", "남성"], index=0)
    with col2:
        pclass = st.selectbox("탑승 클래스", [1, 2, 3], index=2, format_func=lambda x: f"{x}등석")
    with col3:
        age = st.number_input("나이 (세)", min_value=0.0, max_value=120.0, value=30.0, step=1.0)

    submitted = st.form_submit_button("생존 여부 예측", type="primary")

if submitted:
    sex_f = 1 if sex == "여성" else 0
    X_in = np.array([[pclass, sex_f, float(age)]])
    proba = float(model.predict_proba(X_in)[0, 1])
    pred = int(model.predict(X_in)[0])

    st.divider()
    if pred == 1:
        st.success(f"**예측 결과: 생존** (모델 판단)")
    else:
        st.error(f"**예측 결과: 비생존** (모델 판단)")

    st.metric("생존으로 모델이 부여한 확률", f"{proba * 100:.1f}%")
    st.progress(min(max(proba, 0.0), 1.0))

    st.caption(
        f"입력: {sex}, {pclass}등석, {age:g}세 — 확률이 50%보다 높으면 ‘생존’으로 분류합니다."
    )
