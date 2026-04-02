"""
더미데이터·기준정보 엑셀 업로드 기반 수불(입고/사용/재고) 현황.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Literal

import pandas as pd
import streamlit as st

import su_bulk_logic as sbl
import rag_chat_logic as rag
from langchain_community.cross_encoders import HuggingFaceCrossEncoder

st.set_page_config(page_title="수불 현황", page_icon="📊", layout="wide")


def _scale_qty(df: pd.DataFrame, div: float) -> pd.DataFrame:
    if df.empty or div == 1.0:
        return df
    out = df.copy()
    if "qty" in out.columns:
        out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0.0) / div
    return out


_RAG_CHAIN_KEY = "su_hyun_rag_chain"
_RAG_MESSAGES_KEY = "su_hyun_rag_messages"
_RAG_META_KEY = "su_hyun_rag_meta"


@st.cache_resource
def _cross_encoder(model_name: str) -> HuggingFaceCrossEncoder:
    return HuggingFaceCrossEncoder(model_name=model_name)


def _init_rag_state() -> None:
    if _RAG_MESSAGES_KEY not in st.session_state:
        st.session_state[_RAG_MESSAGES_KEY] = []
    if _RAG_CHAIN_KEY not in st.session_state:
        st.session_state[_RAG_CHAIN_KEY] = None
    if _RAG_META_KEY not in st.session_state:
        st.session_state[_RAG_META_KEY] = ""


def main() -> None:
    _init_rag_state()
    st.title("📊 철스크랩 수불현황")

    with st.sidebar:
        st.header("조회·환산")
        qd = st.date_input("조회일자", value=date.today())
        ton_div = st.number_input(
            "수량 ÷ (톤 환산)",
            min_value=1e-9,
            value=1.0,
            help="엑셀이 kg이면 1000을 입력해 톤으로 맞출 수 있습니다.",
        )
        st.divider()
        f_data = st.file_uploader("더미데이터 (.xlsx)", type=["xlsx"])
        f_ref = st.file_uploader("기준정보 (.xlsx)", type=["xlsx"])

        st.divider()
        st.header("💬 RAG 챗봇")
        env_key = os.environ.get("OPENAI_API_KEY", "") or ""
        api_key = st.text_input(
            "OPENAI_API_KEY",
            value=env_key,
            type="password",
            help="비워 두면 환경 변수 `OPENAI_API_KEY`를 사용합니다.",
        )
        llm_model = st.text_input("챗 LLM 모델", value="gpt-4o-mini")

        st.divider()
        st.header("청크 / 검색")
        chunk_size = st.number_input("청크 크기", min_value=50, max_value=8000, value=800, step=50)
        chunk_overlap = st.number_input("청크 오버랩", min_value=0, max_value=2000, value=100, step=10)
        search_k = st.number_input("검색 문서 개수 (k)", min_value=1, max_value=50, value=6, step=1)

        st.divider()
        st.header("하이브리드 검색")
        use_hybrid = st.toggle("하이브리드 검색 (BM25 + 벡터)", value=False)
        w_bm25 = 0.4
        w_vec = 0.6
        if use_hybrid:
            st.caption("가중치 합으로 나누어 정규화됩니다.")
            st.caption("주의: `rank_bm25`가 설치되어 있어야 BM25가 동작합니다.")
            w_bm25 = st.slider("BM25 가중치", 0.0, 1.0, 0.4, 0.05)
            w_vec = st.slider("벡터(임베딩) 가중치", 0.0, 1.0, 0.6, 0.05)
        else:
            st.caption("꺼 두면 **벡터(FAISS) 검색만** 사용합니다.")

        st.divider()
        st.header("리랭커")
        use_reranker = st.toggle("Cross-Encoder 리랭커", value=False)
        rerank_top_n = 4
        rerank_model = "BAAI/bge-reranker-base"
        if use_reranker:
            rerank_top_n = st.number_input(
                "리랭커 top_n",
                min_value=1,
                max_value=30,
                value=4,
                step=1,
                help="검색 결과 중 상위 top_n만 LLM 컨텍스트로 압축·선별합니다.",
            )
            rerank_model = st.selectbox(
                "Cross-Encoder 모델",
                options=[
                    "BAAI/bge-reranker-base",
                    "BAAI/bge-reranker-large",
                    "cross-encoder/ms-marco-MiniLM-L-6-v2",
                ],
                index=0,
            )

        st.divider()
        build_btn = st.button("엑셀 기반 인덱스 구축 및 챗봇 준비", type="primary")

    if not f_data or not f_ref:
        st.info("사이드바에서 **더미데이터**와 **기준정보** 엑셀을 모두 업로드하세요.")
        st.markdown(
            "**더미데이터 시트:** `사용`, `입고`, `예상사용량`, `예상입고량`, `기초재고`  \n"
            "**기준정보 시트:** `사소지역`(또는 `사소기준`) — 열 `사소`, `지역`(포항/광양), "
            "`ITEM공급사` 등 — 열 `ITEM`, `공급사구분`(유통/MOU/회수)"
        )
        return

    raw = sbl.load_workbooks(f_data.getvalue(), f_ref.getvalue())
    errs = sbl.validate_minimum(raw)
    if errs:
        for e in errs:
            st.error(e)
        return

    # RAG 인덱스/체인 구축 (버튼 클릭 시에만 수행)
    key_eff = (api_key or "").strip() or env_key
    if key_eff:
        os.environ["OPENAI_API_KEY"] = key_eff

    if build_btn:
        if not key_eff:
            st.error("OpenAI API 키를 입력하거나 환경 변수를 설정하세요.")
        else:
            with st.spinner("더미 엑셀 문서 적재·청크·인덱스 구축 중…"):
                try:
                    docs_files = [f_data, f_ref]
                    docs = rag.load_uploaded_documents(list(docs_files))
                    if not docs:
                        st.error("지원하는 형식의 문서가 없습니다 (xlsx를 확인하세요).")
                    else:
                        chunks = rag.split_documents(docs, int(chunk_size), int(chunk_overlap))
                        if not chunks:
                            st.error("청크 결과가 비었습니다. 청크 크기를 조정해 보세요.")
                        else:
                            ce = _cross_encoder(rerank_model) if use_reranker else None
                            try:
                                retriever = rag.build_rag_retriever(
                                    chunks,
                                    search_k=int(search_k),
                                    use_hybrid=use_hybrid,
                                    bm25_weight=float(w_bm25),
                                    vector_weight=float(w_vec),
                                    use_reranker=use_reranker,
                                    reranker_top_n=int(rerank_top_n),
                                    cross_encoder=ce,
                                )
                            except ImportError as e:
                                # LangChain의 BM25Retriever는 rank_bm25가 없으면 from_documents에서 ImportError가 납니다.
                                if "rank_bm25" in str(e):
                                    st.warning("`rank_bm25`가 설치되어 있지 않아 하이브리드 검색을 끕니다. 벡터(FAISS) 검색만 사용합니다.")
                                    retriever = rag.build_rag_retriever(
                                        chunks,
                                        search_k=int(search_k),
                                        use_hybrid=False,
                                        bm25_weight=float(w_bm25),
                                        vector_weight=float(w_vec),
                                        use_reranker=use_reranker,
                                        reranker_top_n=int(rerank_top_n),
                                        cross_encoder=ce,
                                    )
                                else:
                                    raise
                            st.session_state[_RAG_CHAIN_KEY] = rag.build_rag_chain(
                                retriever, llm_model=llm_model.strip() or "gpt-4o-mini"
                            )
                            st.session_state[_RAG_MESSAGES_KEY] = []
                            names = ", ".join(f.name for f in docs_files)
                            hybrid_s = (
                                f"하이브리드 ON (BM25={w_bm25:.2f}, 벡터={w_vec:.2f})"
                                if use_hybrid
                                else "벡터만"
                            )
                            rr_s = (
                                f"리랭커 ON (top_n={rerank_top_n}, {rerank_model})"
                                if use_reranker
                                else "리랭커 OFF"
                            )
                            st.session_state[_RAG_META_KEY] = f"{names} | 청크 {len(chunks)}개 | k={search_k} | {hybrid_s} | {rr_s}"
                            st.success("인덱스 구축 완료. 아래에서 질문하세요.")
                except Exception as e:
                    st.exception(e)

    (
        u_a,
        r_a,
        u_f,
        r_f,
        opening,
        ref_site,
        ref_supply,
        ms,
        me,
        periods,
    ) = sbl.prepare_frames(raw, qd)

    u_a = _scale_qty(u_a, ton_div)
    r_a = _scale_qty(r_a, ton_div)
    u_f = _scale_qty(u_f, ton_div)
    r_f = _scale_qty(r_f, ton_div)
    opening = _scale_qty(opening, ton_div)

    um, rm = sbl.full_movement_month(raw, ref_site, ms, me)
    um = _scale_qty(um, ton_div)
    rm = _scale_qty(rm, ton_div)

    month_title = f"{qd.year}년 {qd.month}월"
    st.subheader(f"[{month_title}] 수불 현황")

    def region_block(region: str) -> None:
        grades: list[tuple[str, Literal["all", "high", "low"]]] = [
            ("① 총계 (입고·사용·재고)", "all"),
            ("② 고급 (ADS01)", "high"),
            ("③ 저급 (ADS02·ADS04)", "low"),
        ]
        for sub, g in grades:
            st.markdown(f"**{sub}**")
            xk, xt, ra, ua, ia, rf, uf, ifc, iflag = sbl.bucket_metrics(
                u_a,
                r_a,
                u_f,
                r_f,
                opening,
                ref_site,
                periods,
                region,
                g,
                qd,
                ms,
                me,
            )
            fig = sbl.make_mixed_chart(
                "",
                xk,
                xt,
                ra,
                ua,
                ia,
                rf,
                uf,
                ifc,
                iflag,
                highlight_x_key=qd.isoformat(),
            )
            st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.markdown("### 포항")
        region_block("포항")
    with right:
        st.markdown("### 광양")
        region_block("광양")

    st.divider()
    st.subheader(f"ITEM별/계약구분별 입고 및 사용현황 - {qd.month}월 {qd.day}일")

    if um.empty and rm.empty:
        st.warning("막대용 입고·사용 데이터가 없습니다.")
    else:
        bar_l, bar_r = st.columns(2, gap="large")
        with bar_l:
            st.markdown("##### 포항")
            r_ph = rm[rm["지역"] == "포항"].copy()
            u_ph = um[um["지역"] == "포항"].copy()
            fig_ph = sbl.site_bar_figure_v2(qd, "포항", r_ph, u_ph, ref_supply)
            st.plotly_chart(fig_ph, use_container_width=True)
        with bar_r:
            st.markdown("##### 광양")
            r_gw = rm[rm["지역"] == "광양"].copy()
            u_gw = um[um["지역"] == "광양"].copy()
            fig_gw = sbl.site_bar_figure_v2(qd, "광양", r_gw, u_gw, ref_supply)
            st.plotly_chart(fig_gw, use_container_width=True)

    st.divider()
    st.subheader("엑셀보내기")
    summary_rows: list[dict] = []
    daily_parts: list[pd.DataFrame] = []

    for region in ("포항", "광양"):
        for gname, g in (("총계", "all"), ("고급", "high"), ("저급", "low")):
            xk, xt, ra, ua, ia, rfc, ufc, ifc, iflag = sbl.bucket_metrics(
                u_a,
                r_a,
                u_f,
                r_f,
                opening,
                ref_site,
                periods,
                region,
                g,
                qd,
                ms,
                me,
            )
            for i in range(len(xk)):
                lab = xt[i] if i < len(xt) and xt[i] else xk[i]
                summary_rows.append(
                    {
                        "지역": region,
                        "구분": gname,
                        "일자": xk[i],
                        "x축_틱": lab,
                        "입고_실적": ra[i],
                        "입고_예상": rfc[i],
                        "사용_실적": ua[i],
                        "사용_예상": ufc[i],
                        "재고_실적막대": ia[i],
                        "재고_예상막대": ifc[i],
                        "예상구간": iflag[i],
                    }
                )
            recv_d, use_d, inv_d, fc_d = sbl.daily_recv_use_inv(
                ms, me, qd, r_a, u_a, r_f, u_f, region, g, opening, ref_site
            )
            dr = pd.DataFrame(
                [{"일자": d, "입고": recv_d[d], "사용": use_d[d], "재고": inv_d[d], "예상일": fc_d[d]} for d in sorted(recv_d)]
            )
            dr.insert(0, "지역", region)
            dr.insert(1, "구분", gname)
            daily_parts.append(dr)

    daily_detail = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame()
    xbytes = sbl.export_workbook_bytes(summary_rows, daily_detail)
    st.download_button(
        "집계 결과 엑셀 다운로드",
        data=xbytes,
        file_name=f"수불현황_{qd:%Y%m%d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.divider()
    st.subheader("💬 수불 엑셀 기반 챗봇")
    st.caption("답변은 업로드한 엑셀 텍스트를 근거로 생성되며, `수량 ÷ 톤 환산(ton_div)` 반영값과 다를 수 있습니다.")

    if st.session_state[_RAG_META_KEY]:
        st.info(st.session_state[_RAG_META_KEY])

    if st.session_state[_RAG_CHAIN_KEY] is None:
        st.info("사이드바에서 파일을 올린 뒤 `엑셀 기반 인덱스 구축 및 챗봇 준비`를 눌러 주세요.")
        return

    for msg in st.session_state[_RAG_MESSAGES_KEY]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("엑셀 내용에 대해 질문하세요"):
        st.session_state[_RAG_MESSAGES_KEY].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("답변 생성 중…"):
                try:
                    ans = st.session_state[_RAG_CHAIN_KEY].invoke({"question": prompt})
                except Exception as e:
                    ans = f"오류: {e}"
            st.markdown(ans)

        st.session_state[_RAG_MESSAGES_KEY].append({"role": "assistant", "content": ans})


main()
