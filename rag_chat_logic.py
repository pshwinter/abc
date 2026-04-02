"""
RAG 챗봇: PDF·CSV·엑셀 → 청크 → FAISS (+ 선택 BM25 하이브리드) → (선택) Cross-Encoder 리랭커 → LLM.
"""

from __future__ import annotations

import os
import tempfile
from io import BytesIO
from operator import itemgetter
from typing import Any

import pandas as pd
from langchain_core.retrievers import BaseRetriever
from langchain_classic.retrievers import ContextualCompressionRetriever, EnsembleRetriever
from langchain_classic.retrievers.document_compressors import CrossEncoderReranker
from langchain_classic.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from langchain_community.document_loaders import PyPDFLoader
from langchain_community.retrievers import BM25Retriever
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings


def documents_from_pdf_bytes(data: bytes, source_name: str) -> list[Document]:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(data)
        path = tmp.name
    try:
        loader = PyPDFLoader(path)
        docs = loader.load()
        for d in docs:
            d.metadata = dict(d.metadata or {})
            d.metadata["source"] = source_name
        return docs
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def documents_from_csv_bytes(data: bytes, source_name: str) -> list[Document]:
    last_err: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            df = pd.read_csv(BytesIO(data), encoding=encoding)
            text = df.to_csv(index=False)
            return [
                Document(
                    page_content=text,
                    metadata={"source": source_name},
                )
            ]
        except Exception as e:
            last_err = e
            continue
    raise ValueError(f"CSV를 읽을 수 없습니다: {source_name} — {last_err}")


def documents_from_excel_bytes(data: bytes, source_name: str) -> list[Document]:
    buf = BytesIO(data)
    try:
        xl = pd.ExcelFile(buf, engine="openpyxl" if source_name.lower().endswith(".xlsx") else None)
    except Exception as e:
        raise ValueError(
            f"엑셀을 읽을 수 없습니다. .xlsx(openpyxl)를 권장합니다: {source_name} — {e}"
        ) from e
    out: list[Document] = []
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet)
        text = f"[시트: {sheet}]\n" + df.to_csv(index=False)
        out.append(
            Document(
                page_content=text,
                metadata={"source": source_name, "sheet": sheet},
            )
        )
    return out


def load_uploaded_documents(files: list[Any]) -> list[Document]:
    """Streamlit UploadedFile 리스트 → LangChain Document."""
    all_docs: list[Document] = []
    for f in files:
        name = getattr(f, "name", "upload")
        raw = f.getvalue()
        lower = name.lower()
        if lower.endswith(".pdf"):
            all_docs.extend(documents_from_pdf_bytes(raw, name))
        elif lower.endswith(".csv"):
            all_docs.extend(documents_from_csv_bytes(raw, name))
        elif lower.endswith((".xlsx", ".xls")):
            all_docs.extend(documents_from_excel_bytes(raw, name))
        else:
            continue
    return all_docs


def split_documents(
    documents: list[Document],
    chunk_size: int,
    chunk_overlap: int,
) -> list[Document]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )
    return splitter.split_documents(documents)


def _normalize_hybrid_weights(bm25_weight: float, vector_weight: float) -> tuple[float, float]:
    wsum = bm25_weight + vector_weight
    if wsum <= 0:
        return 0.5, 0.5
    return bm25_weight / wsum, vector_weight / wsum


def build_base_retriever(
    chunks: list[Document],
    search_k: int,
    *,
    use_hybrid: bool,
    bm25_weight: float,
    vector_weight: float,
) -> BaseRetriever:
    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_documents(chunks, embeddings)
    vector_retriever = vectorstore.as_retriever(search_kwargs={"k": search_k})

    if not use_hybrid:
        return vector_retriever

    bm25_weight, vector_weight = _normalize_hybrid_weights(bm25_weight, vector_weight)
    bm25_retriever = BM25Retriever.from_documents(chunks)
    bm25_retriever.k = search_k

    return EnsembleRetriever(
        retrievers=[bm25_retriever, vector_retriever],
        weights=[bm25_weight, vector_weight],
    )


def build_rag_retriever(
    chunks: list[Document],
    search_k: int,
    *,
    use_hybrid: bool,
    bm25_weight: float,
    vector_weight: float,
    use_reranker: bool,
    reranker_top_n: int,
    cross_encoder: HuggingFaceCrossEncoder | None,
) -> BaseRetriever:
    base = build_base_retriever(
        chunks,
        search_k,
        use_hybrid=use_hybrid,
        bm25_weight=bm25_weight,
        vector_weight=vector_weight,
    )
    if use_reranker:
        if cross_encoder is None:
            raise ValueError("리랭커를 켠 경우 Cross-Encoder 모델이 필요합니다.")
        reranker = CrossEncoderReranker(model=cross_encoder, top_n=reranker_top_n)
        return ContextualCompressionRetriever(
            base_compressor=reranker,
            base_retriever=base,
        )
    return base


def build_rag_chain(
    retriever: BaseRetriever,
    llm_model: str = "gpt-4o-mini",
) -> Any:
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "다음 참고 문서(context)만을 근거로 한국어로 답변하세요. 근거가 없으면 모른다고 말하세요.\n\n참고:\n{context}"),
            ("user", "{question}"),
        ]
    )
    llm = ChatOpenAI(model=llm_model, temperature=0)
    return (
        {
            "context": itemgetter("question") | retriever,
            "question": itemgetter("question"),
        }
        | prompt
        | llm
        | StrOutputParser()
    )
