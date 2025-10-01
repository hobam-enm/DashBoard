import pandas as pd
import streamlit as st

# ===== 페이지 기본 설정 =====
st.set_page_config(page_title="Dashboard Test", layout="wide")

# ===== 사이드 네비게이터 =====
with st.sidebar:
    st.image("https://via.placeholder.com/150x50?text=LOGO", use_column_width=True)
    st.title("Navigation")
    st.markdown("### Pages")
    st.button("Overview")
    st.button("Funnel")
    st.button("Customers")
    st.button("Products")

# ===== 데이터 불러오기 =====
SHEET_ID = "1fKVPXGN-R2bsrv018dz8zTmg431ZSBHx1PCTnMpdoWY"
GID = "407131354"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

@st.cache_data(ttl=600)
def load_data(url):
    df = pd.read_csv(url)
    return df

df = load_data(CSV_URL)

# ===== 상단 필터 영역 =====
st.markdown("### 🔍 Filters")

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    ip_sel = st.multiselect("IP", sorted(df["IP"].dropna().unique().tolist()))
with col2:
    prog_sel = st.multiselect("편성", sorted(df["편성"].dropna().unique().tolist()))
with col3:
    media_sel = st.multiselect("매체", sorted(df["매체"].dropna().unique().tolist()))
with col4:
    demo_sel = st.multiselect("데모", sorted(df["데모"].dropna().unique().tolist()))
with col5:
    week_range = st.slider("주차 범위", int(df["주차"].min()), int(df["주차"].max()), (int(df["주차"].min()), int(df["주차"].max())))

# 필터 적용
f = df.copy()
if ip_sel:
    f = f[f["IP"].isin(ip_sel)]
if prog_sel:
    f = f[f["편성"].isin(prog_sel)]
if media_sel:
    f = f[f["매체"].isin(media_sel)]
if demo_sel:
    f = f[f["데모"].isin(demo_sel)]
f = f[(f["주차"] >= week_range[0]) & (f["주차"] <= week_range[1])]

# ===== 오버뷰 KPI (T시청률 평균) =====
st.markdown("## 📊 Overview")

overview_area = st.container()
with overview_area:
    st.subheader("IP별 평균 T시청률")
    kpi_df = f[f["metric"] == "T시청률"].groupby("IP", as_index=False)["value"].mean()
    if kpi_df.empty:
        st.info("조건에 맞는 데이터가 없습니다.")
    else:
        st.dataframe(kpi_df, use_container_width=True)

# ===== 이후 확장 공간 (비워둠) =====
st.divider()
st.markdown("### ⬜ More KPIs and Charts (Reserved)")
