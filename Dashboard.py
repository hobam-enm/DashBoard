import pandas as pd
import streamlit as st

# ===== 페이지 기본 설정 =====
st.set_page_config(page_title="Dashboard Test", layout="wide")

# ===== CSS 커스터마이징 (연한 회색 배경 네비게이터) =====
st.markdown("""
    <style>
    section[data-testid="stSidebar"] {
        background-color: #f5f5f5;
        padding-top: 20px;
    }
    .sidebar-logo {
        font-size: 20px;
        font-weight: bold;
        color: #333;
        text-align: center;
        margin-bottom: 20px;
    }
    .nav-item {
        display: block;
        width: 100%;
        padding: 10px 15px;
        color: #444;
        text-decoration: none;
        font-weight: 500;
        border-radius: 6px;
        margin: 3px 0px; /* 버튼 간격 좁게 */
    }
    .nav-item:hover {
        background-color: #e0e0e0;
        color: #000;
    }
    .active {
        background-color: #4a6cf7; /* 강조색 */
        color: #fff !important;
    }
    </style>
""", unsafe_allow_html=True)

# ===== 사이드바 HTML =====
with st.sidebar:
    st.markdown('<div class="sidebar-logo">📊 DashBoard</div>', unsafe_allow_html=True)
    st.markdown('<a class="nav-item active" href="#">Overview</a>', unsafe_allow_html=True)
    st.markdown('<a class="nav-item" href="#">Funnel</a>', unsafe_allow_html=True)
    st.markdown('<a class="nav-item" href="#">Customers</a>', unsafe_allow_html=True)
    st.markdown('<a class="nav-item" href="#">Products</a>', unsafe_allow_html=True)

# ===== 데이터 불러오기 =====
SHEET_ID = "1fKVPXGN-R2bsrv018dz8zTmg431ZSBHx1PCTnMpdoWY"
GID = "407131354"  # RAW_원본 시트
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

@st.cache_data(ttl=600)
def load_data(url):
    df = pd.read_csv(url)
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "주차시작일" in df.columns:
        df["주차시작일"] = pd.to_datetime(df["주차시작일"], errors="coerce")
    return df

df = load_data(CSV_URL)

# ===== 상단 필터 =====
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
    min_date, max_date = df["주차시작일"].min(), df["주차시작일"].max()
    week_range = st.slider(
        "주차 범위",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY.MM.DD"
    )

# ===== 필터 적용 =====
f = df.copy()
if ip_sel:
    f = f[f["IP"].isin(ip_sel)]
if prog_sel:
    f = f[f["편성"].isin(prog_sel)]
if media_sel:
    f = f[f["매체"].isin(media_sel)]
if demo_sel:
    f = f[f["데모"].isin(demo_sel)]
f = f[(f["주차시작일"] >= week_range[0]) & (f["주차시작일"] <= week_range[1])]

# ===== 오버뷰 KPI (T시청률 평균) =====
st.markdown("## 📊 Overview")

st.subheader("IP별 평균 T시청률")
kpi_df = f[f["metric"] == "T시청률"].groupby("IP", as_index=False)["value"].mean()

if kpi_df.empty:
    st.info("조건에 맞는 데이터가 없습니다.")
else:
    st.dataframe(kpi_df, use_container_width=True)

# ===== 확장 공간 =====
st.divider()
st.markdown("### ⬜ More KPIs and Charts (Reserved)")
