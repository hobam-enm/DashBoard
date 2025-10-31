# 📊 Overview / IP 성과 대시보드 — v2.0  (boot-guard 제거 완전판)

#region [ 1. 라이브러리 임포트 ]
# =====================================================
import os
import sys
import platform
import traceback
import datetime
import re
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import plotly.express as px
from plotly import graph_objects as go
import plotly.io as pio
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
#endregion


#region [ 2. 기본 설정 및 공통 상수 ]
# =====================================================
# 페이지 설정
st.set_page_config(
    page_title="Overview Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Streamlit Secrets에서 환경 변수 읽기
# .streamlit/secrets.toml 예시:
# SHEET_ID="1fKVPXGN-...."
# GID="407131354"  # 또는 RAW_WORKSHEET="RAW_원본"
# [gcp_service_account]
# type="service_account"
# project_id="..."
# private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email="..."
SHEET_ID = st.secrets.get("SHEET_ID", "").strip()
GID_OR_NAME = str(st.secrets.get("GID", st.secrets.get("RAW_WORKSHEET", ""))).strip()

# Plotly 기본 테마
pio.templates.default = "plotly_white"

# 공통 상수
SIDEBAR_WIDTH = 300
#endregion


#region [ 2-2. 라우팅/네비 유틸 ]
# =====================================================
from collections import OrderedDict

NAV_ITEMS: "OrderedDict[str, str]" = OrderedDict([
    ("Overview",   "Overview"),
    ("IP 성과",    "IP 성과"),
    ("데모그래픽", "데모그래픽"),
    ("비교분석",   "비교분석"),
    ("회차별",     "회차별"),
    ("성장스코어", "성장스코어"),
])

def _qp_get_all() -> dict:
    try:
        qp = getattr(st, "query_params", None)
        if qp is not None:
            d = dict(qp)
            return {k: (v[0] if isinstance(v, list) else v) for k, v in d.items()}
    except Exception:
        pass
    try:
        d = st.experimental_get_query_params()
        return {k: (v[0] if isinstance(v, list) else v) for k, v in d.items()}
    except Exception:
        return {}

def _qp_set_all(update_dict: dict) -> None:
    try:
        if hasattr(st, "query_params"):
            try:
                st.query_params.clear()
                for k, v in update_dict.items():
                    st.query_params[k] = v
                return
            except Exception:
                try:
                    st.query_params = update_dict
                    return
                except Exception:
                    pass
    except Exception:
        pass
    try:
        st.experimental_set_query_params(**update_dict)
    except Exception:
        pass

def get_current_page_default(default_page: str = "Overview") -> str:
    qp = _qp_get_all()
    page = str(qp.get("page", "")).strip() or default_page
    return page

def goto_page(new_page: str) -> None:
    qp = _qp_get_all()
    qp["page"] = new_page
    _qp_set_all(qp)
    st.rerun()
# =====================================================
#endregion


#region [ 3. 구글 시트 인증/연결 ]  # (boot-guard 제거)
# =====================================================
def get_gspread_client():
    # 부트가드 제거: 바로 인증 시도 + 친절한 에러
    try:
        creds_info = dict(st.secrets["gcp_service_account"])
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error("gspread 인증 실패: secrets에 서비스계정 또는 권한(scope)을 확인하세요.")
        st.exception(e)
        raise

def open_worksheet(sheet_id: str, gid_or_name: str):
    # 부트가드 제거: 직접 오픈 + 예외 처리
    try:
        sh = GC.open_by_key(sheet_id)
        if not gid_or_name:
            # 기본: 첫번째 워크시트
            ws = sh.sheet1
        else:
            if gid_or_name.isdigit():
                ws = sh.get_worksheet_by_id(int(gid_or_name))
            else:
                ws = sh.worksheet(gid_or_name)
        if ws is None:
            raise RuntimeError(f"워크시트를 찾을 수 없음: {gid_or_name}")
        return ws
    except Exception as e:
        st.error("RAW 워크시트 열기 실패: SHEET_ID, GID(or 이름)을 확인하세요.")
        st.exception(e)
        raise

GC = get_gspread_client()
WS = open_worksheet(SHEET_ID, GID_OR_NAME)
#endregion


#region [ 3. 공통 함수: 데이터 로드 / 유틸리티 ]
# =====================================================
def _df_basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # 날짜 파싱
    if "주차시작일" in df.columns:
        df["주차시작일"] = pd.to_datetime(
            df["주차시작일"].astype(str).str.strip(), format="%Y. %m. %d", errors="coerce"
        )
    if "방영시작일" in df.columns:
        df["방영시작일"] = pd.to_datetime(
            df["방영시작일"].astype(str).str.strip(), format="%Y. %m. %d", errors="coerce"
        )

    # 숫자형
    if "value" in df.columns:
        v = (
            df["value"].astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
        df["value"] = pd.to_numeric(v, errors="coerce").fillna(0)

    # 문자열 정제
    for c in ["IP", "편성", "지표구분", "매체", "데모", "metric", "회차", "주차"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # 파생: 회차 숫자
    if "회차" in df.columns:
        df["회차_numeric"] = df["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    else:
        df["회차_numeric"] = pd.NA
    return df


def _load_df_from_ws(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0] if len(values) >= 1 else []
    rows = values[1:] if len(values) >= 2 else []
    if not header:
        raise ValueError("RAW 시트의 헤더 행을 찾을 수 없습니다. (1행이 비어있음)")
    df = pd.DataFrame(rows, columns=header)
    return df


@st.cache_data(ttl=600)
def load_data(_: str = "") -> pd.DataFrame:
    try:
        if "WS" not in globals() or WS is None:
            raise RuntimeError("WS(워크시트)가 초기화되지 않았습니다.")
        df = _load_df_from_ws(WS)
        df = _df_basic_clean(df)
        return df
    except Exception as e:
        st.error(f"데이터 로드 오류: {e.__class__.__name__}: {e}")
        st.exception(e)
        return pd.DataFrame()
# =====================================================
#endregion


#region [ 4. 공통 스타일 ]
# =====================================================
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background-color: #f8f9fa; }
div[data-testid="stVerticalBlockBorderWrapper"] {
  background-color: #ffffff; border: 1px solid #e9e9e9; border-radius: 10px;
  box-shadow: 0 2px 5px rgba(0,0,0,0.03); padding: 1.25rem 1.25rem 1.5rem 1.25rem; margin-bottom: 1.5rem;
}
section[data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #e0e0e0;
  padding-top: 1rem; padding-left: 0.5rem; padding-right: 0.5rem; min-width:300px !important; max-width:300px !important; }
div[data-testid="collapsedControl"] { display:none !important; }
.sidebar-logo{ font-size:28px; font-weight:700; color:#1a1a1a; text-align:center; margin-bottom:10px; padding-top:10px; }
.nav-item{ display:block; width:100%; padding:12px 15px; color:#333 !important; background:#f1f3f5; text-decoration:none !important; font-weight:600;
  border-radius:8px; margin-bottom:5px; text-align:center; transition: background-color .2s ease, color .2s ease; }
.nav-item:hover{ background:#e9ecef; color:#000 !important; text-decoration:none; }
.active{ background:#004a99; color:#fff !important; text-decoration:none; font-weight:700; }
.active:hover{ background:#003d80; color:#fff !important; }
.kpi-card { background:#fff; border:1px solid #e9e9e9; border-radius:10px; padding:20px 15px; text-align:center; box-shadow:0 2px 5px rgba(0,0,0,0.03);
  height:100%; display:flex; flex-direction:column; justify-content:center; }
.kpi-title { font-size:15px; font-weight:600; margin-bottom:10px; color:#444; }
.kpi-value { font-size:28px; font-weight:700; color:#000; line-height:1.2; }
.kpi-subwrap { margin-top:10px; line-height:1.4; }
.kpi-sublabel { font-size:12px; font-weight:500; color:#555; letter-spacing:.1px; margin-right:6px; }
.kpi-substrong { font-size:14px; font-weight:700; color:#111; }
.kpi-subpct { font-size:14px; font-weight:700; }
.ag-theme-streamlit { font-size:13px; }
.ag-theme-streamlit .ag-root-wrapper { border-radius:8px; }
.ag-theme-streamlit .ag-row-hover { background-color:#f5f8ff !important; }
.ag-theme-streamlit .ag-header-cell-label { justify-content:center !important; }
.ag-theme-streamlit .centered-header .ag-header-cell-label { justify-content:center !important; }
.ag-theme-streamlit .centered-header .ag-sort-indicator-container { margin-left:4px; }
.ag-theme-streamlit .bold-header .ag-header-cell-text { font-weight:700 !important; font-size:13px; color:#111; }
.sec-title{ font-size:20px; font-weight:700; color:#111; margin:0 0 10px 0; padding-bottom:0; border-bottom:none; }
div[data-testid="stMultiSelect"], div[data-testid="stSelectbox"] { margin-top:-10px; }
h3 { margin-top:-15px; margin-bottom:10px; }
h4 { font-weight:700; color:#111; margin-top:0rem; margin-bottom:.5rem; }
hr { margin:1.5rem 0; background-color:#e0e0e0; }
</style>
""", unsafe_allow_html=True)
#endregion


#region [ 4-1. 공통 헬퍼들 ]
# =====================================================
def fmt(x: Optional[float], digits: int = 0, intlike: bool = False) -> str:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "–"
        if intlike:
            return f"{float(x):,.0f}"
        if digits <= 0:
            return f"{float(x):,.0f}"
        return f"{float(x):.{digits}f}"
    except Exception:
        return "–"

def kpi(col, title: str, value_str: str):
    with col:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-title">{title}</div>
              <div class="kpi-value">{value_str}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

# 데모 컬럼 표준 순서 (남/여 x 10~60대)
DEMO_COLS_ORDER = [
    "10대남성","10대여성",
    "20대남성","20대여성",
    "30대남성","30대여성",
    "40대남성","40대여성",
    "50대남성","50대여성",
    "60대남성","60대여성",
]

def get_episode_options(df: pd.DataFrame) -> List[str]:
    if "회차_numeric" in df.columns:
        eps = sorted(df["회차_numeric"].dropna().unique().astype(int).tolist())
        return [f"{e} 화" for e in eps] if eps else []
    if "회차" in df.columns:
        # "01화" 같은 문자열에서 숫자 추출
        eps = (
            df["회차"].astype(str)
            .str.extract(r"(\d+)", expand=False)
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        eps = sorted(eps)
        return [f"{e} 화" for e in eps] if eps else []
    return []
# =====================================================
#endregion


#region [ 5. 사이드바 네비게이션 ]
# =====================================================
current_page = get_current_page_default("Overview")
st.session_state["page"] = current_page

with st.sidebar:
    st.markdown('<hr style="margin:0px 0; border:1px solid #ccc;">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-logo">📊 DashBoard</div>', unsafe_allow_html=True)
    st.markdown('<hr style="margin:0px 0; border:1px solid #ccc;">', unsafe_allow_html=True)

    for key, label in NAV_ITEMS.items():
        active_class = "active" if current_page == key else ""
        st.markdown(
            f'<a class="nav-item {active_class}" href="?page={key}" target="_self">{label}</a>',
            unsafe_allow_html=True
        )
#endregion


#region [ 6. 공통 집계 유틸: KPI 계산 ]
# =====================================================
def _episode_col(df: pd.DataFrame) -> str:
    return "회차_numeric" if "회차_numeric" in df.columns else ("회차_num" if "회차_num" in df.columns else "회차")

def mean_of_ip_episode_sum(df: pd.DataFrame, metric_name: str, media=None) -> float | None:
    sub = df[(df["metric"] == metric_name)].copy()
    if media is not None:
        sub = sub[sub["매체"].isin(media)]
    if sub.empty:
        return None
    ep_col = _episode_col(sub)
    sub = sub.dropna(subset=[ep_col]).copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])
    ep_sum = sub.groupby(["IP", ep_col], as_index=False)["value"].sum()
    per_ip_mean = ep_sum.groupby("IP")["value"].mean()
    return float(per_ip_mean.mean()) if not per_ip_mean.empty else None

def mean_of_ip_episode_mean(df: pd.DataFrame, metric_name: str, media=None) -> float | None:
    sub = df[(df["metric"] == metric_name)].copy()
    if media is not None:
        sub = sub[sub["매체"].isin(media)]
    if sub.empty:
        return None
    ep_col = _episode_col(sub)
    sub = sub.dropna(subset=[ep_col]).copy()
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])
    ep_mean = sub.groupby(["IP", ep_col], as_index=False)["value"].mean()
    per_ip_mean = ep_mean.groupby("IP")["value"].mean()
    return float(per_ip_mean.mean()) if not per_ip_mean.empty else None

def mean_of_ip_sums(df: pd.DataFrame, metric_name: str, media=None) -> float | None:
    sub = df[(df["metric"] == metric_name)].copy()
    if media is not None:
        sub = sub[sub["매체"].isin(media)]
    if sub.empty:
        return None
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])
    per_ip_sum = sub.groupby("IP")["value"].sum()
    return float(per_ip_sum.mean()) if not per_ip_sum.empty else None
#endregion


#region [ 7. 공통 집계 유틸: 데모  ]
# =====================================================
def _gender_from_demo(s: str):
    s = str(s)
    if any(k in s for k in ["여", "F", "female", "Female"]): return "여"
    if any(k in s for k in ["남", "M", "male", "Male"]): return "남"
    return "기타"

def gender_from_demo(s: str):
    s = str(s)
    if any(k in s for k in ["여", "F", "female", "Female"]): return "여"
    if any(k in s for k in ["남", "M", "male", "Male"]):     return "남"
    return None

def _to_decade_label(x: str):
    m = re.search(r"\d+", str(x))
    if not m: return "기타"
    n = int(m.group(0))
    return f"{(n//10)*10}대"

def _decade_label_clamped(x: str):
    m = re.search(r"\d+", str(x))
    if not m: return None
    n = int(m.group(0))
    n = max(10, min(60, (n // 10) * 10))
    return f"{n}대"

def _decade_key(s: str):
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else 999

def _fmt_ep(n):
    try:
        return f"{int(n):02d}화"
    except Exception:
        return str(n)

COLOR_MALE = "#2a61cc"
COLOR_FEMALE = "#d93636"

def render_gender_pyramid(container, title: str, df_src: pd.DataFrame, height: int = 260):
    container.markdown(f"<div class='sec-title'>{title}</div>", unsafe_allow_html=True)
    if df_src.empty:
        container.info("표시할 데이터가 없습니다.")
        return
    df_demo = df_src.copy()
    df_demo["성별"] = df_demo["데모"].apply(_gender_from_demo)
    df_demo["연령대_대"] = df_demo["데모"].apply(_to_decade_label)
    df_demo = df_demo[df_demo["성별"].isin(["남","여"]) & df_demo["연령대_대"].notna()]
    if df_demo.empty:
        container.info("표시할 데모 데이터가 없습니다.")
        return
    order = sorted(df_demo["연령대_대"].unique().tolist(), key=_decade_key)
    pvt = (
        df_demo.groupby(["연령대_대","성별"])["value"]
               .sum()
               .unstack("성별")
               .reindex(order)
               .fillna(0)
    )
    male = -pvt.get("남", pd.Series(0, index=pvt.index))
    female = pvt.get("여", pd.Series(0, index=pvt.index))
    max_abs = float(max(male.abs().max(), female.max()) or 1)
    male_share = (male.abs() / male.abs().sum() * 100) if male.abs().sum() else male.abs()
    female_share = (female / female.sum() * 100) if female.sum() else female
    male_text = [f"{v:.1f}%" for v in male_share]
    female_text = [f"{v:.1f}%" for v in female_share]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=pvt.index, x=male, name="남",
        orientation="h", marker_color=COLOR_MALE,
        text=male_text, textposition="inside", insidetextanchor="end",
        textfont=dict(color="#ffffff", size=12),
        hovertemplate="연령대=%{y}<br>남성=%{customdata[0]:,.0f}명<br>성별내 비중=%{customdata[1]:.1f}%<extra></extra>",
        customdata=np.column_stack([male.abs(), male_share])
    ))
    fig.add_trace(go.Bar(
        y=pvt.index, x=female, name="여",
        orientation="h", marker_color=COLOR_FEMALE,
        text=female_text, textposition="inside", insidetextanchor="start",
        textfont=dict(color="#ffffff", size=12),
        hovertemplate="연령대=%{y}<br>여성=%{customdata[0]:,.0f}명<br>성별내 비중=%{customdata[1]:.1f}%<extra></extra>",
        customdata=np.column_stack([female, female_share])
    ))
    fig.update_layout(
        barmode="overlay", height=height, margin=dict(l=8, r=8, t=10, b=8),
        legend_title=None, bargap=0.15, bargroupgap=0.05
    )
    fig.update_yaxes(categoryorder="array", categoryarray=order, title=None, tickfont=dict(size=12), fixedrange=True)
    fig.update_xaxes(
        range=[-max_abs*1.05, max_abs*1.05],
        title=None, showticklabels=False, showgrid=False, zeroline=True, zerolinewidth=1, zerolinecolor="#888",
        fixedrange=True
    )
    container.plotly_chart(fig, use_container_width=True,
                           config={"scrollZoom": False, "staticPlot": False, "displayModeBar": False})
#endregion


#region [ 8. 페이지 1: Overview ]
# =====================================================
def render_overview():
    df = load_data()
    filter_cols = st.columns(4)
    with filter_cols[0]:
        st.markdown("### 📊 Overview")
    with filter_cols[1]:
        prog_sel = st.multiselect(
            "편성",
            sorted(df["편성"].dropna().unique().tolist()),
            placeholder="편성 선택",
            label_visibility="collapsed"
        )
    if "방영시작일" in df.columns and df["방영시작일"].notna().any():
        date_col_for_filter = "방영시작일"
    else:
        date_col_for_filter = "주차시작일"
    date_series = df[date_col_for_filter].dropna()
    if not date_series.empty:
        all_years = sorted(date_series.dt.year.unique().tolist(), reverse=True)
        all_months = sorted(date_series.dt.month.unique().tolist())
        with filter_cols[2]:
            year_sel = st.multiselect("연도", all_years, placeholder="연도 선택", label_visibility="collapsed")
        with filter_cols[3]:
            month_sel = st.multiselect("월", all_months, placeholder="월 선택", label_visibility="collapsed")
    else:
        year_sel = None; month_sel = None
    f = df.copy()
    if prog_sel: f = f[f["편성"].isin(prog_sel)]
    if year_sel and date_col_for_filter in f.columns:
        f = f[f[date_col_for_filter].dt.year.isin(year_sel)]
    if month_sel and date_col_for_filter in f.columns:
        f = f[f[date_col_for_filter].dt.month.isin(month_sel)]

    def avg_of_ip_means(metric_name: str): return mean_of_ip_episode_mean(f, metric_name)
    def avg_of_ip_tving_epSum_mean(media_name: str): return mean_of_ip_episode_sum(f, "시청인구", [media_name])
    def avg_of_ip_sums(metric_name: str): return mean_of_ip_sums(f, metric_name)
    def count_ip_with_min1(metric_name: str):
        sub = f[f["metric"] == metric_name]
        if sub.empty: return 0
        ip_min = sub.groupby("IP")["value"].min()
        return (ip_min == 1).sum()
    def count_anchor_dramas():
        sub = f[f["metric"]=="T시청률"].groupby(["IP","편성"])["value"].mean().reset_index()
        mon_tue = sub[(sub["편성"]=="월화") & (sub["value"]>2)].shape[0]
        sat_sun = sub[(sub["편성"]=="토일") & (sub["value"]>3)].shape[0]
        return mon_tue + sat_sun

    st.caption('▶ IP별 평균')
    c1, c2, c3, c4, c5 = st.columns(5)
    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)
    c6, c7, c8, c9, c10 = st.columns(5)

    t_rating   = avg_of_ip_means("T시청률")
    h_rating   = avg_of_ip_means("H시청률")
    tving_live = avg_of_ip_tving_epSum_mean("TVING LIVE")
    tving_quick= avg_of_ip_tving_epSum_mean("TVING QUICK")
    tving_vod  = avg_of_ip_tving_epSum_mean("TVING VOD")
    digital_view = avg_of_ip_sums("조회수")
    digital_buzz = avg_of_ip_sums("언급량")
    fundex_top1 = count_ip_with_min1("F_Total")
    anchor_total = count_anchor_dramas()

    kpi(c1, "🎯 타깃 시청률", fmt(t_rating, digits=3))
    kpi(c2, "🏠 가구 시청률", fmt(h_rating, digits=3))
    kpi(c3, "📺 티빙 LIVE", fmt(tving_live, intlike=True))
    kpi(c4, "⚡ 티빙 QUICK", fmt(tving_quick, intlike=True))
    kpi(c5, "▶️ 티빙 VOD", fmt(tving_vod, intlike=True))
    kpi(c6, "👀 디지털 조회", fmt(digital_view, intlike=True))
    kpi(c7, "💬 디지털 언급량", fmt(digital_buzz, intlike=True))
    kpi(c8, "🥇 펀덱스 1위", f"{fundex_top1}작품")
    kpi(c9, "⚓ 앵커드라마", f"{anchor_total}작품")
    kpi(c10, "　", "　")

    st.divider()

    df_trend = f[f["metric"]=="시청인구"].copy()
    tv_weekly = df_trend[df_trend["매체"]=="TV"].groupby("주차시작일")["value"].sum()
    tving_livequick_weekly = df_trend[df_trend["매체"].isin(["TVING LIVE","TVING QUICK"])].groupby("주차시작일")["value"].sum()
    tving_vod_weekly = df_trend[df_trend["매체"]=="TVING VOD"].groupby("주차시작일")["value"].sum()

    df_bar = pd.DataFrame({
        "주차시작일": sorted(set(tv_weekly.index) | set(tving_livequick_weekly.index) | set(tving_vod_weekly.index))
    })
    df_bar["TV 본방"] = df_bar["주차시작일"].map(tv_weekly).fillna(0)
    df_bar["티빙 본방"] = df_bar["주차시작일"].map(tving_livequick_weekly).fillna(0)
    df_bar["티빙 VOD"] = df_bar["주차시작일"].map(tving_vod_weekly).fillna(0)

    df_long = df_bar.melt(id_vars="주차시작일", value_vars=["TV 본방","티빙 본방","티빙 VOD"], var_name="구분", value_name="시청자수")
    fig = px.bar(df_long, x="주차시작일", y="시청자수", color="구분", text="시청자수",
                 title="📊 주차별 시청자수 (TV 본방 / 티빙 본방 / 티빙 VOD, 누적)",
                 color_discrete_map={"TV 본방":"#1f77b4","티빙 본방":"#d62728","티빙 VOD":"#ff7f7f"})
    fig.update_layout(xaxis_title=None, yaxis_title=None, barmode="stack", legend_title="구분", title_font=dict(size=20))
    fig.update_traces(texttemplate='%{text:,.0f}', textposition="inside")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### 🎬 주요 작품 성과")
    df_perf = (
        f.groupby("IP")
        .agg(
            타깃시청률=("value", lambda x: x[f.loc[x.index, "metric"]=="T시청률"].mean()),
            가구시청률=("value", lambda x: x[f.loc[x.index, "metric"]=="H시청률"].mean()),
            티빙LIVE=("value", lambda x: x[(f.loc[x.index, "매체"]=="TVING LIVE") & (f.loc[x.index,"metric"]=="시청인구")].sum()),
            티빙QUICK=("value", lambda x: x[(f.loc[x.index, "매체"]=="TVING QUICK") & (f.loc[x.index,"metric"]=="시청인구")].sum()),
            티빙VOD_6Days=("value", lambda x: x[(f.loc[x.index, "매체"]=="TVING VOD") & (f.loc[x.index,"metric"]=="시청인구")].sum()),
            디지털조회수=("value", lambda x: x[(f.loc[x.index,"metric"]=="조회수")].sum()),
            디지털언급량=("value", lambda x: x[(f.loc[x.index,"metric"]=="언급량")].sum()),
            화제성순위=("value", lambda x: x[(f.loc[x.index,"metric"]=="F_Total")].min())
        )
        .reset_index()
    ).sort_values("타깃시청률", ascending=False)

    fmt_fixed3 = JsCode("""function(params){ if (params.value == null || isNaN(params.value)) return ''; return Number(params.value).toFixed(3);}""")
    fmt_thousands = JsCode("""function(params){ if (params.value == null || isNaN(params.value)) return ''; return Math.round(params.value).toLocaleString();}""")
    fmt_rank = JsCode("""function(params){ if (params.value == null || isNaN(params.value)) return ''; return Math.round(params.value) + '위';}""")

    gb = GridOptionsBuilder.from_dataframe(df_perf)
    gb.configure_default_column(sortable=True, resizable=True, filter=False, cellStyle={'textAlign': 'center'}, headerClass='centered-header')
    gb.configure_grid_options(rowHeight=34, suppressMenuHide=True, domLayout='normal')
    gb.configure_column('IP', header_name='IP', cellStyle={'textAlign':'left'})
    gb.configure_column('타깃시청률', valueFormatter=fmt_fixed3, sort='desc')
    gb.configure_column('가구시청률', valueFormatter=fmt_fixed3)
    gb.configure_column('티빙LIVE', valueFormatter=fmt_thousands)
    gb.configure_column('티빙QUICK', valueFormatter=fmt_thousands)
    gb.configure_column('티빙VOD_6Days', valueFormatter=fmt_thousands)
    gb.configure_column('디지털조회수', valueFormatter=fmt_thousands)
    gb.configure_column('디지털언급량', valueFormatter=fmt_thousands)
    gb.configure_column('화제성순위', valueFormatter=fmt_rank)
    grid_options = gb.build()
    AgGrid(df_perf, gridOptions=grid_options, theme="streamlit", height=300, fit_columns_on_grid_load=True,
           update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True)
#endregion


#region [ 9. 페이지 2: IP 성과 자세히보기 ]
# =====================================================
def render_ip_detail():
    df_full = load_data()
    filter_cols = st.columns([3, 2, 2])
    with filter_cols[0]:
        st.markdown("### 📈 IP 성과 자세히보기")
    ip_options = sorted(df_full["IP"].dropna().unique().tolist())
    with filter_cols[1]:
        ip_selected = st.selectbox("IP (단일선택)", ip_options, index=0 if ip_options else None,
                                   placeholder="IP 선택", label_visibility="collapsed")
    with filter_cols[2]:
        selected_group_criteria = st.multiselect("비교 그룹 기준", ["동일 편성", "방영 연도"], default=["동일 편성"],
                                                 placeholder="비교 그룹 기준", label_visibility="collapsed",
                                                 key="ip_detail_group")

    if "방영시작일" in df_full.columns and df_full["방영시작일"].notna().any():
        date_col_for_filter = "방영시작일"
    else:
        date_col_for_filter = "주차시작일"

    f = df_full[df_full["IP"] == ip_selected].copy()
    if "회차_numeric" in f.columns:
        f["회차_num"] = pd.to_numeric(f["회차_numeric"], errors="coerce")
    else:
        f["회차_num"] = pd.to_numeric(f["회차"].str.extract(r"(\d+)", expand=False), errors="coerce")

    def _week_to_num(x: str):
        m = re.search(r"-?\d+", str(x))
        return int(m.group(0)) if m else None
    has_week_col = "주차" in f.columns
    if has_week_col:
        f["주차_num"] = f["주차"].apply(_week_to_num)

    try:
        sel_prog = f["편성"].dropna().mode().iloc[0]
    except Exception:
        sel_prog = None
    try:
        sel_year = f[date_col_for_filter].dropna().dt.year.mode().iloc[0] if date_col_for_filter in f.columns and not f[date_col_for_filter].dropna().empty else None
    except Exception:
        sel_year = None

    base = df_full.copy()
    group_name_parts = []
    if "동일 편성" in selected_group_criteria:
        if sel_prog:
            base = base[base["편성"] == sel_prog]
            group_name_parts.append(f"'{sel_prog}'")
        else:
            st.warning(f"'{ip_selected}'의 편성 정보가 없어 '동일 편성' 기준은 제외됩니다.", icon="⚠️")
    if "방영 연도" in selected_group_criteria:
        if sel_year:
            base = base[base[date_col_for_filter].dt.year == sel_year]
            group_name_parts.append(f"{int(sel_year)}년")
        else:
            st.warning(f"'{ip_selected}'의 연도 정보가 없어 '방영 연도' 기준은 제외됩니다.", icon="⚠️")
    if not group_name_parts and selected_group_criteria:
        st.warning("그룹핑 기준 정보 부족. 전체 데이터와 비교합니다.", icon="⚠️")
        group_name_parts.append("전체")
        base = df_full.copy()
    elif not group_name_parts:
        group_name_parts.append("전체")
        base = df_full.copy()
    prog_label = " & ".join(group_name_parts) + " 평균"

    if "회차_numeric" in base.columns:
        base["회차_num"] = pd.to_numeric(base["회차_numeric"], errors="coerce")
    else:
        base["회차_num"] = pd.to_numeric(base["회차"].str.extract(r"(\d+)", expand=False), errors="coerce")

    st.markdown(f"<h2 style='text-align:center; color:#333;'>📺 {ip_selected} 성과 상세 리포트</h2>", unsafe_allow_html=True)
    st.markdown("---")

    val_T = mean_of_ip_episode_mean(f, "T시청률")
    val_H = mean_of_ip_episode_mean(f, "H시청률")
    val_live  = mean_of_ip_episode_sum(f, "시청인구", ["TVING LIVE"])
    val_quick = mean_of_ip_episode_sum(f, "시청인구", ["TVING QUICK"])
    val_vod   = mean_of_ip_episode_sum(f, "시청인구", ["TVING VOD"])
    val_buzz  = mean_of_ip_sums(f, "언급량")
    val_view  = mean_of_ip_sums(f, "조회수")

    base_T = mean_of_ip_episode_mean(base, "T시청률")
    base_H = mean_of_ip_episode_mean(base, "H시청률")
    base_live  = mean_of_ip_episode_sum(base, "시청인구", ["TVING LIVE"])
    base_quick = mean_of_ip_episode_sum(base, "시청인구", ["TVING QUICK"])
    base_vod   = mean_of_ip_episode_sum(base, "시청인구", ["TVING VOD"])
    base_buzz  = mean_of_ip_sums(base, "언급량")
    base_view  = mean_of_ip_sums(base, "조회수")

    def _series_ip_metric(base_df: pd.DataFrame, metric_name: str, mode: str = "mean", media: List[str] | None = None):
        sub = base_df[base_df["metric"] == metric_name].copy()
        if media is not None:
            sub = sub[sub["매체"].isin(media)]
        if sub.empty:
            return pd.Series(dtype=float)
        if mode == "mean":
            ep_col = _episode_col(sub)
            sub = sub.dropna(subset=[ep_col])
            ep_mean = sub.groupby(["IP", ep_col], as_index=False)["value"].mean()
            s = ep_mean.groupby("IP")["value"].mean()
        elif mode == "sum":
            s = sub.groupby("IP")["value"].sum()
        elif mode == "ep_sum_mean":
            ep_col = _episode_col(sub)
            sub = sub.dropna(subset=[ep_col])
            ep_sum = sub.groupby(["IP", ep_col], as_index=False)["value"].sum()
            s = ep_sum.groupby("IP")["value"].mean()
        else:
            raise ValueError("unknown mode")
        return s.dropna()

    def _rank_within_program(base_df: pd.DataFrame, metric_name: str, ip_name: str, value: float,
                             mode: str = "mean", media: List[str] | None = None):
        s = _series_ip_metric(base_df, metric_name, mode=mode, media=media)
        if s.empty or value is None or pd.isna(value):
            return (None, 0)
        ranks = s.rank(method="min", ascending=False)
        if ip_name not in ranks.index:
            r = int((s > value).sum() + 1)
        else:
            r = int(ranks.loc[ip_name])
        return (r, int(s.shape[0]))

    def _pct_color(val, base_val):
        if val is None or pd.isna(val) or base_val in (None, 0) or pd.isna(base_val):
            return "#888"
        pct = (val / base_val) * 100
        return "#d93636" if pct > 100 else ("#2a61cc" if pct < 100 else "#444")

    def sublines_html(prog_label: str, rank_tuple: tuple, val, base_val):
        rnk, total = rank_tuple if rank_tuple else (None, 0)
        rank_html = "<span class='kpi-sublabel'>{} 內</span> <span class='kpi-substrong'>{}</span>".format(
            prog_label.replace(" 평균", ""), (f"{rnk}위" if (rnk is not None and total>0) else "–위")
        )
        if val is None or pd.isna(val) or base_val in (None,0) or pd.isna(base_val):
            pct_txt = "–"; col = "#888"
        else:
            pct = (val / base_val) * 100
            pct_txt = f"{pct:.0f}%"; col = _pct_color(val, base_val)
        pct_html = "<span class='kpi-sublabel'>{} 대비</span> <span class='kpi-subpct' style='color:{};'>{}</span>".format(
            prog_label, col, pct_txt
        )
        return f"<div class='kpi-subwrap'>{rank_html}<br/>{pct_html}</div>"

    def kpi_with_rank(col, title, value, base_val, rank_tuple, prog_label, intlike=False, digits=3):
        with col:
            main = f"{(f'{value:,.0f}' if intlike else f'{value:.{digits}f}')}" if value is not None and not pd.isna(value) else "–"
            st.markdown(
                f"<div class='kpi-card'>"
                f"<div class='kpi-title'>{title}</div>"
                f"<div class='kpi-value'>{main}</div>"
                f"{sublines_html(prog_label, rank_tuple, value, base_val)}"
                f"</div>",
                unsafe_allow_html=True
            )

    rk_T     = _rank_within_program(base, "T시청률", ip_selected, val_T,   mode="mean", media=None)
    rk_H     = _rank_within_program(base, "H시청률", ip_selected, val_H,   mode="mean", media=None)
    rk_live  = _rank_within_program(base, "시청인구", ip_selected, val_live,  mode="ep_sum_mean",  media=["TVING LIVE"])
    rk_quick = _rank_within_program(base, "시청인구", ip_selected, val_quick, mode="ep_sum_mean",  media=["TVING QUICK"])
    rk_vod   = _rank_within_program(base, "시청인구", ip_selected, val_vod,   mode="ep_sum_mean",  media=["TVING VOD"])
    rk_buzz  = _rank_within_program(base, "언급량",   ip_selected, val_buzz,  mode="sum",          media=None)
    rk_view  = _rank_within_program(base, "조회수",   ip_selected, val_view,  mode="sum",          media=None)

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    kpi_with_rank(c1, "🎯 타깃시청률",    val_T,   base_T,   rk_T,     prog_label, intlike=False, digits=3)
    kpi_with_rank(c2, "🏠 가구시청률",    val_H,   base_H,   rk_H,     prog_label, intlike=False, digits=3)
    kpi_with_rank(c3, "📺 티빙 LIVE",     val_live,  base_live,  rk_live,  prog_label, intlike=True)
    kpi_with_rank(c4, "⚡ 티빙 QUICK",    val_quick, base_quick, rk_quick, prog_label, intlike=True)
    kpi_with_rank(c5, "▶️ 티빙 VOD",      val_vod,   base_vod,   rk_vod,   prog_label, intlike=True)
    kpi_with_rank(c6, "💬 총 언급량",     val_buzz,  base_buzz,  rk_buzz,  prog_label, intlike=True)
    kpi_with_rank(c7, "👀 디지털 조회수", val_view,  base_view,  rk_view,  prog_label, intlike=True)

    st.divider()
    chart_h = 260
    common_cfg = {"scrollZoom": False, "staticPlot": False, "displayModeBar": False}
    cA, cB = st.columns(2)
    with cA:
        st.markdown("<div class='sec-title'>📈 시청률 추이 (회차별)</div>", unsafe_allow_html=True)
        rsub = f[f["metric"].isin(["T시청률","H시청률"])].dropna(subset=["회차","회차_num"]).copy()
        rsub = rsub.sort_values("회차_num")
        if not rsub.empty:
            ep_order = rsub[["회차","회차_num"]].drop_duplicates().sort_values("회차_num")["회차"].tolist()
            t_series = rsub[rsub["metric"]=="T시청률"].groupby("회차", as_index=False)["value"].mean()
            h_series = rsub[rsub["metric"]=="H시청률"].groupby("회차", as_index=False)["value"].mean()
            ymax = pd.concat([t_series["value"], h_series["value"]]).max()
            y_upper = float(ymax) * 1.4 if pd.notna(ymax) else None
            fig_rate = go.Figure()
            fig_rate.add_trace(go.Scatter(x=h_series["회차"], y=h_series["value"], mode="lines+markers+text", name="가구시청률",
                                          text=[f"{v:.2f}" for v in h_series["value"]], textposition="top center"))
            fig_rate.add_trace(go.Scatter(x=t_series["회차"], y=t_series["value"], mode="lines+markers+text", name="타깃시청률",
                                          text=[f"{v:.2f}" for v in t_series["value"]], textposition="top center"))
            fig_rate.update_xaxes(categoryorder="array", categoryarray=ep_order, title=None, fixedrange=True)
            fig_rate.update_yaxes(title=None, fixedrange=True, range=[0, y_upper] if y_upper else None)
            fig_rate.update_layout(legend_title=None, height=chart_h, margin=dict(l=8,r=8,t=10,b=8))
            st.plotly_chart(fig_rate, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 시청률 데이터가 없습니다.")
    with cB:
        st.markdown("<div class='sec-title'>📊 TVING 시청자 추이 (회차별)</div>", unsafe_allow_html=True)
        t_keep = ["TVING LIVE", "TVING QUICK", "TVING VOD"]
        tsub = f[(f["metric"]=="시청인구") & (f["매체"].isin(t_keep))].dropna(subset=["회차","회차_num"]).copy()
        tsub = tsub.sort_values("회차_num")
        if not tsub.empty:
            ep_order = tsub[["회차","회차_num"]].drop_duplicates().sort_values("회차_num")["회차"].tolist()
            pvt = tsub.pivot_table(index="회차", columns="매체", values="value", aggfunc="sum").fillna(0)
            pvt = pvt.reindex(ep_order)
            fig_tving = go.Figure()
            for col in [c for c in ["TVING LIVE","TVING QUICK","TVING VOD"] if c in pvt.columns]:
                fig_tving.add_trace(go.Bar(name=col, x=pvt.index, y=pvt[col], text=None))
            fig_tving.update_layout(barmode="stack", legend_title=None, bargap=0.15, bargroupgap=0.05,
                                    height=chart_h, margin=dict(l=8,r=8,t=10,b=8))
            fig_tving.update_xaxes(categoryorder="array", categoryarray=ep_order, title=None, fixedrange=True)
            fig_tving.update_yaxes(title=None, fixedrange=True)
            st.plotly_chart(fig_tving, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 TVING 시청자 데이터가 없습니다.")

    cC, cD = st.columns(2)
    with cC:
        st.markdown("<div class='sec-title'>▶ 디지털 조회수</div>", unsafe_allow_html=True)
        dview = f[f["metric"]=="조회수"].copy()
        if not dview.empty:
            if has_week_col and dview["주차"].notna().any():
                order = (dview[["주차","주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
                pvt = dview.pivot_table(index="주차", columns="매체", values="value", aggfunc="sum").fillna(0)
                pvt = pvt.reindex(order)
                x_vals = pvt.index.tolist(); use_category = True
            else:
                pvt = (dview.pivot_table(index="주차시작일", columns="매체", values="value", aggfunc="sum").sort_index().fillna(0))
                x_vals = pvt.index.tolist(); use_category = False
            fig_view = go.Figure()
            for col in pvt.columns:
                fig_view.add_trace(go.Bar(name=col, x=x_vals, y=pvt[col], text=None))
            fig_view.update_layout(barmode="stack", legend_title=None, bargap=0.15, bargroupgap=0.05,
                                   height=chart_h, margin=dict(l=8,r=8,t=10,b=8))
            if use_category:
                fig_view.update_xaxes(categoryorder="array", categoryarray=x_vals, title=None, fixedrange=True)
            else:
                fig_view.update_xaxes(title=None, fixedrange=True)
            fig_view.update_yaxes(title=None, fixedrange=True)
            st.plotly_chart(fig_view, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 조회수 데이터가 없습니다.")
    with cD:
        st.markdown("<div class='sec-title'>💬 디지털 언급량</div>", unsafe_allow_html=True)
        dbuzz = f[f["metric"]=="언급량"].copy()
        if not dbuzz.empty:
            if has_week_col and dbuzz["주차"].notna().any():
                order = (dbuzz[["주차","주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
                pvt = dbuzz.pivot_table(index="주차", columns="매체", values="value", aggfunc="sum").fillna(0)
                pvt = pvt.reindex(order)
                x_vals = pvt.index.tolist(); use_category = True
            else:
                pvt = (dbuzz.pivot_table(index="주차시작일", columns="매체", values="value", aggfunc="sum").sort_index().fillna(0))
                x_vals = pvt.index.tolist(); use_category = False
            fig_buzz = go.Figure()
            for col in pvt.columns:
                fig_buzz.add_trace(go.Bar(name=col, x=x_vals, y=pvt[col], text=None))
            fig_buzz.update_layout(barmode="stack", legend_title=None, bargap=0.15, bargroupgap=0.05,
                                   height=chart_h, margin=dict(l=8,r=8,t=10,b=8))
            if use_category:
                fig_buzz.update_xaxes(categoryorder="array", categoryarray=x_vals, title=None, fixedrange=True)
            else:
                fig_buzz.update_xaxes(title=None, fixedrange=True)
            fig_buzz.update_yaxes(title=None, fixedrange=True)
            st.plotly_chart(fig_buzz, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 언급량 데이터가 없습니다.")

    cE, cF = st.columns(2)
    with cE:
        st.markdown("<div class='sec-title'>🔥 화제성 지수</div>", unsafe_allow_html=True)
        fdx = f[f["metric"]=="F_Total"].copy()
        if not fdx.empty:
            fdx["순위"] = pd.to_numeric(fdx["value"], errors="coerce").round().astype("Int64")
            if has_week_col and fdx["주차"].notna().any():
                order = (fdx[["주차","주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
                s = fdx.groupby("주차", as_index=True)["순위"].min().reindex(order).dropna()
                x_vals = s.index.tolist(); use_category = True
            else:
                s = fdx.set_index("주차시작일")["순위"].sort_index().dropna()
                x_vals = s.index.tolist(); use_category = False
            y_min, y_max = 0.5, 10
            labels = [f"{int(v)}위" for v in s.values]
            text_positions = ["bottom center" if (v <= 1.5) else "top center" for v in s.values]
            fig_fx = go.Figure()
            fig_fx.add_trace(go.Scatter(
                x=x_vals, y=s.values, mode="lines+markers+text", name="화제성 순위",
                text=labels, textposition=text_positions, textfont=dict(size=12, color="#111"),
                cliponaxis=False, marker=dict(size=8)
            ))
            fig_fx.update_yaxes(autorange=False, range=[y_max, y_min], dtick=1, title=None, fixedrange=True)
            if use_category:
                fig_fx.update_xaxes(categoryorder="array", categoryarray=x_vals, title=None, fixedrange=True)
            else:
                fig_fx.update_xaxes(title=None, fixedrange=True)
            fig_fx.update_layout(legend_title=None, height=chart_h, margin=dict(l=8, r=8, t=10, b=8))
            st.plotly_chart(fig_fx, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 화제성 지수 데이터가 없습니다.")
    with cF:
        st.markdown(f"<div style='height:{chart_h}px'></div>", unsafe_allow_html=True)

    cG, cH = st.columns(2)
    tv_demo = f[(f["매체"]=="TV") & (f["metric"]=="시청인구") & f["데모"].notna()].copy()
    render_gender_pyramid(cG, "🎯 TV 데모 분포", tv_demo, height=260)
    t_keep = ["TVING LIVE", "TVING QUICK", "TVING VOD"]
    tving_demo = f[(f["매체"].isin(t_keep)) & (f["metric"]=="시청인구") & f["데모"].notna()].copy()
    render_gender_pyramid(cH, "📺 TVING 데모 분포", tving_demo, height=260)

    st.divider()
    st.markdown("#### 👥 데모분석 상세 표")

    def _build_demo_table_numeric(df_src: pd.DataFrame, medias: List[str]) -> pd.DataFrame:
        sub = df_src[(df_src["metric"] == "시청인구") & (df_src["데모"].notna()) & (df_src["매체"].isin(medias))].copy()
        if sub.empty:
            return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)
        sub["성별"] = sub["데모"].apply(_gender_from_demo)
        sub["연령대_대"] = sub["데모"].apply(_decade_label_clamped)
        sub = sub[sub["성별"].isin(["남","여"]) & sub["연령대_대"].notna()].copy()
        sub = sub.dropna(subset=["회차_num"])
        sub["회차_num"] = sub["회차_num"].astype(int)
        sub["라벨"] = sub.apply(lambda r: f"{r['연령대_대']}{'남성' if r['성별']=='남' else '여성'}", axis=1)
        pvt = sub.pivot_table(index="회차_num", columns="라벨", values="value", aggfunc="sum").fillna(0)
        for c in DEMO_COLS_ORDER:
            if c not in pvt.columns:
                pvt[c] = 0
        pvt = pvt[DEMO_COLS_ORDER].sort_index()
        pvt.insert(0, "회차", pvt.index.map(_fmt_ep))
        return pvt.reset_index(drop=True)

    diff_renderer = JsCode("""
    function(params){
      const api = params.api;
      const colId = params.column.getColId();
      const rowIndex = params.node.rowIndex;
      const val = Number(params.value || 0);
      if (colId === "회차") return params.value;
      let arrow = "";
      if (rowIndex > 0) {
        const prev = api.getDisplayedRowAtIndex(rowIndex - 1);
        if (prev && prev.data && prev.data[colId] != null) {
          const pv = Number(prev.data[colId] || 0);
          if (val > pv) arrow = "🔺";
          else if (val < pv) arrow = "▾";
        }
      }
      const txt = Math.round(val).toLocaleString();
      return arrow + txt;
    }
    """)
    _js_demo_cols = "[" + ",".join([f'"{c}"' for c in DEMO_COLS_ORDER]) + "]"
    cell_style_renderer = JsCode(f"""
    function(params){{
      const field = params.colDef.field;
      if (field === "회차") {{
        return {{'text-align':'left','font-weight':'600','background-color':'#fff'}};
      }}
      const COLS = {_js_demo_cols};
      let rowVals = [];
      for (let k of COLS) {{
        const v = Number((params.data && params.data[k] != null) ? params.data[k] : NaN);
        if (!isNaN(v)) rowVals.push(v);
      }}
      let bg = '#ffffff';
      if (rowVals.length > 0) {{
        const v = Number(params.value || 0);
        const mn = Math.min.apply(null, rowVals);
        const mx = Math.max.apply(null, rowVals);
        let norm = 0.5;
        if (mx > mn) norm = (v - mn) / (mx - mn);
        const alpha = 0.12 + 0.45 * Math.max(0, Math.min(1, norm));
        bg = 'rgba(30,90,255,' + alpha.toFixed(3) + ')';
      }}
      return {{
        'background-color': bg,
        'text-align': 'right',
        'padding': '2px 4px',
        'font-weight': '500'
      }};
    }}
    """)
    def _render_aggrid_table(df_numeric: pd.DataFrame, title: str, height: int = 320):
        st.markdown(f"###### {title}")
        if df_numeric.empty:
            st.info("표시할 데이터가 없습니다.")
            return
        gb = GridOptionsBuilder.from_dataframe(df_numeric)
        gb.configure_grid_options(rowHeight=34, suppressMenuHide=True, domLayout='normal')
        gb.configure_default_column(sortable=False, resizable=True, filter=False,
                                    cellStyle={'textAlign': 'right'}, headerClass='centered-header bold-header')
        gb.configure_column("회차", header_name="회차", cellStyle={'textAlign': 'left'})
        for c in [col for col in df_numeric.columns if col != "회차"]:
            gb.configure_column(c, header_name=c, cellRenderer=diff_renderer, cellStyle=cell_style_renderer)
        grid_options = gb.build()
        AgGrid(df_numeric, gridOptions=grid_options, theme="streamlit", height=height, fit_columns_on_grid_load=True,
               update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True)
    tv_numeric = _build_demo_table_numeric(f, ["TV"])
    _render_aggrid_table(tv_numeric, "📺 TV (시청자수)")
    tving_numeric = _build_demo_table_numeric(f, ["TVING LIVE", "TVING QUICK", "TVING VOD"])
    _render_aggrid_table(tving_numeric, "▶︎ TVING 합산 (LIVE/QUICK/VOD) 시청자수")
#endregion


#region [ 10. 페이지 3: IP간 데모분석 (히트맵 포함) ]
# =====================================================
index_value_formatter = JsCode("""
function(params) {
    const indexValue = params.value;
    if (indexValue == null || (typeof indexValue !== 'number')) return 'N/A';
    if (indexValue === 999) { return 'INF'; }
    const roundedIndex = Math.round(indexValue);
    let arrow = '';
    if (roundedIndex > 5) { arrow = ' ▲'; }
    else if (roundedIndex < -5) { arrow = ' ▼'; }
    let sign = roundedIndex > 0 ? '+' : '';
    if (roundedIndex === 0) sign = '';
    return sign + roundedIndex + '%' + arrow;
}""")
index_cell_style = JsCode("""
function(params) {
    const indexValue = params.value;
    let color = '#333';
    let fontWeight = '500';
    if (indexValue == null || (typeof indexValue !== 'number')) { color = '#888'; }
    else if (indexValue === 999) { color = '#888'; }
    else {
        if (indexValue > 5) { color = '#d93636'; }
        else if (indexValue < -5) { color = '#2a61cc'; }
    }
    return {'color': color, 'font-weight': fontWeight};
}""")

def render_index_table(df_index: pd.DataFrame, title: str, height: int = 400):
    st.markdown(f"###### {title}")
    if df_index.empty: st.info("비교할 데이터가 없습니다."); return
    gb = GridOptionsBuilder.from_dataframe(df_index)
    gb.configure_grid_options(rowHeight=34, suppressMenuHide=True, domLayout='normal')
    gb.configure_default_column(sortable=False, resizable=True, filter=False,
                                cellStyle={'textAlign': 'center'}, headerClass='centered-header bold-header')
    gb.configure_column("회차", header_name="회차", cellStyle={'textAlign': 'left'}, pinned='left', width=70)
    for c in [col for col in df_index.columns if col != "회차" and not c.endswith(('_base', '_comp'))]:
        gb.configure_column(c, header_name=c.replace("남성","M").replace("여성","F"),
                            valueFormatter=index_value_formatter, cellStyle=index_cell_style, width=80)
    for c in [col for col in df_index.columns if c.endswith(('_base', '_comp'))]:
        gb.configure_column(c, hide=True)
    grid_options = gb.build()
    AgGrid(df_index, gridOptions=grid_options, theme="streamlit", height=height,
           update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True, enable_enterprise_modules=False)

def render_heatmap(df_plot: pd.DataFrame, title: str):
    st.markdown(f"###### {title}")
    if df_plot.empty:
        st.info("비교할 데이터가 없습니다.")
        return
    df_heatmap = df_plot.set_index("회차")
    cols_to_drop = [c for c in df_heatmap.columns if c.endswith(('_base', '_comp'))]
    df_heatmap = df_heatmap.drop(columns=cols_to_drop)
    valid_values = df_heatmap.replace(999, np.nan).values
    if pd.isna(valid_values).all():
         v_min, v_max = -10.0, 10.0
    else:
         v_min = np.nanmin(valid_values); v_max = np.nanmax(valid_values)
         if pd.isna(v_min): v_min = 0.0
         if pd.isna(v_max): v_max = 0.0
    abs_max = max(abs(v_min), abs(v_max), 10.0)
    fig = px.imshow(
        df_heatmap, text_auto=False, aspect="auto",
        color_continuous_scale='RdBu_r', range_color=[-abs_max, abs_max], color_continuous_midpoint=0
    )
    text_template_df = df_heatmap.applymap(lambda x: "INF" if x == 999 else (f"{x:+.0f}%" if pd.notna(x) else ""))
    fig.update_traces(
        text=text_template_df.values, texttemplate="%{text}",
        hovertemplate="회차: %{y}<br>데모: %{x}<br>증감: %{text}",
        textfont=dict(size=10, color="black")
    )
    fig.update_layout(height=max(520, len(df_heatmap.index) * 46), xaxis_title=None, yaxis_title=None, xaxis=dict(side="top"))
    st.plotly_chart(fig, use_container_width=True)

def get_avg_demo_pop_by_episode(df_src: pd.DataFrame, medias: List[str]) -> pd.DataFrame:
    sub = df_src[(df_src["metric"] == "시청인구") & (df_src["데모"].notna()) & (df_src["매체"].isin(medias))].copy()
    if sub.empty:
        return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])
    sub["성별"] = sub["데모"].apply(gender_from_demo)
    sub["연령대_대"] = sub["데모"].apply(_decade_label_clamped)
    sub = sub[sub["성별"].isin(["남", "여"]) & sub["연령대_대"].notna()].copy()
    sub = sub.dropna(subset=["회차_numeric"])
    sub["회차_num"] = sub["회차_numeric"].astype(int)
    sub["라벨"] = sub.apply(lambda r: f"{r['연령대_대']}{'남성' if r['성별']=='남' else '여성'}", axis=1)
    ip_ep_demo_sum = sub.groupby(["IP", "회차_num", "라벨"])["value"].sum().reset_index()
    ep_demo_mean = ip_ep_demo_sum.groupby(["회차_num", "라벨"])["value"].mean().reset_index()
    pvt = ep_demo_mean.pivot_table(index="회차_num", columns="라벨", values="value").fillna(0)
    for c in DEMO_COLS_ORDER:
        if c not in pvt.columns:
            pvt[c] = 0
    pvt = pvt[DEMO_COLS_ORDER].sort_index()
    pvt.insert(0, "회차", pvt.index.map(_fmt_ep))
    return pvt.reset_index(drop=True)

def render_demographic():
    df_all = load_data()
    ip_options = sorted(df_all["IP"].dropna().unique().tolist())
    selected_ip1 = None; selected_ip2 = None; selected_group_criteria = None
    filter_cols = st.columns([3, 2, 2, 3, 3])
    with filter_cols[0]:
        st.markdown("### 👥 IP 오디언스 히트맵")
    with filter_cols[1]:
        comparison_mode = st.selectbox("비교 모드", ["IP vs IP", "IP vs 그룹"], index=0, key="demo_compare_mode", label_visibility="collapsed")
    with filter_cols[2]:
        selected_media_type = st.selectbox("분석 매체", ["TV", "TVING"], index=0, key="demo_media_type", label_visibility="collapsed")
    with filter_cols[3]:
        selected_ip1 = st.selectbox("기준 IP", ip_options, index=0 if ip_options else None, label_visibility="collapsed", key="demo_ip1_unified")
    with filter_cols[4]:
        if comparison_mode == "IP vs IP":
            selected_ip2 = st.selectbox("비교 IP", [ip for ip in ip_options if ip != selected_ip1],
                                        index=1 if len([ip for ip in ip_options if ip != selected_ip1]) > 1 else 0,
                                        label_visibility="collapsed", key="demo_ip2")
        else:
            selected_group_criteria = st.multiselect("비교 그룹 기준", ["동일 편성", "방영 연도"], default=["동일 편성"],
                                                     label_visibility="collapsed", key="demo_group_criteria")

    media_list_label = "TV" if selected_media_type == "TV" else "TVING (L+Q+V 합산)"
    st.caption(f"선택된 두 대상의 회차별 데모 시청인구 비교 ( {media_list_label} / 비교대상 대비 % 증감 )")
    st.divider()
    if not selected_ip1: st.warning("기준 IP를 선택해주세요."); return
    if comparison_mode == "IP vs IP" and (not selected_ip2): st.warning("비교 IP를 선택해주세요."); return

    df_base = pd.DataFrame(); df_comp = pd.DataFrame(); comp_name = ""
    media_list = ["TV"] if selected_media_type == "TV" else ["TVING LIVE", "TVING QUICK", "TVING VOD"]
    df_ip1_data = df_all[df_all["IP"] == selected_ip1].copy()
    if not df_ip1_data.empty:
        df_base = get_avg_demo_pop_by_episode(df_ip1_data, media_list)
    if comparison_mode == "IP vs IP":
        if selected_ip2:
            df_ip2_data = df_all[df_all["IP"] == selected_ip2].copy()
            if not df_ip2_data.empty:
                df_comp = get_avg_demo_pop_by_episode(df_ip2_data, media_list)
            comp_name = selected_ip2
        else:
            st.warning("비교 IP를 선택해주세요."); return
    else:
        df_group_filtered = df_all.copy(); group_name_parts = []
        base_ip_info_rows = df_all[df_all["IP"] == selected_ip1]
        if not base_ip_info_rows.empty:
            base_ip_prog = base_ip_info_rows["편성"].dropna().mode().iloc[0] if not base_ip_info_rows["편성"].dropna().empty else None
            date_col = "방영시작일" if "방영시작일" in df_all.columns and df_all["방영시작일"].notna().any() else "주차시작일"
            base_ip_year = base_ip_info_rows[date_col].dropna().dt.year.mode().iloc[0] if not base_ip_info_rows[date_col].dropna().empty else None
            if not selected_group_criteria:
                st.info("비교 그룹 기준이 선택되지 않아 '전체'와 비교합니다.")
                group_name_parts.append("전체")
            else:
                if "동일 편성" in selected_group_criteria:
                    if base_ip_prog:
                        df_group_filtered = df_group_filtered[df_group_filtered["편성"] == base_ip_prog]
                        group_name_parts.append(f"'{base_ip_prog}'")
                    else: st.warning("기준 IP 편성 정보 없음 (동일 편성 제외)", icon="⚠️")
                if "방영 연도" in selected_group_criteria:
                    if base_ip_year:
                        df_group_filtered = df_group_filtered[df_group_filtered[date_col].dt.year == int(base_ip_year)]
                        group_name_parts.append(f"{int(base_ip_year)}년")
                    else: st.warning("기준 IP 연도 정보 없음 (방영 연도 제외)", icon="⚠️")
                if not group_name_parts:
                    st.error("비교 그룹을 정의할 수 없습니다. (기준 IP 정보 부족)"); return
            if not df_group_filtered.empty:
                df_comp = get_avg_demo_pop_by_episode(df_group_filtered, media_list)
                comp_name = " & ".join(group_name_parts) + " 평균"
            else:
                st.warning("선택하신 그룹 조건에 맞는 데이터가 없습니다.")
                comp_name = " & ".join(group_name_parts) + " 평균"
        else:
            st.error("기준 IP 정보를 찾을 수 없습니다."); return

    if df_base.empty:
        st.warning("기준 IP의 데모 데이터를 생성할 수 없습니다.")
        render_heatmap(pd.DataFrame(), f"{media_list_label} 데모 증감 비교 ({selected_ip1} vs {comp_name})")
        return
    if df_comp.empty:
        st.warning(f"비교 대상({comp_name})의 데모 데이터를 생성할 수 없습니다. Index 계산 시 비교값은 0으로 처리됩니다.")
        df_comp = pd.DataFrame({'회차': df_base['회차']})
        for col in DEMO_COLS_ORDER: df_comp[col] = 0.0

    df_merged = pd.merge(df_base, df_comp, on="회차", suffixes=('_base', '_comp'), how='left')
    df_index = df_merged[["회차"]].copy()
    for col in DEMO_COLS_ORDER:
        base_col = col + '_base'
        comp_col = col + '_comp'
        if base_col not in df_merged.columns: df_merged[base_col] = 0.0
        else: df_merged[base_col] = pd.to_numeric(df_merged[base_col], errors='coerce').fillna(0.0)
        if comp_col not in df_merged.columns: df_merged[comp_col] = 0.0
        else: df_merged[comp_col] = pd.to_numeric(df_merged[comp_col], errors='coerce').fillna(0.0)
        base_values = df_merged[base_col].values
        comp_values = df_merged[comp_col].values
        index_values = np.where(
            comp_values != 0,
            ((base_values - comp_values) / comp_values) * 100,
            np.where(base_values == 0, 0.0, 999)
        )
        df_index[col] = index_values
        df_index[base_col] = base_values
        df_index[comp_col] = comp_values

    table_title = f"{media_list_label} 데모 증감 비교 ({selected_ip1} vs {comp_name})"
    render_heatmap(df_index, table_title)
#endregion


#region [ 11. 페이지 4: IP간 비교분석 ]
# =====================================================
@st.cache_data(ttl=600)
def get_kpi_data_for_all_ips(df_all: pd.DataFrame) -> pd.DataFrame:
    df = df_all.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.loc[df["value"] == 0, "value"] = np.nan
    df = df.dropna(subset=["value"])
    if "회차_numeric" in df.columns:
        df = df.dropna(subset=["회차_numeric"])
    def _ip_mean_of_ep_mean(metric_name: str) -> pd.Series:
        sub = df[df["metric"] == metric_name]
        if sub.empty: return pd.Series(dtype=float, name=metric_name)
        ep_mean = sub.groupby(["IP", "회차_numeric"])["value"].mean().reset_index()
        return ep_mean.groupby("IP")["value"].mean().rename(metric_name)
    kpi_t_rating = _ip_mean_of_ep_mean("T시청률")
    kpi_h_rating = _ip_mean_of_ep_mean("H시청률")
    sub_vod = df[(df["metric"] == "시청인구") & (df["매체"] == "TVING VOD")]
    if not sub_vod.empty:
        vod_ep_sum = sub_vod.groupby(["IP", "회차_numeric"])["value"].sum().reset_index()
        kpi_vod = vod_ep_sum.groupby("IP")["value"].mean().rename("TVING VOD")
    else:
        kpi_vod = pd.Series(dtype=float, name="TVING VOD")
    sub_lq = df[(df["metric"] == "시청인구") & (df["매체"].isin(["TVING LIVE", "TVING QUICK"]))]
    if not sub_lq.empty:
        lq_ep_sum = sub_lq.groupby(["IP", "회차_numeric"])["value"].sum().reset_index()
        kpi_livequick = lq_ep_sum.groupby("IP")["value"].mean().rename("TVING 라이브+QUICK")
    else:
        kpi_livequick = pd.Series(dtype=float, name="TVING 라이브+QUICK")
    kpi_view = df[df["metric"] == "조회수"].groupby("IP")["value"].sum().rename("디지털 조회수")
    kpi_buzz = df[df["metric"] == "언급량"].groupby("IP")["value"].sum().rename("디지털 언급량")
    kpi_df = pd.concat([kpi_t_rating, kpi_h_rating, kpi_vod, kpi_livequick, kpi_view, kpi_buzz], axis=1)
    kpi_percentiles = kpi_df.rank(pct=True) * 100
    return kpi_percentiles.fillna(0)

def get_agg_kpis_for_ip_page4(df_ip: pd.DataFrame) -> Dict[str, float | None]:
    kpis = {}
    kpis["T시청률"] = mean_of_ip_episode_mean(df_ip, "T시청률")
    kpis["H시청률"] = mean_of_ip_episode_mean(df_ip, "H시청률")
    kpis["TVING VOD"] = mean_of_ip_episode_sum(df_ip, "시청인구", ["TVING VOD"])
    kpis["TVING 라이브+QUICK"] = mean_of_ip_episode_sum(df_ip, "시청인구", ["TVING LIVE", "TVING QUICK"])
    kpis["디지털 조회수"] = mean_of_ip_sums(df_ip, "조회수")
    kpis["디지털 언급량"] = mean_of_ip_sums(df_ip, "언급량")
    fundex = df_ip[df_ip["metric"] == "F_Total"]["value"]
    kpis["화제성 순위"] = fundex.min() if not fundex.empty else None
    kpis["화제성 순위(평균)"] = fundex.mean() if not fundex.empty else None
    return kpis

def render_ip_vs_group_comparison(df_all: pd.DataFrame, ip: str, group_criteria: List[str], kpi_percentiles: pd.DataFrame):
    df_ip = df_all[df_all["IP"] == ip].copy()
    df_group = df_all.copy()
    group_name_parts = []
    ip_prog = df_ip["편성"].dropna().mode().iloc[0] if not df_ip["편성"].dropna().empty else None
    date_col = "방영시작일" if "방영시작일" in df_ip.columns and df_ip["방영시작일"].notna().any() else "주차시작일"
    ip_year = df_ip[date_col].dropna().dt.year.mode().iloc[0] if not df_ip[date_col].dropna().empty else None
    if "동일 편성" in group_criteria:
        if ip_prog:
            df_group = df_group[df_group["편성"] == ip_prog]
            group_name_parts.append(f"'{ip_prog}'")
        else:
            st.warning(f"'{ip}'의 편성 정보가 없어 '동일 편성' 기준은 제외됩니다.")
            group_criteria.remove("동일 편성")
    if "방영 연도" in group_criteria:
        if ip_year:
            df_group = df_group[df_group[date_col].dt.year == ip_year]
            group_name_parts.append(f"{int(ip_year)}년")
        else:
            st.warning(f"'{ip}'의 연도 정보가 없어 '방영 연도' 기준은 제외됩니다.")
            group_criteria.remove("방영 연도")
    if not group_name_parts:
        st.error("비교 그룹을 정의할 수 없습니다.")
        return
    group_name = " & ".join(group_name_parts) + " 평균"
    st.markdown(f"### ⚖️ IP vs 그룹 평균 비교: <span style='color:#d93636;'>{ip}</span> vs <span style='color:#2a61cc;'>{group_name}</span>", unsafe_allow_html=True)
    st.divider()
    kpis_ip = get_agg_kpis_for_ip_page4(df_ip)
    kpis_group = get_agg_kpis_for_ip_page4(df_group)
    def calc_delta(ip_val, group_val):
        ip_val = ip_val or 0; group_val = group_val or 0
        if group_val is None or group_val == 0: return None
        return (ip_val - group_val) / group_val
    def calc_delta_rank(ip_val, group_val):
        if ip_val is None or group_val is None: return None
        return ip_val - group_val
    delta_t = calc_delta(kpis_ip.get('T시청률'), kpis_group.get('T시청률'))
    delta_h = calc_delta(kpis_ip.get('H시청률'), kpis_group.get('H시청률'))
    delta_lq = calc_delta(kpis_ip.get('TVING 라이브+QUICK'), kpis_group.get('TVING 라이브+QUICK'))
    delta_vod = calc_delta(kpis_ip.get('TVING VOD'), kpis_group.get('TVING VOD'))
    delta_view = calc_delta(kpis_ip.get('디지털 조회수'), kpis_group.get('디지털 조회수'))
    delta_buzz = calc_delta(kpis_ip.get('디지털 언급량'), kpis_group.get('디지털 언급량'))
    delta_rank = calc_delta_rank(kpis_ip.get('화제성 순위'), kpis_group.get('화제성 순위'))
    st.markdown(f"#### 1. 주요 성과 ({group_name} 대비)")
    kpi_cols = st.columns(7)
    with kpi_cols[0]: st.metric("🎯 타깃시청률", f"{kpis_ip.get('T시청률', 0):.2f}%", f"{delta_t * 100:.1f}%" if delta_t is not None else "N/A", help=f"그룹 평균: {kpis_group.get('T시청률', 0):.2f}%")
    with kpi_cols[1]: st.metric("🏠 가구시청률", f"{kpis_ip.get('H시청률', 0):.2f}%", f"{delta_h * 100:.1f}%" if delta_h is not None else "N/A", help=f"그룹 평균: {kpis_group.get('H시청률', 0):.2f}%")
    with kpi_cols[2]: st.metric("⚡ 티빙 라이브+QUICK", f"{kpis_ip.get('TVING 라이브+QUICK', 0):,.0f}", f"{delta_lq * 100:.1f}%" if delta_lq is not None else "N/A", help=f"그룹 평균: {kpis_group.get('TVING 라이브+QUICK', 0):,.0f}")
    with kpi_cols[3]: st.metric("▶️ 티빙 VOD", f"{kpis_ip.get('TVING VOD', 0):,.0f}", f"{delta_vod * 100:.1f}%" if delta_vod is not None else "N/A", help=f"그룹 평균: {kpis_group.get('TVING VOD', 0):,.0f}")
    with kpi_cols[4]: st.metric("👀 디지털 조회수", f"{kpis_ip.get('디지털 조회수', 0):,.0f}", f"{delta_view * 100:.1f}%" if delta_view is not None else "N/A", help=f"그룹 평균: {kpis_group.get('디지털 조회수', 0):,.0f}")
    with kpi_cols[5]: st.metric("💬 디지털 언급량", f"{kpis_ip.get('디지털 언급량', 0):,.0f}", f"{delta_buzz * 100:.1f}%" if delta_buzz is not None else "N/A", help=f"그룹 평균: {kpis_group.get('디지털 언급량', 0):,.0f}")
    with kpi_cols[6]: st.metric("🔥 화제성(최고순위)", f"{kpis_ip.get('화제성 순위', 0):.0f}위" if kpis_ip.get('화제성 순위') else "N/A",
                                f"{delta_rank:.0f}위" if delta_rank is not None else "N/A", delta_color="inverse",
                                help=f"그룹 평균: {kpis_group.get('화제성 순위', 0):.1f}위")
    st.divider()
    st.markdown(f"#### 2. 성과 포지셔닝 ({group_name} 대비)")
    col_radar, col_dev = st.columns(2)
    with col_radar:
        st.markdown(f"###### 성과 시그니처 (백분위 점수)")
        group_ips = df_group["IP"].unique()
        group_percentiles_avg = kpi_percentiles.loc[kpi_percentiles.index.isin(group_ips)].mean()
        radar_metrics = ["T시청률", "H시청률", "TVING 라이브+QUICK", "TVING VOD", "디지털 조회수", "디지털 언급량"]
        score_ip_series = kpi_percentiles.loc[ip][radar_metrics]
        score_group_series = group_percentiles_avg[radar_metrics]
        fig_radar_group = go.Figure()
        fig_radar_group.add_trace(go.Scatterpolar(r=score_ip_series.values,
            theta=score_ip_series.index.map({"T시청률":"타깃","H시청률":"가구","TVING 라이브+QUICK":"TVING L+Q","TVING VOD":"TVING VOD","디지털 조회수":"조회수","디지털 언급량":"언급량"}),
            fill='toself', name=ip, line=dict(color="#d93636")))
        fig_radar_group.add_trace(go.Scatterpolar(r=score_group_series.values,
            theta=score_group_series.index.map({"T시청률":"타깃","H시청률":"가구","TVING 라이브+QUICK":"TVING L+Q","TVING VOD":"TVING VOD","디지털 조회수":"조회수","디지털 언급량":"언급량"}),
            fill='toself', name=group_name, line=dict(color="#2a61cc")))
        fig_radar_group.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                                      showlegend=True, height=350, margin=dict(l=60, r=60, t=40, b=40),
                                      legend=dict(orientation="h", yanchor="bottom", y=1.05))
        st.plotly_chart(fig_radar_group, use_container_width=True)
    with col_dev:
        st.markdown(f"###### 주요 지표 편차 (%)")
        metrics_to_compare = {"T시청률":"타깃","H시청률":"가구","TVING 라이브+QUICK":"TVING L+Q","TVING VOD":"TVING VOD","디지털 조회수":"조회수","디지털 언급량":"언급량"}
        delta_data = []
        for m_key, m_label in metrics_to_compare.items():
            delta_val = calc_delta(kpis_ip.get(m_key), kpis_group.get(m_key))
            delta_data.append({"metric": m_label, "delta_pct": (delta_val * 100) if delta_val is not None else 0})
        df_delta = pd.DataFrame(delta_data)
        df_delta["color"] = df_delta["delta_pct"].apply(lambda x: "#d93636" if x > 0 else "#2a61cc")
        fig_dev_kpi = px.bar(df_delta, x="metric", y="delta_pct", text="delta_pct")
        fig_dev_kpi.update_traces(texttemplate='%{text:.1f}%', textposition='outside', marker_color=df_delta["color"])
        fig_dev_kpi.update_layout(height=350, yaxis_title="편차 (%)", xaxis_title=None, margin=dict(t=40, b=0))
        st.plotly_chart(fig_dev_kpi, use_container_width=True)
    st.divider()
    st.markdown(f"#### 3. 시청률 트렌드 비교 ({group_name} 대비)")
    col_trend_t, col_trend_h = st.columns(2)
    with col_trend_t:
        st.markdown("###### 🎯 타깃시청률 (회차별)")
        ip_trend_t = df_ip[df_ip["metric"] == "T시청률"].groupby("회차_numeric")["value"].mean().reset_index(); ip_trend_t["구분"]=ip
        group_ep_avg_t = df_group[df_group["metric"] == "T시청률"].groupby(["IP", "회차_numeric"])["value"].mean().reset_index()
        group_trend_t = group_ep_avg_t.groupby("회차_numeric")["value"].mean().reset_index(); group_trend_t["구분"]=group_name
        trend_data_t = pd.concat([ip_trend_t, group_trend_t])
        if not trend_data_t.empty:
            fig_trend_t = px.line(trend_data_t, x="회차_numeric", y="value", color="구분", line_dash="구분", markers=True,
                                  color_discrete_map={ip:"#d93636", group_name:"#aaaaaa"},
                                  line_dash_map={ip:"solid", group_name:"dot"})
            fig_trend_t.update_layout(height=350, yaxis_title="타깃시청률 (%)", xaxis_title="회차",
                                      margin=dict(t=20, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_trend_t, use_container_width=True)
        else: st.info("타깃시청률 트렌드 데이터 없음")
    with col_trend_h:
        st.markdown("###### 🏠 가구시청률 (회차별)")
        ip_trend_h = df_ip[df_ip["metric"] == "H시청률"].groupby("회차_numeric")["value"].mean().reset_index(); ip_trend_h["구분"]=ip
        group_ep_avg_h = df_group[df_group["metric"] == "H시청률"].groupby(["IP", "회차_numeric"])["value"].mean().reset_index()
        group_trend_h = group_ep_avg_h.groupby("회차_numeric")["value"].mean().reset_index(); group_trend_h["구분"]=group_name
        trend_data_h = pd.concat([ip_trend_h, group_trend_h])
        if not trend_data_h.empty:
            fig_trend_h = px.line(trend_data_h, x="회차_numeric", y="value", color="구분", line_dash="구분", markers=True,
                                  color_discrete_map={ip:"#d93636", group_name:"#aaaaaa"},
                                  line_dash_map={ip:"solid", group_name:"dot"})
            fig_trend_h.update_layout(height=350, yaxis_title="가구시청률 (%)", xaxis_title="회차",
                                      margin=dict(t=20, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_trend_h, use_container_width=True)
        else: st.info("가구시청률 트렌드 데이터 없음")
    st.divider()
    st.markdown(f"#### 4. 시청인구 비교 ({group_name} 대비)")
    col_demo_tv, col_demo_tving = st.columns(2)
    def get_demo_avg_pop(df_demo_src, media_filter: List[str]):
        df_demo = df_demo_src[(df_demo_src["metric"] == "시청인구") & (df_demo_src["매체"].isin(media_filter)) & (df_demo_src["데모"].notna())].copy()
        df_demo["연령대_대"] = df_demo["데모"].apply(_to_decade_label)
        df_demo["성별"] = df_demo["데모"].apply(_gender_from_demo)
        df_demo = df_demo[df_demo["성별"].isin(["남", "여"]) & (df_demo["연령대_대"] != "기타")]
        df_demo["데모_구분"] = df_demo["연령대_대"] + df_demo["성별"]
        agg = df_demo.groupby(["IP", "회차_numeric", "데모_구분"])["value"].sum().reset_index()
        avg_pop = agg.groupby("데모_구분")["value"].mean()
        return avg_pop
    with col_demo_tv:
        st.markdown(f"###### 📺 TV (평균 시청인구)")
        ip_pop_tv = get_demo_avg_pop(df_ip, ["TV"])
        group_pop_tv = get_demo_avg_pop(df_group, ["TV"])
        df_demo_tv = pd.DataFrame({"IP": ip_pop_tv, "Group": group_pop_tv}).fillna(0).reset_index()
        df_demo_tv_melt = df_demo_tv.melt(id_vars="데모_구분", var_name="구분", value_name="시청인구")
        sort_map = {f"{d}대{'남' if g == 0 else '여'}": d*10 + g for d in range(1, 7) for g in range(2)}
        df_demo_tv_melt["sort_key"] = df_demo_tv_melt["데모_구분"].map(sort_map).fillna(999)
        df_demo_tv_melt = df_demo_tv_melt.sort_values("sort_key")
        if not df_demo_tv_melt.empty:
            fig_demo_tv = px.bar(df_demo_tv_melt, x="데모_구분", y="시청인구", color="구분", barmode="group",
                                 text="시청인구", color_discrete_map={"IP":"#d93636", "Group":"#2a61cc"})
            fig_demo_tv.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig_demo_tv.update_layout(height=350, yaxis_title="평균 시청인구", xaxis_title=None,
                                      margin=dict(t=20, b=0),
                                      legend=dict(title=None, orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_demo_tv, use_container_width=True)
        else: st.info("TV 데모 데이터 없음")
    with col_demo_tving:
        st.markdown(f"###### ▶️ TVING (평균 시청인구)")
        tving_media = ["TVING LIVE", "TVING QUICK", "TVING VOD"]
        ip_pop_tving = get_demo_avg_pop(df_ip, tving_media)
        group_pop_tving = get_demo_avg_pop(df_group, tving_media)
        df_demo_tving = pd.DataFrame({"IP": ip_pop_tving, "Group": group_pop_tving}).fillna(0).reset_index()
        df_demo_tving_melt = df_demo_tving.melt(id_vars="데모_구분", var_name="구분", value_name="시청인구")
        df_demo_tving_melt["sort_key"] = df_demo_tving_melt["데모_구분"].map(sort_map).fillna(999)
        df_demo_tving_melt = df_demo_tving_melt.sort_values("sort_key")
        if not df_demo_tving_melt.empty:
            fig_demo_tving = px.bar(df_demo_tving_melt, x="데모_구분", y="시청인구", color="구분", barmode="group",
                                    text="시청인구", color_discrete_map={"IP":"#d93636", "Group":"#2a61cc"})
            fig_demo_tving.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig_demo_tving.update_layout(height=350, yaxis_title="평균 시청인구", xaxis_title=None,
                                         margin=dict(t=20, b=0),
                                         legend=dict(title=None, orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_demo_tving, use_container_width=True)
        else: st.info("TVING 데모 데이터 없음")

def _render_kpi_card_comparison(title: str, val1: float | None, val2: float | None, ip1_name: str, ip2_name: str,
                                format_str: str = "{:,.0f}", higher_is_better: bool = True):
    val1_disp = format_str.format(val1) if val1 is not None else "–"
    val2_disp = format_str.format(val2) if val2 is not None else "–"
    winner = 0
    if val1 is not None and val2 is not None:
        if higher_is_better:
            if val1 > val2: winner = 1
            elif val2 > val1: winner = 2
        else:
            if val1 < val2: winner = 1
            elif val2 < val1: winner = 2
    val1_style = "color:#d93636; font-weight: 700;" if winner == 1 else ("color:#888; font-weight: 400;" if winner == 2 else "color:#333; font-weight: 400;")
    val2_style = "color:#2a61cc; font-weight: 700;" if winner == 2 else ("color:#888; font-weight: 400;" if winner == 1 else "color:#333; font-weight: 400;")
    st.markdown(f"""
    <div class="kpi-card" style="height: 100px; display: flex; flex-direction: column; justify-content: center;">
        <div class="kpi-title">{title}</div>
        <div class="kpi_value" style="font-size: 1.1rem; line-height: 1.4; margin-top: 5px;">
            <span style="{val1_style}">
                <span style="font-size: 0.8em; color: #d93636;">{ip1_name}:</span> {val1_disp}
            </span>
            <br>
            <span style="{val2_style}">
                <span style="font-size: 0.8em; color: #2a61cc;">{ip2_name}:</span> {val2_disp}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_ip_vs_ip_comparison(df_all: pd.DataFrame, ip1: str, ip2: str, kpi_percentiles: pd.DataFrame):
    st.markdown(f"### ⚖️ IP 직접 비교: <span style='color:#d93636;'>{ip1}</span> vs <span style='color:#2a61cc;'>{ip2}</span>", unsafe_allow_html=True)
    st.divider()
    df1 = df_all[df_all["IP"] == ip1].copy()
    df2 = df_all[df_all["IP"] == ip2].copy()
    kpis1 = get_agg_kpis_for_ip_page4(df1)
    kpis2 = get_agg_kpis_for_ip_page4(df2)
    st.markdown("#### 1. 주요 성과 요약")
    kpi_cols_1 = st.columns(4)
    with kpi_cols_1[0]: _render_kpi_card_comparison("🎯 타깃시청률", kpis1.get("T시청률"), kpis2.get("T시청률"), ip1, ip2, "{:.2f}%")
    with kpi_cols_1[1]: _render_kpi_card_comparison("🏠 가구시청률", kpis1.get("H시청률"), kpis2.get("H시청률"), ip1, ip2, "{:.2f}%")
    with kpi_cols_1[2]: _render_kpi_card_comparison("⚡ 티빙 라이브+QUICK", kpis1.get("TVING 라이브+QUICK"), kpis2.get("TVING 라이브+QUICK"), ip1, ip2, "{:,.0f}")
    with kpi_cols_1[3]: _render_kpi_card_comparison("▶️ 티빙 VOD", kpis1.get("TVING VOD"), kpis2.get("TVING VOD"), ip1, ip2, "{:,.0f}")
    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
    kpi_cols_2 = st.columns(4)
    with kpi_cols_2[0]: _render_kpi_card_comparison("👀 디지털 조회수", kpis1.get("디지털 조회수"), kpis2.get("디지털 조회수"), ip1, ip2, "{:,.0f}")
    with kpi_cols_2[1]: _render_kpi_card_comparison("💬 디지털 언급량", kpis1.get("디지털 언급량"), kpis2.get("디지털 언급량"), ip1, ip2, "{:,.0f}")
    with kpi_cols_2[2]: _render_kpi_card_comparison("🔥 화제성(최고순위)", kpis1.get("화제성 순위"), kpis2.get("화제성 순위"), ip1, ip2, "{:,.0f}위", higher_is_better=False)
    with kpi_cols_2[3]: st.markdown("")
    st.divider()
    st.markdown("#### 2. 성과 시그니처 (백분위 점수)")
    radar_metrics = ["T시청률", "H시청률", "TVING 라이브+QUICK", "TVING VOD", "디지털 조회수", "디지털 언급량"]
    score1 = kpi_percentiles.loc[ip1][radar_metrics].reset_index().rename(columns={'index': 'metric', ip1: 'score'}); score1["IP"] = ip1
    score2 = kpi_percentiles.loc[ip2][radar_metrics].reset_index().rename(columns={'index': 'metric', ip2: 'score'}); score2["IP"] = ip2
    radar_data = pd.concat([score1, score2])
    radar_data["metric_label"] = radar_data["metric"].replace({"T시청률": "타깃", "H시청률": "가구", "TVING 라이브+QUICK": "TVING L+Q", "TVING VOD": "TVING VOD", "디지털 조회수": "조회수", "디지털 언급량": "언급량"})
    fig_radar = px.line_polar(radar_data, r="score", theta="metric_label", line_close=True, color="IP",
                              color_discrete_map={ip1: "#d93636", ip2: "#2a61cc"}, range_r=[0, 100], markers=True)
    fig_radar.update_layout(height=400, margin=dict(l=80, r=80, t=40, b=40))
    st.plotly_chart(fig_radar, use_container_width=True)
    st.divider()
    st.markdown("#### 3. 트렌드 비교")
    c_trend1, c_trend2 = st.columns(2)
    with c_trend1:
        st.markdown("###### 📈 시청률 추이 (회차별)")
        t_trend1 = df1[df1["metric"] == "T시청률"].groupby("회차_numeric")["value"].mean().rename("타깃")
        h_trend1 = df1[df1["metric"] == "H시청률"].groupby("회차_numeric")["value"].mean().rename("가구")
        t_trend2 = df2[df2["metric"] == "T시청률"].groupby("회차_numeric")["value"].mean().rename("타깃")
        h_trend2 = df2[df2["metric"] == "H시청률"].groupby("회차_numeric")["value"].mean().rename("가구")
        fig_t = go.Figure()
        fig_t.add_trace(go.Scatter(x=h_trend1.index, y=h_trend1.values, name=f"{ip1} (가구)", mode='lines+markers', line=dict(color="#d93636", dash="solid")))
        fig_t.add_trace(go.Scatter(x=t_trend1.index, y=t_trend1.values, name=f"{ip1} (타깃)", mode='lines+markers', line=dict(color="#2a61cc", dash="solid")))
        fig_t.add_trace(go.Scatter(x=h_trend2.index, y=h_trend2.values, name=f"{ip2} (가구)", mode='lines+markers', line=dict(color="#d93636", dash="dot")))
        fig_t.add_trace(go.Scatter(x=t_trend2.index, y=t_trend2.values, name=f"{ip2} (타깃)", mode='lines+markers', line=dict(color="#2a61cc", dash="dot")))
        fig_t.update_layout(height=300, yaxis_title="시청률 (%)", xaxis_title="회차", margin=dict(t=20, b=0),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_t, use_container_width=True)
    with c_trend2:
        st.markdown("###### 🔥 화제성 순위 (주차별)")
        f_trend1 = df1[df1["metric"] == "F_Total"].groupby("주차")["value"].min().reset_index(); f_trend1["IP"] = ip1
        f_trend2 = df2[df2["metric"] == "F_Total"].groupby("주차")["value"].min().reset_index(); f_trend2["IP"] = ip2
        f_trend_data = pd.concat([f_trend1, f_trend2])
        if not f_trend_data.empty:
            fig_f = px.line(f_trend_data, x="주차", y="value", color="IP", title=None, markers=True,
                            color_discrete_map={ip1: "#d93636", ip2: "#2a61cc"})
            fig_f.update_layout(height=300, yaxis_title="화제성 순위", yaxis=dict(autorange="reversed"),
                                margin=dict(t=20, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_f, use_container_width=True)
        else:
            st.info("화제성 트렌드 데이터가 없습니다.")
    st.divider()
    st.markdown("#### 4. TV 시청자 데모 비교 (TV 시청인구 비중)")
    demo1 = df1[(df1["metric"] == "시청인구") & (df1["매체"] == "TV") & (df1["데모"].notna())]
    demo2 = df2[(df2["metric"] == "시청인구") & (df2["매체"] == "TV") & (df2["데모"].notna())]
    def prep_demo_data(df_demo, ip_name):
        df_demo["연령대_대"] = df_demo["데모"].apply(_to_decade_label)
        df_demo = df_demo[df_demo["연령대_대"] != "기타"]
        agg = df_demo.groupby("연령대_대")["value"].sum()
        total = agg.sum()
        return pd.DataFrame({"연령대": agg.index, "비중": (agg / total * 100) if total > 0 else agg, "IP": ip_name})
    demo_agg1 = prep_demo_data(demo1, ip1)
    demo_agg2 = prep_demo_data(demo2, ip2)
    demo_data_grouped = pd.concat([demo_agg1, demo_agg2])
    all_decades = sorted(demo_data_grouped["연령대"].unique(), key=_decade_key)
    fig_demo = px.bar(demo_data_grouped, x="연령대", y="비중", color="IP", barmode="group", text="비중",
                      color_discrete_map={ip1: "#d93636", ip2: "#2a61cc"},
                      category_orders={"연령대": all_decades})
    fig_demo.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig_demo.update_layout(height=350, margin=dict(t=20, b=20, l=20, r=20),
                           yaxis_title="시청 비중 (%)", xaxis_title="연령대",
                           legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_demo, use_container_width=True)

def render_comparison():
    df_all = load_data()
    try:
        kpi_percentiles = get_kpi_data_for_all_ips(df_all)
    except Exception as e:
        st.error(f"KPI 백분위 계산 중 오류: {e}")
        kpi_percentiles = pd.DataFrame()
    filter_cols = st.columns([3, 2, 3, 3])
    ip_options = sorted(df_all["IP"].dropna().unique().tolist())
    selected_ip1 = None; selected_ip2 = None; selected_group_criteria = None
    with filter_cols[0]:
        st.markdown("## ⚖️ IP간 비교분석")
    with filter_cols[1]:
        comparison_mode = st.radio("비교 모드", ["IP vs IP", "IP vs 그룹 평균"], index=1, horizontal=True, label_visibility="collapsed")
    with filter_cols[2]:
        selected_ip1 = st.selectbox("기준 IP", ip_options, index=0 if ip_options else None, label_visibility="collapsed")
    with filter_cols[3]:
        if comparison_mode == "IP vs IP":
            ip_options_2 = [ip for ip in ip_options if ip != selected_ip1]
            selected_ip2 = st.selectbox("비교 IP", ip_options_2,
                                        index=1 if len(ip_options_2) > 1 else (0 if len(ip_options_2) > 0 else None),
                                        label_visibility="collapsed")
        else:
            selected_group_criteria = st.multiselect("비교 그룹 기준", ["동일 편성", "방영 연도"], default=["동일 편성"], label_visibility="collapsed")
    if comparison_mode == "IP vs 그룹 평균":
        if selected_ip1 and selected_group_criteria and not kpi_percentiles.empty:
            render_ip_vs_group_comparison(df_all, selected_ip1, selected_group_criteria, kpi_percentiles)
        elif kpi_percentiles.empty:
            st.error("Radar Chart KPI 데이터 로드 실패.")
        elif not selected_group_criteria:
            st.warning("필터에서 비교 그룹 기준을 1개 이상 선택해주세요.")
        else:
            st.info("필터에서 기준 IP와 비교 그룹 기준을 선택해주세요.")
    else:
        if selected_ip1 and selected_ip2 and not kpi_percentiles.empty:
            render_ip_vs_ip_comparison(df_all, selected_ip1, selected_ip2, kpi_percentiles)
        elif kpi_percentiles.empty:
            st.error("Radar Chart KPI 데이터 로드 실패.")
        else:
            st.info("필터에서 비교할 두 IP를 선택해주세요.")
#endregion


#region [ 12. 페이지 5: 회차별 비교 ]
# =====================================================
def filter_data_for_episode_comparison(df_all_filtered: pd.DataFrame, selected_episode: str, selected_metric: str) -> pd.DataFrame:
    episode_num_str = selected_episode.split(" ")[0]
    target_episode_num_str = episode_num_str
    try:
        target_episode_num = float(target_episode_num_str)
    except ValueError:
        return pd.DataFrame({'IP': df_all_filtered["IP"].unique(), 'value': 0})
    base_filtered = pd.DataFrame()
    if "회차_numeric" in df_all_filtered.columns:
        base_filtered = df_all_filtered[df_all_filtered["회차_numeric"] == target_episode_num].copy()
    if base_filtered.empty and "회차" in df_all_filtered.columns:
        possible_strs = [target_episode_num_str + "화", target_episode_num_str + "차"]
        existing_ep_strs_in_filtered = df_all_filtered['회차'].unique()
        episode_filter_str = None
        for p_str in possible_strs:
            if p_str in existing_ep_strs_in_filtered:
                episode_filter_str = p_str; break
        if episode_filter_str:
            base_filtered = df_all_filtered[df_all_filtered["회차"] == episode_filter_str].copy()
    result_df = pd.DataFrame(columns=["IP", "value"])
    if not base_filtered.empty:
        if selected_metric in ["T시청률", "H시청률"]:
            filtered = base_filtered[base_filtered["metric"] == selected_metric]
            if not filtered.empty:
                result_df = filtered.groupby("IP")["value"].mean().reset_index()
        elif selected_metric == "TVING 라이브+QUICK":
            df_lq = base_filtered[(base_filtered["metric"] == "시청인구") & (base_filtered["매체"].isin(["TVING LIVE", "TVING QUICK"]))]
            if not df_lq.empty:
                result_df = df_lq.groupby("IP")["value"].sum().reset_index()
        elif selected_metric == "TVING VOD":
            df_vod = base_filtered[(base_filtered["metric"] == "시청인구") & (base_filtered["매체"] == "TVING VOD")]
            if not df_vod.empty:
                result_df = df_vod.groupby("IP")["value"].sum().reset_index()
        elif selected_metric in ["조회수", "언급량"]:
            filtered = base_filtered[base_filtered["metric"] == selected_metric]
            if not filtered.empty:
                result_df = filtered.groupby("IP")["value"].sum().reset_index()
        else:
            filtered = base_filtered[base_filtered["metric"] == selected_metric]
            if not filtered.empty:
                result_df = filtered.groupby("IP")["value"].mean().reset_index()
    all_ips_in_filter = df_all_filtered["IP"].unique()
    if result_df.empty:
        result_df = pd.DataFrame({'IP': all_ips_in_filter, 'value': 0})
    else:
        if 'value' not in result_df.columns: result_df['value'] = 0
        result_df = result_df.set_index("IP").reindex(all_ips_in_filter, fill_value=0).reset_index()
    result_df['value'] = pd.to_numeric(result_df['value'], errors='coerce').fillna(0)
    return result_df.sort_values("value", ascending=False)

def plot_episode_comparison(df_result: pd.DataFrame, selected_metric: str, selected_episode: str, base_ip: str):
    colors = ['#d93636' if ip == base_ip else '#666666' for ip in df_result['IP']]
    metric_label = selected_metric.replace("T시청률", "타깃").replace("H시청률", "가구")
    fig = px.bar(df_result, x="IP", y="value", text="value", title=f"{selected_episode} - '{metric_label}' (기준: {base_ip})")
    hover_template = "<b>%{x}</b><br>" + f"{metric_label}: %{{y:,.2f}}" if selected_metric in ["T시청률", "H시청률"] else "<b>%{x}</b><br>" + f"{metric_label}: %{{y:,.0f}}"
    fig.update_traces(marker_color=colors, textposition='outside', hovertemplate=hover_template)
    if selected_metric in ["T시청률", "H시청률"]:
        fig.update_traces(texttemplate='%{text:.2f}%'); fig.update_layout(yaxis_title=f"{metric_label} (%)")
    else:
        fig.update_traces(texttemplate='%{text:,.0f}'); fig.update_layout(yaxis_title=metric_label)
    fig.update_layout(xaxis_title=None, xaxis=dict(tickfont=dict(size=11)), height=350, margin=dict(t=40, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)

def render_episode():
    df_all = load_data()
    filter_cols = st.columns([3, 3, 2])
    ip_options_main = sorted(df_all["IP"].dropna().unique().tolist())
    episode_options_main = get_episode_options(df_all)
    with filter_cols[0]:
        st.markdown("## 🎬 회차별 비교 ")
    with filter_cols[1]:
        selected_base_ip = st.selectbox("기준 IP (하이라이트)", ip_options_main, index=0 if ip_options_main else None,
                                        label_visibility="collapsed", key="ep_base_ip_main")
    with filter_cols[2]:
        selected_episode = st.selectbox("회차", episode_options_main, index=0 if episode_options_main else None,
                                        label_visibility="collapsed", key="ep_selected_episode_main")
    comparison_group = st.radio("비교 대상 그룹", options=["전체 IP", "동일 편성", "방영 연도", "동일 편성 & 연도"],
                                index=0, key="ep_comp_group", horizontal=True)
    st.divider()
    if not selected_base_ip: st.warning("필터에서 기준 IP를 선택해주세요."); return
    if not selected_episode: st.warning("필터에서 회차를 선택해주세요."); return
    df_filtered_main = df_all.copy()
    group_filter_applied = []
    if comparison_group != "전체 IP":
        base_ip_info_rows = df_all[df_all["IP"] == selected_base_ip]
        if not base_ip_info_rows.empty:
            base_ip_prog = base_ip_info_rows["편성"].dropna().mode().iloc[0] if not base_ip_info_rows["편성"].dropna().empty else None
            date_col = "방영시작일" if "방영시작일" in df_all.columns and df_all["방영시작일"].notna().any() else "주차시작일"
            base_ip_year = base_ip_info_rows[date_col].dropna().dt.year.mode().iloc[0] if not base_ip_info_rows[date_col].dropna().empty else None
            if "동일 편성" in comparison_group:
                if base_ip_prog:
                    df_filtered_main = df_filtered_main[df_filtered_main["편성"] == base_ip_prog]
                    group_filter_applied.append(f"편성='{base_ip_prog}'")
                else: st.warning(f"기준 IP '{selected_base_ip}'의 편성 정보 없음")
            if "방영 연도" in comparison_group:
                if base_ip_year:
                    df_filtered_main = df_filtered_main[df_filtered_main[date_col].dt.year == int(base_ip_year)]
                    group_filter_applied.append(f"연도={int(base_ip_year)}")
                else: st.warning(f"기준 IP '{selected_base_ip}'의 연도 정보 없음")
        else:
            st.warning(f"기준 IP '{selected_base_ip}' 정보를 찾을 수 없습니다.")
            df_filtered_main = pd.DataFrame()
    if df_filtered_main.empty:
        st.warning("선택하신 필터에 해당하는 데이터가 없습니다."); return
    if selected_base_ip not in df_filtered_main["IP"].unique():
        st.warning(f"선택하신 그룹 '{comparison_group}'에 기준 IP '{selected_base_ip}'가 포함되지 않습니다."); return
    key_metrics = ["T시청률","H시청률","TVING 라이브+QUICK","TVING VOD","조회수","언급량"]
    filter_desc = " (" + ", ".join(group_filter_applied) + ")" if group_filter_applied else "(전체 IP)"
    st.markdown(f"#### {selected_episode} 성과 비교 {filter_desc} (기준 IP: {selected_base_ip})")
    st.caption("선택된 IP 그룹의 성과를 보여줍니다. 기준 IP는 붉은색으로 표시됩니다.")
    st.markdown("---")
    chart_cols = st.columns(2); col_idx = 0
    for metric in key_metrics:
        current_col = chart_cols[col_idx % 2]
        with current_col:
            try:
                df_result = filter_data_for_episode_comparison(df_filtered_main, selected_episode, metric)
                if df_result.empty or df_result['value'].isnull().all() or (df_result['value'] == 0).all():
                    metric_label = metric.replace("T시청률", "타깃").replace("H시청률", "가구")
                    st.markdown(f"###### {selected_episode} - '{metric_label}'"); st.info(f"데이터 없음"); st.markdown("---")
                else:
                    plot_episode_comparison(df_result, metric, selected_episode, selected_base_ip); st.markdown("---")
            except Exception as e:
                metric_label = metric.replace("T시청률", "타깃").replace("H시청률", "가구")
                st.markdown(f"###### {selected_episode} - '{metric_label}'"); st.error(f"차트 생성 오류: {e}"); st.markdown("---")
        col_idx += 1
#endregion


#region [ 13. 페이지 6: 성장스코어-방영성과  ]
# =====================================================
def render_growth_score():
    df_all = load_data().copy()
    EP_CHOICES = [2, 4, 6, 8, 10, 12, 14, 16]
    ROW_LABELS = ["S","A","B","C","D"]
    COL_LABELS = ["+2","+1","0","-1","-2"]
    ABS_SCORE  = {"S":5,"A":4,"B":3,"C":2,"D":1}
    SLO_SCORE  = {"+2":5,"+1":4,"0":3,"-1":2,"-2":1}
    METRICS = [
        ("가구시청률", "H시청률", None),
        ("타깃시청률", "T시청률", None),
        ("TVING LIVE", "시청인구", "LIVE"),
        ("TVING VOD",  "시청인구", "VOD"),
    ]
    ips = sorted(df_all["IP"].dropna().unique().tolist())
    if not ips:
        st.warning("IP 데이터가 없습니다."); return

    st.markdown("""
    <style>
      .kpi-card{border-radius:16px;border:1px solid #e7ebf3;background:#fff;padding:12px 14px;box-shadow:0 1px 2px rgba(0,0,0,0.04)}
      .kpi-title{font-size:13px;color:#5b6b83;margin-bottom:4px;font-weight:600}
      .kpi-value{font-weight:800;letter-spacing:-0.2px}
      .centered-header .ag-header-cell-label{justify-content:center;}
      .bold-header .ag-header-cell-text{font-weight:700;}
    </style>
    """, unsafe_allow_html=True)

    _ep_display = st.session_state.get("growth_ep_cutoff", 4)
    head = st.columns([5, 3, 2])
    with head[0]:
        st.markdown(
            f"## 🚀 성장스코어-방영지표 <span style='font-size:20px;color:#6b7b93'>(~{_ep_display}회 기준)</span>",
            unsafe_allow_html=True
        )
    with head[1]:
        selected_ip = st.selectbox("IP 선택", ips, index=0, key="growth_ip_select", label_visibility="collapsed")
    with head[2]:
        ep_cutoff = st.selectbox("회차 기준", EP_CHOICES, index=1, key="growth_ep_cutoff", label_visibility="collapsed")

    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("""
**등급 체계**
- **절대값 등급**: 각 지표의 절대 수준을 IP 간 백분위 20% 단위로 구분 → `S / A / B / C / D`
- **상승률 등급**: 동일 기간(선택 회차 범위) 내 회차-값 선형회귀 기울기(slope)를 IP 간 백분위 20% 단위로 구분 → `+2 / +1 / 0 / -1 / -2`
- **종합등급**: 절대값과 상승률 등급을 결합해 표기 (예: `A+2`).

**회차 기준(~N회)**
- 각 IP의 **1~N회** 데이터만 사용 (없는 회차는 자동 제외).
- **0 패딩/비정상값 제외** 처리로 왜곡 방지.
        """)

    st.markdown(f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기</span>", unsafe_allow_html=True)

    def _filter_to_ep(df, n):
        if "회차_numeric" in df.columns:
            return df[pd.to_numeric(df["회차_numeric"], errors="coerce") <= float(n)]
        m = df["회차"].astype(str).str.extract(r"(\d+)", expand=False)
        return df[pd.to_numeric(m, errors="coerce") <= float(n)]

    def _series_for_reg(ip_df, metric, media):
        sub = ip_df[ip_df["metric"] == metric].copy()
        if media == "LIVE":
            sub = sub[sub["매체"] == "TVING LIVE"]
        elif media == "VOD":
            sub = sub[sub["매체"] == "TVING VOD"]
        sub = _filter_to_ep(sub, ep_cutoff)
        sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
        sub = sub.dropna(subset=["value", "회차_numeric"])
        if sub.empty: return None
        if metric in ["H시청률", "T시청률"]:
            s = sub.groupby("회차_numeric")["value"].mean().reset_index()
        else:
            s = sub.groupby("회차_numeric")["value"].sum().reset_index()
        s = s.sort_values("회차_numeric")
        x = s["회차_numeric"].astype(float).values
        y = s["value"].astype(float).values
        return (x, y) if len(x) >= 2 else None

    def _slope(ip_df, metric, media=None):
        xy = _series_for_reg(ip_df, metric, media)
        if xy is None: return np.nan
        try: return float(np.polyfit(xy[0], xy[1], 1)[0])
        except Exception: return np.nan

    def _abs_value(ip_df, metric, media=None):
        ip_df = _filter_to_ep(ip_df, ep_cutoff)
        if metric in ["H시청률", "T시청률"]:
            return mean_of_ip_episode_mean(ip_df, metric)
        if metric == "시청인구" and media == "LIVE":
            return mean_of_ip_episode_sum(ip_df, "시청인구", ["TVING LIVE"])
        if metric == "시청인구" and media == "VOD":
            return mean_of_ip_episode_sum(ip_df, "시청인구", ["TVING VOD"])
        return None

    def _quintile_grade(series, labels):
        s = pd.Series(series).astype(float)
        valid = s.dropna()
        if valid.empty:
            return pd.Series(index=s.index, data=np.nan)
        ranks = valid.rank(method="average", ascending=False, pct=True)
        bins = [0, .2, .4, .6, .8, 1.0000001]
        idx = np.digitize(ranks.values, bins, right=True) - 1
        idx = np.clip(idx, 0, 4)
        out = pd.Series([labels[i] for i in idx], index=valid.index)
        return out.reindex(s.index)

    def _to_percentile(s):
        s = pd.Series(s).astype(float)
        return s.rank(pct=True) * 100

    rows = []
    for ip in ips:
        ip_df = df_all[df_all["IP"] == ip]
        row = {"IP": ip}
        for disp, metric, media in METRICS:
            row[f"{disp}_절대"] = _abs_value(ip_df, metric, media)
            row[f"{disp}_기울기"] = _slope(ip_df, metric, media)
        rows.append(row)
    base = pd.DataFrame(rows)
    for disp, _, _ in METRICS:
        base[f"{disp}_절대등급"] = _quintile_grade(base[f"{disp}_절대"], ["S","A","B","C","D"])
        base[f"{disp}_상승등급"] = _quintile_grade(base[f"{disp}_기울기"], ["+2","+1","0","-1","-2"])
        base[f"{disp}_종합"]   = base[f"{disp}_절대등급"].astype(str) + base[f"{disp}_상승등급"].astype(str)
    base["_ABS_PCT_MEAN"]   = pd.concat([_to_percentile(base[f"{d}_절대"])   for d,_,_ in METRICS], axis=1).mean(axis=1)
    base["_SLOPE_PCT_MEAN"] = pd.concat([_to_percentile(base[f"{d}_기울기"]) for d,_,_ in METRICS], axis=1).mean(axis=1)
    base["종합_절대등급"] = _quintile_grade(base["_ABS_PCT_MEAN"],   ["S","A","B","C","D"])
    base["종합_상승등급"] = _quintile_grade(base["_SLOPE_PCT_MEAN"], ["+2","+1","0","-1","-2"])
    base["종합등급"] = base["종합_절대등급"].astype(str) + base["종합_상승등급"].astype(str)

    focus = base[base["IP"] == selected_ip].iloc[0]
    card_cols = st.columns([2, 1, 1, 1, 1])
    with card_cols[0]:
        st.markdown(
            f"""
            <div class="kpi-card" style="height:110px;border:2px solid #004a99;background:linear-gradient(180deg,#e8f0ff, #ffffff);">
              <div class="kpi-title" style="font-size:15px;color:#003d80;">종합등급</div>
              <div class="kpi-value" style="font-size:40px;color:#003d80;">{focus['종합등급'] if pd.notna(focus['종합등급']) else '–'}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    def _grade_card(col, title, val):
        with col:
            st.markdown(
                f"""
                <div class="kpi-card" style="height:110px;">
                  <div class="kpi-title">{title}</div>
                  <div class="kpi-value" style="font-size:28px;">{val if pd.notna(val) else '–'}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
    _grade_card(card_cols[1], "가구시청률 등급", focus["가구시청률_종합"])
    _grade_card(card_cols[2], "타깃시청률 등급", focus["타깃시청률_종합"])
    _grade_card(card_cols[3], "TVING LIVE 등급", focus["TVING LIVE_종합"])
    _grade_card(card_cols[4], "TVING VOD 등급",  focus["TVING VOD_종합"])

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    from plotly import graph_objects as go
    _ip_all = df_all[df_all["IP"] == selected_ip].copy()
    if "회차_numeric" in _ip_all.columns:
        _ip_all["ep"] = pd.to_numeric(_ip_all["회차_numeric"], errors="coerce")
    else:
        _ip_all["ep"] = pd.to_numeric(_ip_all["회차"].astype(str).str.extract(r"(\d+)", expand=False), errors="coerce")
    _ip_all["value_num"] = pd.to_numeric(_ip_all["value"], errors="coerce").replace(0, np.nan)
    _valid_eps = _ip_all.loc[_ip_all["value_num"].notna(), "ep"]
    if _valid_eps.notna().any():
        _max_ep = int(np.nanmax(_valid_eps)); _Ns = [n for n in EP_CHOICES if n <= _max_ep]
    else:
        _Ns = [min(EP_CHOICES)]

    def _abs_value_n(ip_df, metric, media, n):
        sub = _filter_to_ep(ip_df, n)
        if metric in ["H시청률", "T시청률"]:
            return mean_of_ip_episode_mean(sub, metric)
        if metric == "시청인구" and media == "LIVE":
            return mean_of_ip_episode_sum(sub, "시청인구", ["TVING LIVE"])
        if metric == "시청인구" and media == "VOD":
            return mean_of_ip_episode_sum(sub, "시청인구", ["TVING VOD"])
        return None

    def _slope_n(ip_df, metric, media, n):
        sub = ip_df[ip_df["metric"] == metric].copy()
        if media == "LIVE": sub = sub[sub["매체"] == "TVING LIVE"]
        elif media == "VOD": sub = sub[sub["매체"] == "TVING VOD"]
        sub = _filter_to_ep(sub, n)
        sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
        sub = sub.dropna(subset=["value", "회차_numeric"])
        if sub.empty: return np.nan
        if metric in ["H시청률", "T시청률"]:
            s = sub.groupby("회차_numeric")["value"].mean().reset_index()
        else:
            s = sub.groupby("회차_numeric")["value"].sum().reset_index()
        s = s.sort_values("회차_numeric")
        x = s["회차_numeric"].astype(float).values; y = s["value"].astype(float).values
        if len(x) < 2: return np.nan
        try: return float(np.polyfit(x, y, 1)[0])
        except Exception: return np.nan

    ABS_NUM = {"S":5, "A":4, "B":3, "C":2, "D":1}
    evo_rows = []
    for n in _Ns:
        tmp = []
        for ip in ips:
            ip_df = df_all[df_all["IP"] == ip]
            row = {"IP": ip}
            for disp, metric, media in METRICS:
                row[f"{disp}_절대"]   = _abs_value_n(ip_df, metric, media, n)
                row[f"{disp}_기울기"] = _slope_n(ip_df, metric, media, n)
            tmp.append(row)
        tmp = pd.DataFrame(tmp)
        for disp, _, _ in METRICS:
            tmp[f"{disp}_절대등급"] = _quintile_grade(tmp[f"{disp}_절대"],   ["S","A","B","C","D"])
            tmp[f"{disp}_상승등급"] = _quintile_grade(tmp[f"{disp}_기울기"], ["+2","+1","0","-1","-2"])
        tmp["_ABS_PCT_MEAN"]   = pd.concat([_to_percentile(tmp[f"{d}_절대"])   for d,_,_ in METRICS], axis=1).mean(axis=1)
        tmp["_SLOPE_PCT_MEAN"] = pd.concat([_to_percentile(tmp[f"{d}_기울기"]) for d,_,_ in METRICS], axis=1).mean(axis=1)
        tmp["종합_절대등급"] = _quintile_grade(tmp["_ABS_PCT_MEAN"],   ["S","A","B","C","D"])
        tmp["종합_상승등급"] = _quintile_grade(tmp["_SLOPE_PCT_MEAN"], ["+2","+1","0","-1","-2"])
        row = tmp[tmp["IP"] == selected_ip]
        if not row.empty and pd.notna(row.iloc[0]["종합_절대등급"]):
            ag = str(row.iloc[0]["종합_절대등급"])
            sg = str(row.iloc[0]["종합_상승등급"]) if pd.notna(row.iloc[0]["종합_상승등급"]) else ""
            evo_rows.append({"N": n, "회차라벨": f"{n}회차", "ABS_GRADE": ag, "SLOPE_GRADE": sg, "ABS_NUM": ABS_NUM.get(ag, np.nan)})
    evo = pd.DataFrame(evo_rows)
    if evo.empty:
        st.info("회차별 등급 추이를 표시할 데이터가 부족합니다.")
    else:
        fig_e = go.Figure()
        fig_e.add_vrect(x0=ep_cutoff - 0.5, x1=ep_cutoff + 0.5, fillcolor="rgba(0,90,200,0.12)", line_width=0)
        fig_e.add_trace(go.Scatter(x=evo["N"], y=evo["ABS_NUM"], mode="lines+markers",
                                   line=dict(shape="spline", width=3), marker=dict(size=8),
                                   name=selected_ip, hoverinfo="skip"))
        for xi, yi, ag, sg in zip(evo["N"], evo["ABS_NUM"], evo["ABS_GRADE"], evo["SLOPE_GRADE"]):
            label = f"{ag}{sg}" if isinstance(ag, str) and isinstance(sg, str) else ""
            fig_e.add_annotation(x=xi, y=yi, text=label, showarrow=False,
                                 font=dict(size=12, color="#333", family="sans-serif"), yshift=14)
        fig_e.update_xaxes(tickmode="array", tickvals=evo["N"].tolist(),
                           ticktext=[f"{int(n)}회차" for n in evo["N"].tolist()],
                           showgrid=False, zeroline=False, showline=False)
        fig_e.update_yaxes(tickmode="array", tickvals=[5,4,3,2,1], ticktext=["S","A","B","C","D"],
                           range=[0.7, 5.3], showgrid=False, zeroline=False, showline=False)
        fig_e.update_layout(height=200, margin=dict(l=8, r=8, t=8, b=8), showlegend=False)
        st.plotly_chart(fig_e, use_container_width=True, config={"displayModeBar": False})

    st.divider()
    st.markdown("#### 🗺️ 포지셔닝맵")
    pos_map = {(r, c): [] for r in ROW_LABELS for c in COL_LABELS}
    for _, r in base.iterrows():
        ra = str(r["종합_절대등급"]) if pd.notna(r["종합_절대등급"]) else None
        rs = str(r["종합_상승등급"]) if pd.notna(r["종합_상승등급"]) else None
        if ra in ROW_LABELS and rs in COL_LABELS:
            pos_map[(ra, rs)].append(r["IP"])
    z = []
    for rr in ROW_LABELS:
        row_z = []
        for cc in COL_LABELS:
            row_z.append((ABS_SCORE[rr] + SLO_SCORE[cc]) / 2.0)
        z.append(row_z)
    fig = px.imshow(z, x=COL_LABELS, y=ROW_LABELS, origin="upper", color_continuous_scale="Blues",
                    range_color=[1, 5], text_auto=False, aspect="auto").update_traces(xgap=0.0, ygap=0.0)
    fig.update_xaxes(showticklabels=False, title=None, ticks="")
    fig.update_yaxes(showticklabels=False, title=None, ticks="")
    fig.update_layout(height=760, margin=dict(l=2, r=2, t=2, b=2), coloraxis_showscale=False)
    fig.update_traces(hovertemplate="<extra></extra>")
    def _font_color(val: float) -> str: return "#FFFFFF" if val >= 3.3 else "#111111"
    for r_idx, rr in enumerate(ROW_LABELS):
        for c_idx, cc in enumerate(COL_LABELS):
            cell_val = z[r_idx][c_idx]
            names = pos_map[(rr, cc)]
            color = _font_color(cell_val)
            fig.add_annotation(x=cc, y=rr, xref="x", yref="y", text=f"<b style='letter-spacing:0.5px'>{rr}{cc}</b>",
                               showarrow=False, font=dict(size=22, color=color, family="sans-serif"),
                               xanchor="center", yanchor="top", xshift=0, yshift=80, align="left")
            if names:
                fig.add_annotation(x=cc, y=rr, xref="x", yref="y",
                                   text=f"<span style='line-height:1.04'>{'<br>'.join(names)}</span>",
                                   showarrow=False, font=dict(size=12, color=color, family="sans-serif"),
                                   xanchor="center", yanchor="middle", yshift=6)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.divider()
    st.markdown("#### 📋 IP 전체 표 (정렬: 종합 절대등급 ↓, 상승등급 ↓)")
    sort_abs = {"S":5,"A":4,"B":3,"C":2,"D":1}
    sort_slo = {"+2":5,"+1":4,"0":3,"-1":2,"-2":1}
    table = base.copy()
    table["종합_절대_점수"] = table["종합_절대등급"].map(sort_abs)
    table["종합_상승_점수"] = table["종합_상승등급"].map(sort_slo)
    table = table.sort_values(["종합_절대_점수","종합_상승_점수"], ascending=False)
    view_cols = ["IP","종합_절대등급","종합_상승등급","종합등급",
                 "가구시청률_종합","타깃시청률_종합","TVING LIVE_종합","TVING VOD_종합"]
    table = table[view_cols]
    gb = GridOptionsBuilder.from_dataframe(table)
    gb.configure_default_column(sortable=True, resizable=True, filter=False,
                                cellStyle={'textAlign': 'center'}, headerClass='centered-header bold-header')
    gb.configure_column("IP", cellStyle={'textAlign':'left'})
    grid_options = gb.build()
    AgGrid(table, gridOptions=grid_options, theme="streamlit", height=450, fit_columns_on_grid_load=True,
           update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True)
#endregion


#region [ 14. 라우팅 ]
# =====================================================
if current_page == "Overview":
    render_overview()
elif current_page == "IP 성과":
    render_ip_detail()
elif current_page == "데모그래픽":
    render_demographic()
elif current_page == "비교분석":
    render_comparison()
elif current_page == "회차별":
    render_episode()
elif current_page == "성장스코어":
    render_growth_score()
else:
    st.info("페이지를 선택하세요.")
# =====================================================
#endregion
