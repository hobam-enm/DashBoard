# Dashboard_Cloud.py
# =====================================================
# 📊 Overview / IP 성과 대시보드 — v2.0 (Cloud setup)
# - 시트/인증은 전부 st.secrets에서만 읽음
# - 승인 팝업 없음(서비스계정)
# - 로컬판 7페이지 네비 구조 유지
# - 각 페이지 렌더러는 기존 이름을 우선 호출 (동일 로직 유지)
# =====================================================

#region [ 1. 라이브러리 임포트 ]
# =====================================================
import re
import inspect
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import streamlit as st
from plotly import graph_objects as go
import plotly.io as pio
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# 구글 인증
from google.oauth2.service_account import Credentials
import gspread
# =====================================================
#endregion


#region [ 2. 기본 설정 및 공통 상수 ]
# =====================================================
st.set_page_config(page_title="Overview Dashboard", layout="wide", initial_sidebar_state="expanded")

# ===== 네비게이션 아이템 정의 (v2.0) — 로컬판과 동일 =====
NAV_ITEMS = {
    "Overview": "📊 Overview",
    "IP 성과": "📈 IP 성과 자세히보기",
    "데모그래픽": "👥 IP 오디언스 히트맵",
    "비교분석": "⚖️ IP간 비교분석",
    "성장스코어-방영지표": "🚀 성장스코어-방영지표",
    "성장스코어-디지털": "🛰️ 성장스코어-디지털",
    "회차별": "🎬 회차별 비교",
}
DEFAULT_PAGE = "Overview"

# ===== 시크릿에서 시트/워크시트 식별자 로드 =====
def _get_sheet_settings():
    # 권장 구조: st.secrets["sheets"]={SHEET_ID, RAW_WORKSHEET}
    sheet_id = None
    worksheet = None

    if "sheets" in st.secrets:
        ss = st.secrets["sheets"]
        sheet_id = ss.get("SHEET_ID")
        worksheet = ss.get("RAW_WORKSHEET")

    # 백업: 최상위 키 지원
    if sheet_id is None:
        sheet_id = st.secrets.get("SHEET_ID")
    if worksheet is None:
        worksheet = st.secrets.get("RAW_WORKSHEET")

    if not sheet_id or not worksheet:
        raise RuntimeError("시트 설정 누락: st.secrets에 SHEET_ID와 RAW_WORKSHEET를 넣어주세요. (권장: [sheets] 섹션)")

    return str(sheet_id), str(worksheet)

SHEET_ID, RAW_WORKSHEET = _get_sheet_settings()

# ===== Plotly 공통 테마 (로컬판과 동일 톤) =====
dashboard_theme = go.Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='sans-serif', size=12, color='#333333'),
    title=dict(font=dict(size=16, color="#111"), x=0.05),
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, bgcolor='rgba(0,0,0,0)'),
    margin=dict(l=20, r=20, t=50, b=20),
    xaxis=dict(showgrid=False, zeroline=True, zerolinecolor='#e0e0e0', zerolinewidth=1),
    yaxis=dict(showgrid=True, gridcolor='#f0f0f0', zeroline=True, zerolinecolor='#e0e0e0'),
)
pio.templates['dashboard_theme'] = go.layout.Template(layout=dashboard_theme)
pio.templates.default = 'dashboard_theme'
# =====================================================
#endregion


#region [ 3. 구글 시트 인증/연결 ]
# =====================================================
_GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _load_service_account_info() -> dict:
    """
    st.secrets에서 서비스계정 정보를 dict로 로드:
      1) st.secrets["gcp_service_account"] (dict 권장)
      2) st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"] (string JSON)
      3) st.secrets 값 중 {"type":"service_account"} 를 가진 dict
    """
    # 1) 명시 dict
    if "gcp_service_account" in st.secrets:
        info = st.secrets["gcp_service_account"]
        if isinstance(info, dict) and info.get("type") == "service_account":
            return info

    # 2) string JSON
    if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
        import json
        try:
            return json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON 파싱 실패: {e}")

    # 3) 값 스캔
    for v in st.secrets.values():
        if isinstance(v, dict) and v.get("type") == "service_account":
            return v

    raise RuntimeError("서비스계정 시크릿 누락: gcp_service_account(or GOOGLE_SERVICE_ACCOUNT_JSON)를 추가하세요.")

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=_GOOGLE_SCOPES)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_worksheet(sheet_id: str, gid_or_name: str):
    """
    gid_or_name 이 숫자면 워크시트ID, 아니면 탭명으로 접근.
    """
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = None

    # 숫자(워크시트 ID) 케이스
    try:
        if str(gid_or_name).isdigit():
            try:
                ws = sh.get_worksheet_by_id(int(gid_or_name))
            except Exception:
                # gspread 버전 호환
                for _ws in sh.worksheets():
                    if int(_ws.id) == int(gid_or_name):
                        ws = _ws
                        break
        else:
            ws = sh.worksheet(gid_or_name)
    except Exception:
        ws = None

    if ws is None:
        ws = sh.get_worksheet(0)
    return ws

WS = open_worksheet(SHEET_ID, RAW_WORKSHEET)
# =====================================================
#endregion


#region [ 4. 공통 함수: 데이터 로드 / 유틸리티 ]
# =====================================================
def _has_service_account() -> bool:
    try:
        if "gcp_service_account" in st.secrets:
            return True
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
            return True
        for _, v in st.secrets.items():
            if isinstance(v, dict) and v.get("type") == "service_account":
                return True
    except Exception:
        pass
    return False

def _read_dataframe_via_gspread(ws) -> pd.DataFrame:
    """
    gspread 워크시트로부터 DataFrame 구성 (헤더 1행 가정)
    """
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:] if len(values) > 1 else []
    return pd.DataFrame(rows, columns=header)

@st.cache_data(ttl=600, show_spinner=True)
def load_data() -> pd.DataFrame:
    """
    데이터 로드(비공개/서비스계정 전제)
    """
    if not _has_service_account():
        raise RuntimeError("서비스계정 시크릿이 없습니다. st.secrets에 gcp_service_account를 추가하세요.")

    if WS is None:
        raise RuntimeError("워크시트 핸들이 없습니다. SHEET_ID/RAW_WORKSHEET 설정을 확인하세요.")

    df = _read_dataframe_via_gspread(WS)

    # --- 날짜 파싱 ---
    if "주차시작일" in df.columns:
        df["주차시작일"] = pd.to_datetime(
            df["주차시작일"].astype(str).str.strip(),
            format="%Y. %m. %d", errors="coerce"
        )
    if "방영시작일" in df.columns:
        df["방영시작일"] = pd.to_datetime(
            df["방영시작일"].astype(str).str.strip(),
            format="%Y. %m. %d", errors="coerce"
        )

    # --- 숫자형 데이터 변환 ---
    if "value" in df.columns:
        v = df["value"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
        df["value"] = pd.to_numeric(v, errors="coerce").fillna(0)

    # --- 문자열 데이터 정제 ---
    for c in ["IP", "편성", "지표구분", "매체", "데모", "metric", "회차", "주차"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # --- 파생 컬럼 생성 ---
    if "회차" in df.columns:
        df["회차_numeric"] = df["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    else:
        df["회차_numeric"] = pd.NA

    return df

def _get_query_params() -> dict:
    """Streamlit 버전별 query_params 호환 래퍼."""
    try:
        qp = st.query_params
        if hasattr(qp, "to_dict"):
            return dict(qp.to_dict())
        return dict(qp)
    except Exception:
        return dict(st.experimental_get_query_params())

def _set_query_params(**kwargs):
    """?page=... 등 쿼리 파라미터 갱신."""
    try:
        st.query_params.clear()
        for k, v in kwargs.items():
            if v is not None:
                st.query_params[k] = v
    except Exception:
        st.experimental_set_query_params(**kwargs)

def get_current_page_default(default_page: str) -> str:
    qp = _get_query_params()
    page = qp.get("page") or qp.get("PAGE") or default_page
    if isinstance(page, list):
        page = page[0] if page else default_page
    if page not in NAV_ITEMS:
        page = default_page
    return page

def set_current_page(page_key: str):
    if page_key not in NAV_ITEMS:
        page_key = DEFAULT_PAGE
    _set_query_params(page=page_key)
    st.rerun()
# =====================================================
#endregion


#region [ 5. 공통 스타일 (로컬판 유지) ]
# =====================================================
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background-color: #f8f9fa; }
div[data-testid="stVerticalBlockBorderWrapper"] {
  background-color: #ffffff; border: 1px solid #e9e9e9; border-radius: 10px;
  box-shadow: 0 2px 5px rgba(0,0,0,0.03); padding: 1.25rem 1.25rem 1.5rem 1.25rem; margin-bottom: 1.5rem;
}
section[data-testid="stSidebar"] {
  background:#fff; border-right:1px solid #e0e0e0; padding-top:1rem; padding-left:.5rem; padding-right:.5rem;
  min-width:300px !important; max-width:300px !important;
}
div[data-testid="collapsedControl"] { display:none !important; }
.sidebar-logo{ font-size:28px; font-weight:700; color:#1a1a1a; text-align:center; margin-bottom:10px; padding-top:10px;}
.nav-item{ display:block; width:100%; padding:12px 15px; color:#333 !important; background:#f1f3f5; text-decoration:none !important;
  font-weight:600; border-radius:8px; margin-bottom:5px; text-align:center; transition: background-color .2s ease, color .2s ease; }
.nav-item:hover{ background:#e9ecef; color:#000 !important; text-decoration:none; }
.active{ background:#004a99; color:#fff !important; text-decoration:none; font-weight:700; }
.active:hover{ background:#003d80; color:#fff !important; }

.kpi-card{ background:#fff; border:1px solid #e9e9e9; border-radius:10px; padding:20px 15px; text-align:center;
  box-shadow:0 2px 5px rgba(0,0,0,0.03); height:100%; display:flex; flex-direction:column; justify-content:center; }
.kpi-title{ font-size:15px; font-weight:600; margin-bottom:10px; color:#444; }
.kpi-value{ font-size:28px; font-weight:700; color:#000; line-height:1.2; }

.kpi-subwrap{ margin-top:10px; line-height:1.4; }
.kpi-sublabel{ font-size:12px; font-weight:500; color:#555; letter-spacing:.1px; margin-right:6px; }
.kpi-substrong{ font-size:14px; font-weight:700; color:#111; }
.kpi-subpct{ font-size:14px; font-weight:700; }

.ag-theme-streamlit{ font-size:13px; }
.ag-theme-streamlit .ag-root-wrapper{ border-radius:8px; }
.ag-theme-streamlit .ag-row-hover{ background-color:#f5f8ff !important; }
.ag-theme-streamlit .ag-header-cell-label{ justify-content:center !important; }
.ag-theme-streamlit .centered-header .ag-header-cell-label{ justify-content:center !important; }
.ag-theme-streamlit .centered-header .ag-sort-indicator-container{ margin-left:4px; }
.ag-theme-streamlit .bold-header .ag-header-cell-text{ font-weight:700 !important; font-size:13px; color:#111; }

.sec-title{ font-size:20px; font-weight:700; color:#111; margin:0 0 10px 0; padding-bottom:0; border-bottom:none; }
div[data-testid="stMultiSelect"], div[data-testid="stSelectbox"] { margin-top:-10px; }
h3{ margin-top:-15px; margin-bottom:10px; }
h4{ font-weight:700; color:#111; margin-top:0rem; margin-bottom:.5rem; }
hr{ margin:1.5rem 0; background-color:#e0e0e0; }
</style>
""", unsafe_allow_html=True)
# =====================================================
#endregion


#region [ 6. 사이드바 네비게이션 ]
# =====================================================
current_page = get_current_page_default(DEFAULT_PAGE)
st.session_state["page"] = current_page

with st.sidebar:
    st.markdown('<hr style="margin:0px 0; border:1px solid #ccc;">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-logo">📊 DashBoard</div>', unsafe_allow_html=True)
    st.markdown('<hr style="margin:0px 0; border:1px solid #ccc;">', unsafe_allow_html=True)
    for key, label in NAV_ITEMS.items():
        active_class = "active" if current_page == key else ""
        st.markdown(f'<a class="nav-item {active_class}" href="?page={key}" target="_self">{label}</a>', unsafe_allow_html=True)
# =====================================================
#endregion


#region [ 7. 렌더 유틸 (기존 함수 호출 래퍼) ]
# =====================================================
def _safe_call(func_name: str, **kwargs):
    """
    전역에 func_name이 있으면 해당 함수를 호출하고 결과를 반환.
    없으면 None.
    """
    f = globals().get(func_name)
    if callable(f):
        sig = inspect.signature(f)
        call_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return f(**call_kwargs)
    return None

def route_and_render(page_key: str, *, ws=None, df=None):
    """
    페이지 라우팅 → 기존 렌더러 이름을 우선 호출.
    (동일 로직 유지, 이름만 다양한 케이스를 안전하게 커버)
    """
    if page_key == "Overview":
        if _safe_call("render_overview", ws=ws, df=df) is None:
            _safe_call("overview_render", ws=ws, df=df)

    elif page_key == "IP 성과":
        # 주 사용: render_ip_performance / render_ip_detail
        if _safe_call("render_ip_performance", ws=ws, df=df) is None:
            if _safe_call("render_ip_detail", ws=ws, df=df) is None:
                _safe_call("render_ip성공", ws=ws, df=df)

    elif page_key == "데모그래픽":
        if _safe_call("render_demographic", ws=ws, df=df) is None:
            _safe_call("render_demographics", ws=ws, df=df)

    elif page_key == "비교분석":
        if _safe_call("render_comparison", ws=ws, df=df) is None:
            _safe_call("render_compare", ws=ws, df=df)

    elif page_key == "성장스코어-방영지표":
        # 예시 이름: render_growth_score_broadcast / render_growth_onair / render_growth_broadcast
        if _safe_call("render_growth_score_broadcast", ws=ws, df=df) is None:
            if _safe_call("render_growth_onair", ws=ws, df=df) is None:
                _safe_call("render_growth_broadcast", ws=ws, df=df)

    elif page_key == "성장스코어-디지털":
        # 예시 이름: render_growth_score_digital / render_growth_digital
        if _safe_call("render_growth_score_digital", ws=ws, df=df) is None:
            _safe_call("render_growth_digital", ws=ws, df=df)

    elif page_key == "회차별":
        if _safe_call("render_episode", ws=ws, df=df) is None:
            if _safe_call("render_episode_page", ws=ws, df=df) is None:
                _safe_call("render_episodes", ws=ws, df=df)

    else:
        st.warning("알 수 없는 페이지입니다. Overview로 이동합니다.")
        set_current_page("Overview")
# =====================================================
#endregion


#region [ 8. 메인 실행부 ]
# =====================================================
try:
    df = load_data()
except Exception as e:
    st.error(f"데이터 로드 오류: {e}")
    df = pd.DataFrame()

route_and_render(current_page, ws=WS, df=df)
# =====================================================
#endregion
