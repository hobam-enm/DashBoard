# 📊 Overview / IP 성과 대시보드 — v2.0 


#region [ 1. 라이브러리 임포트 ]
# =====================================================
import re
from typing import List, Dict, Any, Optional 
import time, uuid
import textwrap
import hashlib
import datetime
import numpy as np
import pandas as pd
import plotly.express as px
from plotly import graph_objects as go
import plotly.io as pio
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
import gspread
from google.oauth2.service_account import Credentials
import extra_streamlit_components as stx  # [추가] 쿠키 매니저 라이브러리
#endregion


#region [ 1-0. 페이지 설정  ]
# =====================================================
st.set_page_config(
    page_title="Drama Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)
#endregion


#region [ 1-1. 입장게이트 - 쿠키 인증 ]#region [ 1-1. 입장게이트 - 쿠키 인증 (세션 보완) ]
# =====================================================
# [수정] _rerun 함수 복구
def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

# 쿠키 이름 및 유효기간 설정
COOKIE_NAME = "dmb_auth_token"
COOKIE_EXPIRY_DAYS = 1

# [수정] 캐시 제거 (위젯 오류 방지)
def get_cookie_manager():
    return stx.CookieManager(key="dmb_cookie_manager")

def _hash_password(password: str) -> str:
    return hashlib.sha256(str(password).encode()).hexdigest()

def check_password_with_cookie() -> bool:
    cookie_manager = get_cookie_manager()
    
    # 1. Streamlit Secrets 확인
    secret_pwd = st.secrets.get("DASHBOARD_PASSWORD")
    if not secret_pwd:
        st.error("설정 파일(.streamlit/secrets.toml)에 'DASHBOARD_PASSWORD'가 없습니다.")
        st.stop()
        
    hashed_secret = _hash_password(str(secret_pwd))
    
    # 2. 쿠키 읽기
    cookies = cookie_manager.get_all()
    current_token = cookies.get(COOKIE_NAME)
    
    # 3. [핵심 수정] 인증 검사 (쿠키 OR 세션스테이트 둘 중 하나라도 통과면 OK)
    # 쿠키가 있거나, 방금 로그인을 성공해서 세션에 기록이 남아있다면 통과
    is_cookie_valid = (current_token == hashed_secret)
    is_session_valid = st.session_state.get("auth_success", False)
    
    if is_cookie_valid or is_session_valid:
        # 쿠키가 유효하면 세션도 True로 갱신 (새로고침 대비)
        if is_cookie_valid:
            st.session_state["auth_success"] = True
        return True

    # 4. 로그인 UI
    with st.sidebar:
        st.markdown("## 🔐 로그인")
        input_pwd = st.text_input("비밀번호를 입력하세요", type="password", key="__login_pwd__")
        login_btn = st.button("로그인")

    # 5. 로그인 처리
    if login_btn:
        if _hash_password(input_pwd) == hashed_secret:
            # A. 쿠키 굽기 (브라우저 저장용)
            expires = datetime.datetime.now() + datetime.timedelta(days=COOKIE_EXPIRY_DAYS)
            cookie_manager.set(COOKIE_NAME, hashed_secret, expires_at=expires)
            
            # B. [핵심] 세션에 '로그인 성공' 도장 찍기 (쿠키 딜레이 방어용)
            st.session_state["auth_success"] = True
            
            st.success("로그인 성공! 잠시 후 이동합니다.")
            time.sleep(1.5) # 딜레이를 약간 늘림 (안정성 확보)
            _rerun()
        else:
            st.sidebar.warning("비밀번호가 일치하지 않습니다.")
            
    return False

if not check_password_with_cookie():
    st.stop()
#endregion


#region [ 2. 공통 스타일 통합 ]
# =====================================================

st.markdown("""
<style>
            
 /* -------------------------------------------------------------------
   0. [추가] 스트림릿 기본 헤더(Toolbar) 숨기기
   ------------------------------------------------------------------- */
header[data-testid="stHeader"] {
    display: none !important; /* 상단 헤더 영역 전체 숨김 */
}
div[data-testid="stDecoration"] {
    display: none !important; /* 상단 컬러 데코레이션 바 숨김 */
}
                       
/* -------------------------------------------------------------------
   1. 앱 전체 기본 설정
   ------------------------------------------------------------------- */
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');

html, body, [class*="css"] {
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, sans-serif !important;
}

/* 페이지 배경: 흰색 */
[data-testid="stAppViewContainer"] {
    background-color: #f9fafb !important;
    background-image: none !important;
}

/* 상단 여백 */
.block-container {
    padding-top: 2rem;
    padding-bottom: 5rem;
    max-width: 1600px !important;
}


/* -------------------------------------------------------------------
   2. 사이드바 스타일 
   ------------------------------------------------------------------- */
section[data-testid="stSidebar"] {
    background-color: #ffffff !important; 
    border-right: 1px solid #e0e0e0;
    box-shadow: 4px 0 15px rgba(0, 0, 0, 0.1); /* 오른쪽(10px)으로 퍼지는 연한 그림자 */
    min-width: 280px !important;
    max-width: 280px !important;
    padding-top: 1rem;
    padding-left: 0 !important;
    padding-right: 0 !important;
}

/* 내부 여백 정리 */
section[data-testid="stSidebar"] .block-container,
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
    padding-left: 0 !important;
    padding-right: 0 !important;
    width: 100% !important;
}

/* 내부 카드 효과 제거 */
section[data-testid="stSidebar"] div[data-testid="stVerticalBlockBorderWrapper"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    transform: none !important;
}

/* [핵심 1] 버튼 컨테이너 틈 제거 */
section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] {
    gap: 0rem !important;
}

section[data-testid="stSidebar"] .stButton {
    margin: 0 !important;
    padding: 0 !important;
    width: 100% !important;
}

/* [핵심 2] 버튼 스타일: 패딩을 8px로 확 줄여서 '다닥다닥' 구현 */
section[data-testid="stSidebar"] .stButton > button {
    width: 100%;
    box-sizing: border-box;
    text-align: left;
    
    padding: 16px 20px !important;  /* [수정] 높이 축소 */
    margin: 0 !important;
    
    border-radius: 0px !important;
    border: none !important;
    border-bottom: 1px solid #e9ecef !important; /* 연한 구분선 */
    
    background: transparent !important;
    color: #333333 !important;
    font-weight: 600;
    
    box-shadow: none !important;
    transition: background-color 0.15s;
}

/* 버튼 호버 */
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #e5e7eb !important;
    color: #000000 !important;
}

/* 선택된 버튼 (Active): 파란 배경 + 흰색 글씨 */
section[data-testid="stSidebar"] [data-testid="baseButton-primary"] > button,
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background: #0b61ff !important;    
    color: #ffffff !important;         
    border-bottom: 1px solid #0b61ff !important;
    font-weight: 700;
}

section[data-testid="stSidebar"] button svg { display: none !important; }

/* 사이드바 텍스트 여백 */
section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, 
section[data-testid="stSidebar"] h3, section[data-testid="stSidebar"] .stMarkdown, 
section[data-testid="stSidebar"] .stSelectbox, section[data-testid="stSidebar"] .stMultiSelect {
    padding-left: 0px !important;
    padding-right: 0px !important;
}

/* [핵심 3] 사이드바 제목: 꽉 차고 크게 */
.page-title-wrap { 
    display: flex; 
    align-items: center; 
    justify-content: center; 
    gap: 8px; 
    margin: 10px 0 20px 0; 
    padding: 0 0px;
    width: 100%;
}
.page-title-emoji { font-size: 26px; line-height: 1; }
.page-title-main {
    font-size: 26px; /* [수정] 폰트 크기 확대 */
    font-weight: 800; 
    letter-spacing: -0.5px;
    line-height: 1.2;
    background: linear-gradient(90deg, #6A5ACD, #FF7A8A);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    text-align: center;
    width: 100%;
    white-space: nowrap; /* 줄바꿈 방지 */
}


/* -------------------------------------------------------------------
   3. 메인 컨텐츠 카드 
   ------------------------------------------------------------------- */
div[data-testid="stVerticalBlockBorderWrapper"] {
    background-color: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.03);
    
    padding: 1.5rem;
    margin-bottom: 1.5rem;
    
    /* 들썩임 방지 */
    transition: transform 0.25s ease, box-shadow 0.25s ease;
    will-change: transform, box-shadow;
    backface-visibility: hidden; 
}

/* 마우스 올렸을 때 플로팅 */
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 24px rgba(0,0,0,0.08);
    border-color: #d1d5db;
    z-index: 5;
}

/* 투명 예외 처리 */
div[data-testid="stVerticalBlockBorderWrapper"]:has(.kpi-card),
div[data-testid="stVerticalBlockBorderWrapper"]:has(.page-title),
div[data-testid="stVerticalBlockBorderWrapper"]:has(h1),
div[data-testid="stVerticalBlockBorderWrapper"]:has(h2),
div[data-testid="stVerticalBlockBorderWrapper"]:has(h3),
div[data-testid="stVerticalBlockBorderWrapper"]:has(div[data-testid="stSelectbox"]),
div[data-testid="stVerticalBlockBorderWrapper"]:has(div[data-testid="stMultiSelect"]),
div[data-testid="stVerticalBlockBorderWrapper"]:has(div[data-testid="stSlider"]),
div[data-testid="stVerticalBlockBorderWrapper"]:has(div[data-testid="stRadio"]),
div[data-testid="stVerticalBlockBorderWrapper"]:has(.filter-group),
div[data-testid="stVerticalBlockBorderWrapper"]:has(.mode-switch) {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    margin-bottom: 0.5rem !important;
    transform: none !important; 
}


/* -------------------------------------------------------------------
   4. 기타 컴포넌트
   ------------------------------------------------------------------- */
h1, h2, h3 { color: #111827; font-weight: 800; letter-spacing: -0.02em; }

.page-title {
    font-size: 28px;
    font-weight: 800;
    display: inline-flex; align-items: center; gap: 10px;
    margin: 10px 0 20px 0;
}

/* KPI 카드 (자체 플로팅) */
.kpi-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 20px 15px;
    text-align: center;
    box-shadow: 0 2px 5px rgba(0,0,0,0.03); 
    height: 100%;
    display: flex; flex-direction: column; justify-content: center;
    
    transition: transform 0.25s ease, box-shadow 0.25s ease;
    will-change: transform, box-shadow;
}
.kpi-card:hover { 
    transform: translateY(-4px); 
    box-shadow: 0 12px 24px rgba(0,0,0,0.08);
    border-color: #d1d5db;
}

.kpi-title { font-size: 14px; font-weight: 600; color: #6b7280; margin-bottom: 8px; }
.kpi-value { font-size: 26px; font-weight: 800; color: #111827; line-height: 1.2; }
.kpi-subwrap { margin-top: 8px; font-size: 12px; color: #9ca3af; }

.ag-theme-streamlit .ag-header { 
    background-color: #f9fafb; font-weight: 700; color: #374151; 
    border-bottom: 1px solid #e5e7eb;
}

</style>
""", unsafe_allow_html=True)
#endregion


#region [ 2.1. 기본 설정 및 공통 상수 ]
# =====================================================

# ===== 네비게이션 아이템 정의 =====
# [수정] 데모그래픽, 회차별 메뉴 제거 및 통합 반영
NAV_ITEMS = {
    "Overview": "Overview",
    "IP 성과": "IP 성과 자세히보기",
    "비교분석": "성과 비교분석", 
    "성장스코어-방영지표": "성장스코어-방영지표",
    "성장스코어-디지털": "성장스코어-디지털",
}

# ===== 데모 컬럼 순서 (페이지 2, 3에서 공통 사용) =====
DECADES = ["10대","20대","30대","40대","50대","60대"]
DEMO_COLS_ORDER = [f"{d}남성" for d in DECADES] + [f"{d}여성" for d in DECADES]

# ===== Plotly 공통 테마 설정 =====
dashboard_theme = go.Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='sans-serif', size=12, color='#333333'),
    title=dict(font=dict(size=16, color="#111"), x=0.05),
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='right',
        x=1,
        bgcolor='rgba(0,0,0,0)'
    ),
    margin=dict(l=20, r=20, t=50, b=20),
    xaxis=dict(
        showgrid=False, 
        zeroline=True, 
        zerolinecolor='#e0e0e0', 
        zerolinewidth=1
    ),
    yaxis=dict(
        showgrid=True, 
        gridcolor='#f0f0f0',
        zeroline=True, 
        zerolinecolor='#e0e0e0'
    ),
)
pio.templates['dashboard_theme'] = go.layout.Template(layout=dashboard_theme)
pio.templates.default = 'dashboard_theme'
# =====================================================
#endregion


#region [ 3. 공통 함수: 데이터 로드 / 유틸리티 ]
# =====================================================

# ===== 3.1. 데이터 로드 (gspread) =====
@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    """
    [수정] Streamlit Secrets와 gspread를 사용하여 비공개 Google Sheet에서 데이터를 인증하고 로드합니다.
    st.secrets에 'gcp_service_account', 'SHEET_ID', 'SHEET_NAME'이 있어야 합니다.
    """
    
    # --- 1. Google Sheets 인증 ---
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    try:
        creds_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(creds)

        # --- 2. 데이터 로드 ---
        sheet_id = st.secrets["SHEET_ID"]
        worksheet_name = st.secrets["SHEET_NAME"] 
        
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        data = worksheet.get_all_records() 
        df = pd.DataFrame(data)

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Streamlit Secrets의 SHEET_NAME 값 ('{worksheet_name}')에 해당하는 워크시트를 찾을 수 없습니다.")
        return pd.DataFrame()
    except KeyError as e:
        st.error(f"Streamlit Secrets에 필요한 키({e})가 없습니다. TOML 설정을 확인하세요.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Google Sheets 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()

    # --- 3. 데이터 전처리 (원본 코드와 동일) ---
    if "주차시작일" in df.columns:
        df["주차시작일"] = pd.to_datetime(
            df["주차시작일"].astype(str).str.strip(),
            format="%Y. %m. %d", 
            errors="coerce"
        )
    if "방영시작일" in df.columns:
        df["방영시작일"] = pd.to_datetime(
            df["방영시작일"].astype(str).str.strip(),
            format="%Y. %m. %d", 
            errors="coerce"
        )

    if "value" in df.columns:
        v = df["value"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
        df["value"] = pd.to_numeric(v, errors="coerce").fillna(0)

    for c in ["IP", "편성", "지표구분", "매체", "데모", "metric", "회차", "주차"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip() 

    if "회차" in df.columns:
        df["회차_numeric"] = df["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    else:
        df["회차_numeric"] = pd.NA

    return df

# ===== 3.2. UI / 포맷팅 헬퍼 함수 =====

def fmt(v, digits=3, intlike=False):
    """
    숫자 포맷팅 헬퍼 (None이나 NaN은 '–'로 표시)
    """
    if v is None or pd.isna(v):
        return "–"
    return f"{v:,.0f}" if intlike else f"{v:.{digits}f}"

def kpi(col, title, value):
    """
    Streamlit 컬럼 내에 KPI 카드를 렌더링합니다. (CSS .kpi-card 필요)
    """
    with col:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-title">{title}</div>'
            f'<div class="kpi-value">{value}</div></div>',
            unsafe_allow_html=True
        )

def render_gradient_title(main_text: str, emoji: str = "🎬"):
    """
    사이드바용 그라디언트 타이틀을 렌더링합니다. (CSS .page-title-wrap 필요)
    """
    st.markdown(
        f"""
        <div class="page-title-wrap">
          <span class="page-title-emoji">{emoji}</span>
          <span class="page-title-main">{main_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ===== 3.3. 페이지 라우팅 / 데이터 헬퍼 함수 =====

def get_current_page_default(default="Overview"):
    """
    URL 쿼리 파라미터(?page=...)에서 현재 페이지를 읽어옵니다.
    """
    try:
        qp = st.query_params
        p = qp.get("page", None)
        if p is None:
            return default
        return p if isinstance(p, str) else p[0]
    except Exception:
        qs = st.experimental_get_query_params()
        return (qs.get("page", [default])[0])

def _set_page_query_param(page_key: str):
    """
    URL 쿼리 파라미터에 page 키를 설정합니다. (리로드 없음)
    """
    try:
        qp = st.query_params
        qp["page"] = page_key
        st.query_params = qp
    except Exception:
        st.experimental_set_query_params(page=page_key)

def get_episode_options(df: pd.DataFrame) -> List[str]:
    """데이터에서 사용 가능한 회차 목록 (문자열, '00' 제외, '차'/'화' 제거)을 추출합니다."""
    
    valid_options = []
    if "회차_numeric" in df.columns:
        unique_episodes_num = sorted([
            int(ep) for ep in df["회차_numeric"].dropna().unique() if ep > 0
        ])
        if unique_episodes_num:
            max_ep_num = unique_episodes_num[-1]
            for ep_num in unique_episodes_num: valid_options.append(str(ep_num))
            last_ep_str_num = str(max_ep_num)
            if last_ep_str_num in valid_options and valid_options[-1] != last_ep_str_num:
                 valid_options.remove(last_ep_str_num); valid_options.append(last_ep_str_num)
            if len(valid_options) > 0 and "(마지막화)" not in valid_options[-1]:
                 valid_options[-1] = f"{valid_options[-1]} (마지막화)"
            return valid_options
        else: return []
    elif "회차" in df.columns:
        raw_options = sorted(df["회차"].dropna().unique())
        for opt in raw_options:
            if not opt.startswith("00"):
                cleaned_opt = re.sub(r"[화차]", "", opt)
                if cleaned_opt.isdigit() and int(cleaned_opt) > 0: 
                    valid_options.append(cleaned_opt)
        return sorted(list(set(valid_options)), key=lambda x: int(x) if x.isdigit() else float('inf')) 
    else: return []

# [신규] 피드백 3번 반영: 조회수 필터 로직 통합
def _get_view_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    '조회수' metric만 필터링하고, 유튜브 PGC/UGC 규칙을 적용하는 공통 유틸.
    """
    sub = df[df["metric"] == "조회수"].copy()
    if sub.empty:
        return sub
        
    if "매체" in sub.columns and "세부속성1" in sub.columns:
        yt_mask = (sub["매체"] == "유튜브")
        attr_mask = sub["세부속성1"].isin(["PGC", "UGC"])
        sub = sub[~yt_mask | (yt_mask & attr_mask)]
    
    return sub
#endregion


#region [ 4. 사이드바 네비게이션 ]
# =====================================================
current_page = get_current_page_default("Overview")
st.session_state["page"] = current_page

with st.sidebar:

    render_gradient_title("드라마 성과 대시보드", emoji="")
    st.markdown(
        "<p class='sidebar-contact' style='font-size:12px; color:gray;'>문의 : 미디어)디지털마케팅팀 데이터파트</p>",
        unsafe_allow_html=True
    )
    st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
    st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)

    for key, label in NAV_ITEMS.items():
        is_active = (current_page == key)
        
        wrapper_cls = "nav-active" if is_active else "nav-inactive"
        st.markdown(f'<div class="{wrapper_cls}">', unsafe_allow_html=True)

        clicked = st.button(
            label,
            key=f"navbtn__{key}",
            use_container_width=True,
            type=("primary" if is_active else "secondary")
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        if clicked and not is_active:
            st.session_state["page"] = key
            _set_page_query_param(key)
            _rerun()
#endregion


#region [ 5. 공통 집계 유틸: KPI 계산 ]
# =====================================================

def _episode_col(df: pd.DataFrame) -> str:
    """데이터프레임에 존재하는 회차 숫자 컬럼명을 반환합니다."""
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
    
    if metric_name == "조회수":
        sub = _get_view_data(df) 
    else:
        sub = df[df["metric"] == metric_name].copy()

    if media is not None:
        sub = sub[sub["매체"].isin(media)]
    
    if sub.empty:
        return None
        
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])

    per_ip_sum = sub.groupby("IP")["value"].sum()
    return float(per_ip_sum.mean()) if not per_ip_sum.empty else None


#endregion


#region [ 6. 공통 집계 유틸: 데모  ]
# =====================================================

# ===== 6.1. 데모 문자열 파싱 유틸 =====
def _gender_from_demo(s: str):
    """'데모' 문자열에서 성별(남/여/기타)을 추출합니다. (페이지 1, 2, 4용)"""
    s = str(s)
    if any(k in s for k in ["여", "F", "female", "Female"]): return "여"
    if any(k in s for k in ["남", "M", "male", "Male"]): return "남"
    return "기타"

def gender_from_demo(s: str):
    """ '데모' 문자열에서 성별 (남/여) 추출, 그 외 None (페이지 3용) """
    s = str(s)
    if any(k in s for k in ["여", "F", "female", "Female"]): return "여"
    if any(k in s for k in ["남", "M", "male", "Male"]):     return "남"
    return None

def _to_decade_label(x: str):
    """'데모' 문자열에서 연령대(10대, 20대...)를 추출합니다. (페이지 1, 2, 4용)"""
    m = re.search(r"\d+", str(x))
    if not m: return "기타"
    n = int(m.group(0))
    return f"{(n//10)*10}대"

def _decade_label_clamped(x: str):
    """ 10대~60대 범위로 연령대 라벨 생성, 그 외는 None (페이지 2, 3용) """
    m = re.search(r"\d+", str(x))
    if not m: return None
    n = int(m.group(0))
    n = max(10, min(60, (n // 10) * 10))
    return f"{n}대"

def _decade_key(s: str):
    """연령대 정렬을 위한 숫자 키를 추출합니다. (페이지 1, 2, 4용)"""
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else 999

def _fmt_ep(n):
    """ 회차 번호를 '01화' 형태로 포맷팅 (페이지 2, 3용) """
    try:
        return f"{int(n):02d}화"
    except Exception:
        return str(n)

# ===== 6.2. 피라미드 차트 렌더링 (페이지 1, 2) =====
COLOR_MALE = "#2a61cc"
COLOR_FEMALE = "#d93636"

def render_gender_pyramid(container, title: str, df_src: pd.DataFrame, height: int = 260):

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
        orientation="h",
        marker_color=COLOR_MALE,
        text=male_text,
        textposition="inside",
        insidetextanchor="end",
        textfont=dict(color="#ffffff", size=12),
        hovertemplate="연령대=%{y}<br>남성=%{customdata[0]:,.0f}명<br>성별내 비중=%{customdata[1]:.1f}%<extra></extra>",
        customdata=np.column_stack([male.abs(), male_share])
    ))
    fig.add_trace(go.Bar(
        y=pvt.index, x=female, name="여",
        orientation="h",
        marker_color=COLOR_FEMALE,
        text=female_text,
        textposition="inside",
        insidetextanchor="start",
        textfont=dict(color="#ffffff", size=12),
        hovertemplate="연령대=%{y}<br>여성=%{customdata[0]:,.0f}명<br>성별내 비중=%{customdata[1]:.1f}%<extra></extra>",
        customdata=np.column_stack([female, female_share])
    ))

    fig.update_layout(
        barmode="overlay",
        height=height,
        margin=dict(l=8, r=8, t=48, b=8),
        legend_title=None,
        bargap=0.15,
        bargroupgap=0.05,
    )
    # 피라미드 차트 전용 로컬 제목 (전역 테마 오버라이드)
    fig.update_layout(
        title=dict(
            text=title,
            x=0.0, xanchor="left",
            y=0.98, yanchor="top",
            font=dict(size=14)
        )
    )
    fig.update_yaxes(
        categoryorder="array",
        categoryarray=order,
        title=None,
        tickfont=dict(size=12),
        fixedrange=True
    )
    fig.update_xaxes(
        range=[-max_abs*1.05, max_abs*1.05],
        title=None,
        showticklabels=False,
        showgrid=False,
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="#888",
        fixedrange=True
    )

    container.plotly_chart(fig, use_container_width=True,
                           config={"scrollZoom": False, "staticPlot": False, "displayModeBar": False})

# ===== 6.3. 그룹 데모 평균 계산 (페이지 3, 4 통합용) =====
def get_avg_demo_pop_by_episode(df_src: pd.DataFrame, medias: List[str], max_ep: float = None) -> pd.DataFrame:
    """
    여러 IP가 포함된 df_src에서, 회차별/데모별 *평균* 시청자수(시청인구)를 계산합니다.
    [수정] max_ep 파라미터 추가: 지정된 회차까지만 필터링하여 계산
    """
    # 1. 매체 및 지표 필터링
    sub = df_src[
        (df_src["metric"] == "시청인구") &
        (df_src["데모"].notna()) &
        (df_src["매체"].isin(medias))
    ].copy()

    if sub.empty:
        return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)
    
    # 2. 회차 Numeric 컬럼 확보 및 필터링
    if "회차_numeric" not in sub.columns:
        sub["회차_numeric"] = sub["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    
    sub = sub.dropna(subset=["회차_numeric"])
    
    # [핵심] max_ep가 있으면 그 이하 회차만 남김
    if max_ep is not None:
        sub = sub[sub["회차_numeric"] <= max_ep]

    if sub.empty:
        return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)

    sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
    sub = sub.dropna(subset=["value"])

    sub["성별"] = sub["데모"].apply(gender_from_demo)
    sub["연령대_대"] = sub["데모"].apply(_decade_label_clamped)
    sub = sub[sub["성별"].isin(["남", "여"]) & sub["연령대_대"].notna()].copy()
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

# ===== 6.4. [이동] 히트맵 렌더링 (구 Region 9에서 이동) =====
def render_heatmap(df_plot: pd.DataFrame, title: str):
    """
    데이터프레임을 받아 Plotly 히트맵을 렌더링합니다.
    """
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
         v_min = np.nanmin(valid_values)
         v_max = np.nanmax(valid_values)
         if pd.isna(v_min): v_min = 0.0
         if pd.isna(v_max): v_max = 0.0
    
    abs_max = max(abs(v_min), abs(v_max), 10.0)
    
    fig = px.imshow(
        df_heatmap,
        text_auto=False, 
        aspect="auto",
        color_continuous_scale='RdBu_r', 
        range_color=[-abs_max, abs_max], 
        color_continuous_midpoint=0
    )

    text_template_df = df_heatmap.applymap(
        lambda x: "INF" if x == 999 else (f"{x:+.0f}%" if pd.notna(x) else "")
    )

    fig.update_traces(
        text=text_template_df.values,
        texttemplate="%{text}",
        hovertemplate="회차: %{y}<br>데모: %{x}<br>증감: %{text}<extra></extra>",
        textfont=dict(size=10, color="black")
    )

    fig.update_layout(
        height=max(520, len(df_heatmap.index) * 46), 
        xaxis_title=None,
        yaxis_title=None,
        xaxis=dict(side="top"),
    )
    
    c_heatmap, = st.columns(1)
    with c_heatmap:
        st.plotly_chart(fig, use_container_width=True)
#endregion


#region [ 7. 페이지 1: Overview ]
# =====================================================
def render_overview():
    df = load_data() 
  
    # --- 페이지 전용 필터 ---   
    filter_cols = st.columns(4)
    
    with filter_cols[0]:
        st.markdown("### 📊 Overview")
    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("<div class='gd-guideline'>", unsafe_allow_html=True)
        st.markdown(textwrap.dedent("""
            **지표 기준**
        - **시청률** `회차평균`: 전국 기준 가구 & 타깃(2049) 시청률
        - **티빙 LIVE UV** `회차평균`: 실시간 시청 UV
        - **티빙 당일 VOD UV** `회차평균`: 본방송 당일 VOD UV
        - **티빙 주간 VOD UV** `회차평균`: [회차 방영일부터 +6일까지의 7일간 VOD UV] - [티빙 당일 VOD]
        - **디지털 조회** `회차총합`: 방영주간 월~일 발생 총합 / 유튜브,인스타그램,틱톡,네이버TV,페이스북
        - **디지털 언급량** `회차총합`: 방영주차(월~일) 내 총합 / 커뮤니티,트위터,블로그                            
        - **화제성 점수** `회차평균`: 방영기간 주차별 화제성 점수의 평균 (펀덱스)
        - **앵커드라마 기준**: 토일 3%↑, 월화 2%↑
        """).strip())
        st.markdown("</div>", unsafe_allow_html=True)


    with filter_cols[1]:
        prog_sel = st.multiselect(
            "편성", 
            sorted(df["편성"].dropna().unique().tolist()),
            placeholder="편성 선택",
            label_visibility="collapsed"
        )

    # [수정] 연도 필터: '편성연도' 컬럼 사용 (날짜 파싱 X)
    all_years = []
    if "편성연도" in df.columns:
        unique_vals = df["편성연도"].dropna().unique()
        try:
            all_years = sorted(unique_vals, reverse=True)
        except:
            all_years = sorted([str(x) for x in unique_vals], reverse=True)

    # [수정] 월 필터: 기존 날짜 컬럼 사용 (연도와 분리)
    if "방영시작일" in df.columns and df["방영시작일"].notna().any():
        date_col_for_month = "방영시작일"
    else:
        date_col_for_month = "주차시작일"
    
    all_months = []
    if date_col_for_month in df.columns:
        date_series = df[date_col_for_month].dropna()
        if not date_series.empty:
            all_months = sorted(date_series.dt.month.unique().tolist())

    with filter_cols[2]:
        year_sel = st.multiselect(
            "연도", 
            all_years, 
            placeholder="연도 선택",
            label_visibility="collapsed"
        )
    with filter_cols[3]:
        month_sel = st.multiselect(
            "월", 
            all_months, 
            placeholder="월 선택",
            label_visibility="collapsed"
        )

    # --- 필터 적용 ---
    f = df.copy()
    if prog_sel:
        f = f[f["편성"].isin(prog_sel)]
    
    # [수정] 연도 필터 적용 (값 직접 비교)
    if year_sel and "편성연도" in f.columns:
        f = f[f["편성연도"].isin(year_sel)]
        
    # [수정] 월 필터 적용 (기존 날짜 컬럼 활용)
    if month_sel and date_col_for_month in f.columns:
        f = f[f[date_col_for_month].dt.month.isin(month_sel)]

    # --- 요약카드 계산 서브함수 (KPI 공통 유틸 사용) ---
    def avg_of_ip_means(metric_name: str):
        return mean_of_ip_episode_mean(f, metric_name) # [5. 공통 함수]

    def avg_of_ip_tving_epSum_mean(media_name: str):
        return mean_of_ip_episode_sum(f, "시청인구", [media_name]) # [5. 공통 함수]

    def avg_of_ip_tving_quick():
        return mean_of_ip_episode_sum(f, "시청인구", ["TVING QUICK"])

    def avg_of_ip_tving_vod_weekly():
        return mean_of_ip_episode_sum(f, "시청인구", ["TVING VOD"])

    def avg_of_ip_sums(metric_name: str):
        return mean_of_ip_sums(f, metric_name) # [5. 공통 함수]

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

    # --- 요약 카드 ---
    st.caption('▶ IP별 평균')

    c1, c2, c3, c4, c5 = st.columns(5)
    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)
    c6, c7, c8, c9, c10 = st.columns(5)

    t_rating   = avg_of_ip_means("T시청률")
    h_rating   = avg_of_ip_means("H시청률")
    tving_live = avg_of_ip_tving_epSum_mean("TVING LIVE")
    tving_quick= avg_of_ip_tving_quick()        
    tving_vod  = avg_of_ip_tving_vod_weekly()   

    digital_view = avg_of_ip_sums("조회수")
    digital_buzz = avg_of_ip_sums("언급량")
    f_score      = avg_of_ip_means("F_Score")
    fundex_top1 = count_ip_with_min1("F_Total")
    anchor_total = count_anchor_dramas()

    kpi(c1, "🎯 타깃 시청률", fmt(t_rating, digits=3))
    kpi(c2, "🏠 가구 시청률", fmt(h_rating, digits=3))
    kpi(c3, "📺 티빙 LIVE UV", fmt(tving_live, intlike=True))
    kpi(c4, "⚡ 티빙 당일 VOD UV", fmt(tving_quick, intlike=True)) 
    kpi(c5, "▶️ 티빙 주간 VOD UV", fmt(tving_vod, intlike=True))   
    
    kpi(c6, "👀 디지털 조회수", fmt(digital_view, intlike=True))
    kpi(c7, "💬 디지털 언급량", fmt(digital_buzz, intlike=True))
    kpi(c8, "🔥 화제성 점수",  fmt(f_score, intlike=True))
    kpi(c9, "🥇 펀덱스 1위", f"{fundex_top1}작품")
    kpi(c10, "⚓ 앵커드라마", f"{anchor_total}작품")

    st.divider()

    # --- 주차별 시청자수 트렌드 (Stacked Bar) ---
    df_trend = f[f["metric"]=="시청인구"].copy()
    if not df_trend.empty:
        tv_weekly = df_trend[df_trend["매체"]=="TV"].groupby("주차시작일")["value"].sum()
        
        tving_live_weekly = df_trend[df_trend["매체"]=="TVING LIVE"].groupby("주차시작일")["value"].sum()
        tving_quick_weekly = df_trend[df_trend["매체"]=="TVING QUICK"].groupby("주차시작일")["value"].sum() 
        tving_vod_weekly = df_trend[df_trend["매체"]=="TVING VOD"].groupby("주차시작일")["value"].sum()     

        all_dates = sorted(list(
            set(tv_weekly.index) | set(tving_live_weekly.index) | 
            set(tving_quick_weekly.index) | set(tving_vod_weekly.index)
        ))
        
        if all_dates:
            df_bar = pd.DataFrame({"주차시작일": all_dates})
            df_bar["TV 본방"] = df_bar["주차시작일"].map(tv_weekly).fillna(0)
            df_bar["티빙 본방"] = df_bar["주차시작일"].map(tving_live_weekly).fillna(0)
            df_bar["티빙 당일"] = df_bar["주차시작일"].map(tving_quick_weekly).fillna(0) 
            df_bar["티빙 주간"] = df_bar["주차시작일"].map(tving_vod_weekly).fillna(0)   

            df_long = df_bar.melt(id_vars="주차시작일",
                                  value_vars=["TV 본방","티빙 본방","티빙 당일","티빙 주간"],
                                  var_name="구분", value_name="시청자수")

            def fmt_kor_hover(x):
                if pd.isna(x) or x == 0: return "0"
                val = int(round(x / 10000))
                uk = val // 10000
                man = val % 10000
                if uk > 0: return f"{uk}억{man:04d}만"
                else: return f"{man}만"

            df_long["hover_txt"] = df_long["시청자수"].apply(fmt_kor_hover)

            fig = px.bar(
                df_long, x="주차시작일", y="시청자수", color="구분",
                title="📊 주차별 시청자수",
                color_discrete_map={
                    "TV 본방": "#2c3e50",     
                    "티빙 본방": "#d32f2f",   
                    "티빙 당일": "#ff5252",   
                    "티빙 주간": "#ffcdd2"    
                },
                custom_data=["hover_txt"]
            )
            
            fig.update_layout(
                xaxis_title=None, yaxis_title=None,
                barmode="stack", legend_title="구분",
                title_font=dict(size=20),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                margin=dict(t=60) 
            )
            
            fig.update_traces(
                textposition='none', 
                hovertemplate="<b>%{x}</b><br>%{data.name}: %{customdata[0]}<extra></extra>"
            )
            
            c_trend, = st.columns(1)
            with c_trend:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("주차별 시청자수 트렌드 데이터가 없습니다.")
    else:
        st.info("주차별 시청자수 트렌드 데이터가 없습니다.")


    st.divider()

    # --- 주요작품 테이블 (AgGrid) ---
    st.markdown("#### 🎬 전체 작품 RAW")

    def calculate_overview_performance(df):
        all_ips = df["IP"].unique()
        if len(all_ips) == 0: return pd.DataFrame()

        ep_col = _episode_col(df) # [5. 공통 함수]
        
        def _get_mean_of_ep_sums(df, metric_name, media_list=None):
            sub = df[df["metric"] == metric_name]
            if media_list: sub = sub[sub["매체"].isin(media_list)]
            if sub.empty or ep_col not in sub.columns: 
                return pd.Series(dtype=float).reindex(all_ips).fillna(0)
            sub = sub.dropna(subset=[ep_col]).copy()
            sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
            sub = sub.dropna(subset=["value"])
            if sub.empty: return pd.Series(dtype=float).reindex(all_ips).fillna(0)
            ep_sum = sub.groupby(["IP", ep_col], as_index=False)["value"].sum()
            per_ip_mean = ep_sum.groupby("IP")["value"].mean()
            return per_ip_mean.reindex(all_ips).fillna(0) 

        def _get_mean_of_ep_means(df, metric_name):
            sub = df[df["metric"] == metric_name]
            if sub.empty or ep_col not in sub.columns:
                return pd.Series(dtype=float).reindex(all_ips).fillna(0)
            sub = sub.dropna(subset=[ep_col]).copy()
            sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
            sub = sub.dropna(subset=["value"])
            if sub.empty: return pd.Series(dtype=float).reindex(all_ips).fillna(0)
            ep_mean = sub.groupby(["IP", ep_col], as_index=False)["value"].mean()
            per_ip_mean = ep_mean.groupby("IP")["value"].mean()
            return per_ip_mean.reindex(all_ips).fillna(0)
        
        aggs = {}
        aggs["타깃시청률"] = _get_mean_of_ep_means(df, "T시청률")
        aggs["가구시청률"] = _get_mean_of_ep_means(df, "H시청률")
        aggs["티빙LIVE"] = _get_mean_of_ep_sums(df, "시청인구", ["TVING LIVE"])
        aggs["티빙당일"] = _get_mean_of_ep_sums(df, "시청인구", ["TVING QUICK"])
        aggs["티빙주간"] = _get_mean_of_ep_sums(df, "시청인구", ["TVING VOD"]) 
        aggs["디지털언급량"] = df[df["metric"] == "언급량"].groupby("IP")["value"].sum().reindex(all_ips).fillna(0)
        aggs["디지털조회수"] = _get_view_data(df).groupby("IP")["value"].sum().reindex(all_ips).fillna(0)
        aggs["화제성순위"] = df[df["metric"] == "F_Total"].groupby("IP")["value"].min().reindex(all_ips).fillna(0)
        aggs["화제성점수"] = _get_mean_of_ep_sums(df, "F_Score", media_list=None)

        df_perf = pd.DataFrame(aggs).fillna(0).reset_index().rename(columns={"index": "IP"})
        return df_perf.sort_values("타깃시청률", ascending=False)

    df_perf = calculate_overview_performance(f)

    fmt_fixed3 = JsCode("""
    function(params){
      if (params.value == null || isNaN(params.value)) return '';
      return Number(params.value).toFixed(3);
    }""")
    fmt_thousands = JsCode("""
    function(params){
      if (params.value == null || isNaN(params.value)) return '';
      return Math.round(params.value).toLocaleString();
    }""")
    fmt_rank = JsCode("""
    function(params){
      if (params.value == null || isNaN(params.value)) return '';
      if (params.value == 0) return '–';
      return Math.round(params.value) + '위';
    }""")

    gb = GridOptionsBuilder.from_dataframe(df_perf)
    gb.configure_default_column(
        sortable=True, resizable=True, filter=False,
        cellStyle={'textAlign': 'center'},
        headerClass='centered-header'
    )
    gb.configure_grid_options(rowHeight=34, suppressMenuHide=True, domLayout='normal')
    
    gb.configure_column('IP', header_name='IP', cellStyle={'textAlign':'left'}) 
    gb.configure_column('타깃시청률', valueFormatter=fmt_fixed3, sort='desc')
    gb.configure_column('가구시청률', valueFormatter=fmt_fixed3)
    gb.configure_column('티빙LIVE', valueFormatter=fmt_thousands)
    gb.configure_column('티빙당일', header_name="티빙 당일 VOD", valueFormatter=fmt_thousands)
    gb.configure_column('티빙주간', header_name="티빙 주간 VOD", valueFormatter=fmt_thousands)
    gb.configure_column('디지털조회수', valueFormatter=fmt_thousands)
    gb.configure_column('디지털언급량', valueFormatter=fmt_thousands)
    gb.configure_column('화제성순위', valueFormatter=fmt_rank)
    gb.configure_column('화제성점수', valueFormatter=fmt_thousands)

    grid_options = gb.build()

    AgGrid(
        df_perf,
        gridOptions=grid_options,
        theme="streamlit",
        height=300,
        fit_columns_on_grid_load=True, 
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True
    )
#endregion


#region [ 8. 페이지 2: IP 성과 자세히보기 ]
# =====================================================
def render_ip_detail():
    
    df_full = load_data() # [3. 공통 함수]

    # [수정] 컬럼 비율 조정
    # 순서: 타이틀(3) | IP선택(2) | 방영연도(2) | 편성기준(2)
    filter_cols = st.columns([3, 2, 2, 2])

    with filter_cols[0]:
        st.markdown("<div class='page-title'>📈 IP 성과 자세히보기</div>", unsafe_allow_html=True)
    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("<div class='gd-guideline'>", unsafe_allow_html=True)
        st.markdown(textwrap.dedent("""
            **지표 기준**
        - **시청률** `누적 회차평균`: 전국 기준 가구 & 타깃(2049) 시청률
        - **티빙 LIVE UV** `누적 회차평균`: 실시간 시청 UV
        - **티빙 당일 VOD UV** `누적 회차평균`: 본방송 당일 VOD UV
        - **티빙 주간 VOD UV** `누적 회차평균`: [회차 방영일부터 +6일까지의 7일간 VOD UV] - [티빙 당일 VOD]
        - **디지털 조회** `누적 회차총합`: 방영주간 월~일 발생 총합 / 유튜브,인스타그램,틱톡,네이버TV,페이스북
        - **디지털 언급량** `누적 회차총합`: 방영주차(월~일) 내 총합 / 커뮤니티,트위터,블로그                            
        - **화제성 점수** `누적 회차평균`: 방영기간 주차별 화제성 점수의 평균 (펀덱스)
        """).strip())
        st.markdown("</div>", unsafe_allow_html=True)

    ip_options = sorted(df_full["IP"].dropna().unique().tolist())
    
    # [Col 1] IP 선택
    with filter_cols[1]:
        ip_selected = st.selectbox(
            "IP (단일선택)",
            ip_options,
            index=0 if ip_options else None,
            placeholder="IP 선택",
            label_visibility="collapsed"
        )

    # --- 데이터 전처리 (Default 설정을 위해 위치 이동) ---
    # [수정] 방영 연도 필터 기준을 '편성연도' 컬럼으로 변경 (날짜 파싱 X)
    date_col_for_filter = "편성연도"

    target_ip_rows = df_full[df_full["IP"] == ip_selected]
    
    # Default 연도/편성 추출
    default_year_list = []
    sel_prog = None
    
    if not target_ip_rows.empty:
        try:
            # [수정] 날짜 파싱(.dt.year) 없이 값 그대로 모드 추출
            if date_col_for_filter in target_ip_rows.columns:
                y_mode = target_ip_rows[date_col_for_filter].dropna().mode()
                if not y_mode.empty:
                    default_year_list = [y_mode.iloc[0]]
            
            sel_prog = target_ip_rows["편성"].dropna().mode().iloc[0]
        except Exception:
            pass
            
    all_years = []
    # [수정] '편성연도' 컬럼의 고유값을 그대로 사용
    if date_col_for_filter in df_full.columns:
        unique_vals = df_full[date_col_for_filter].dropna().unique()
        try:
            all_years = sorted(unique_vals, reverse=True)
        except:
            # 정렬 실패(타입 혼재 등) 시 문자열 변환 후 정렬
            all_years = sorted([str(x) for x in unique_vals], reverse=True)

    # [Col 2] 방영 연도
    with filter_cols[2]:
        selected_years = st.multiselect(
            "방영 연도",
            all_years,
            default=default_year_list,
            placeholder="방영 연도 선택",
            label_visibility="collapsed"
        )

    # [Col 3] 동일 편성 여부 (셀렉트박스)
    with filter_cols[3]:
        comp_type = st.selectbox(
            "편성 기준",
            ["동일 편성", "전체"], 
            index=0,
            label_visibility="collapsed"
        )
        use_same_prog = (comp_type == "동일 편성")

    # --- 선택 IP 데이터 필터링 ---
    f = target_ip_rows.copy()

    if "회차_numeric" in f.columns:
        f["회차_num"] = pd.to_numeric(f["회차_numeric"], errors="coerce")
    else:
        f["회차_num"] = pd.to_numeric(f["회차"].str.extract(r"(\d+)", expand=False), errors="coerce")
    
    my_max_ep = f["회차_num"].max()

    def _week_to_num(x: str):
        m = re.search(r"-?\d+", str(x))
        return int(m.group(0)) if m else None

    has_week_col = "주차" in f.columns
    if has_week_col:
        f["주차_num"] = f["주차"].apply(_week_to_num)

    # --- 베이스(비교 그룹) 데이터 필터링 ---
    base_raw = df_full.copy()
    group_name_parts = []

    # 1. 동일 편성 필터
    if use_same_prog:
        if sel_prog:
            base_raw = base_raw[base_raw["편성"] == sel_prog]
            group_name_parts.append(f"'{sel_prog}'")
        else:
            st.warning(f"'{ip_selected}'의 편성 정보가 없어 '동일 편성' 기준은 제외됩니다.", icon="⚠️")

    # 2. 방영 연도 필터
    if selected_years:
        # [수정] 날짜 파싱(.dt.year) 없이 컬럼 값 그대로 비교
        base_raw = base_raw[base_raw[date_col_for_filter].isin(selected_years)]
        
        if len(selected_years) <= 3:
            years_str = ",".join(map(str, sorted(selected_years)))
            # [수정] 데이터 값 자체("24년")를 사용하므로 "년" 접미사 제거
            group_name_parts.append(f"{years_str}")
        else:
            try:
                group_name_parts.append(f"{min(selected_years)}~{max(selected_years)}")
            except:
                group_name_parts.append("선택연도")
    else:
        st.warning("선택된 연도가 없습니다. (전체 연도 데이터와 비교)", icon="⚠️")

    if not group_name_parts:
        group_name_parts.append("전체")
    
    prog_label = " & ".join(group_name_parts) + " 평균"

    # --- (이하 로직 동일) ---
    if "회차_numeric" in base_raw.columns:
        base_raw["회차_num"] = pd.to_numeric(base_raw["회차_numeric"], errors="coerce")
    else:
        base_raw["회차_num"] = pd.to_numeric(base_raw["회차"].str.extract(r"(\d+)", expand=False), errors="coerce")
    
    if pd.notna(my_max_ep):
        base = base_raw[base_raw["회차_num"] <= my_max_ep].copy()
    else:
        base = base_raw.copy()

    st.markdown(
        f"<div class='sub-title'>📺 {ip_selected} 성과 상세 리포트</div>",
        unsafe_allow_html=True
    )
    st.markdown("---")

    # --- Metric Normalizer & Formatters ---
    def _normalize_metric(s: str) -> str:
        if s is None: return ""
        s2 = re.sub(r"[^A-Za-z0-9가-힣]+", "", str(s)).lower()
        return s2

    def _metric_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
        target = _normalize_metric(name)
        if "metric_norm" not in df.columns:
            df = df.copy()
            df["metric_norm"] = df["metric"].apply(_normalize_metric)
        return df[df["metric_norm"] == target]

    def fmt_kor(x):
        if pd.isna(x): return "0"
        val = float(x)
        if val == 0: return "0"
        rounded = int(round(val / 10000)) 
        if rounded == 0 and val > 0: return f"{val/10000:.1f}만"
        uk = rounded // 10000; man = rounded % 10000
        if uk > 0: return f"{uk}억{man:04d}만" if man > 0 else f"{uk}억"
        return f"{man}만"

    def fmt_live_kor(x):
        if pd.isna(x): return "0"
        val = float(x)
        if val == 0: return "0"
        man = int(val // 10000); cheon = int((val % 10000) // 1000)
        if man > 0: return f"{man}만{cheon}천" if cheon > 0 else f"{man}만"
        return f"{cheon}천" if cheon > 0 else f"{int(val)}"

    def get_axis_ticks(max_val, formatter=fmt_kor):
        if pd.isna(max_val) or max_val <= 0: return None, None
        step = max_val / 4
        power = 10 ** int(np.log10(step))
        base = step / power
        if base < 1.5: step = 1 * power
        elif base < 3.5: step = 2 * power
        elif base < 7.5: step = 5 * power
        else: step = 10 * power
        vals = np.arange(0, max_val + step * 0.1, step)
        texts = [formatter(v) for v in vals]
        return vals, texts
    
    # --- Aggregation Helpers ---
    def _series_ip_metric(base_df: pd.DataFrame, metric_name: str, mode: str = "mean", media: List[str] | None = None):
        if metric_name == "조회수": sub = _get_view_data(base_df)
        else: sub = _metric_filter(base_df, metric_name).copy()
        if media is not None: sub = sub[sub["매체"].isin(media)]
        if sub.empty: return pd.Series(dtype=float)
        ep_col = _episode_col(sub)
        sub = sub.dropna(subset=[ep_col])
        sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
        sub = sub.dropna(subset=["value"])
        if sub.empty: return pd.Series(dtype=float)
        
        if mode == "mean":
            ep_mean = sub.groupby(["IP", ep_col], as_index=False)["value"].mean()
            s = ep_mean.groupby("IP")["value"].mean()
        elif mode == "sum": s = sub.groupby("IP")["value"].sum()
        elif mode == "ep_sum_mean":
            ep_sum = sub.groupby(["IP", ep_col], as_index=False)["value"].sum()
            s = ep_sum.groupby("IP")["value"].mean()
        elif mode == "min": s = sub.groupby("IP")["value"].min()
        else: s = sub.groupby("IP")["value"].mean()
        return pd.to_numeric(s, errors="coerce").dropna()

    def _min_of_ip_metric(df_src: pd.DataFrame, metric_name: str) -> float | None:
        sub = _metric_filter(df_src, metric_name).copy()
        if sub.empty: return None
        s = pd.to_numeric(sub["value"], errors="coerce").dropna()
        return float(s.min()) if not s.empty else None

    def _mean_like_rating(df_src: pd.DataFrame, metric_name: str) -> float | None:
        sub = _metric_filter(df_src, metric_name).copy()
        if sub.empty: return None
        sub["val"] = pd.to_numeric(sub["value"], errors="coerce")
        sub = sub.dropna(subset=["val"])
        if sub.empty: return None
        if "회차_num" in sub.columns and sub["회차_num"].notna().any():
            g = sub.dropna(subset=["회차_num"]).groupby("회차_num", as_index=False)["val"].mean()
            return float(g["val"].mean())
        if date_col_for_filter in sub.columns and sub[date_col_for_filter].notna().any():
            g = sub.dropna(subset=[date_col_for_filter]).groupby(date_col_for_filter, as_index=False)["val"].mean()
            return float(g["val"].mean())
        return float(sub["val"].mean())

    # --- KPI Calculation ---
    val_T = mean_of_ip_episode_mean(f, "T시청률")
    val_H = mean_of_ip_episode_mean(f, "H시청률")
    val_live = mean_of_ip_episode_sum(f, "시청인구", ["TVING LIVE"])
    val_quick = mean_of_ip_episode_sum(f, "시청인구", ["TVING QUICK"]) 
    val_vod = mean_of_ip_episode_sum(f, "시청인구", ["TVING VOD"])
    val_buzz = mean_of_ip_sums(f, "언급량")
    val_view = mean_of_ip_sums(f, "조회수")
    val_topic_min = _min_of_ip_metric(f, "F_Total")
    val_topic_avg = _mean_like_rating(f, "F_score")

    base_T = mean_of_ip_episode_mean(base, "T시청률")
    base_H = mean_of_ip_episode_mean(base, "H시청률")
    base_live = mean_of_ip_episode_sum(base, "시청인구", ["TVING LIVE"])
    base_quick = mean_of_ip_episode_sum(base, "시청인구", ["TVING QUICK"])
    base_vod = mean_of_ip_episode_sum(base, "시청인구", ["TVING VOD"])
    base_buzz = mean_of_ip_sums(base, "언급량")
    base_view = mean_of_ip_sums(base, "조회수")
    base_topic_min_series = _series_ip_metric(base, "F_Total", mode="min")
    base_topic_min = float(base_topic_min_series.mean()) if not base_topic_min_series.empty else None
    base_topic_avg = _mean_like_rating(base, "F_score")

    # --- Ranking ---
    def _rank_within_program(base_df, metric_name, ip_name, value, mode="mean", media=None, low_is_good=False):
        s = _series_ip_metric(base_df, metric_name, mode=mode, media=media)
        if s.empty or value is None or pd.isna(value): return (None, 0)
        s = s.dropna()
        if ip_name not in s.index: return (None, int(s.shape[0]))
        ranks = s.rank(method="min", ascending=low_is_good)
        return (int(ranks.loc[ip_name]), int(s.shape[0]))

    rk_T     = _rank_within_program(base, "T시청률", ip_selected, val_T,   mode="mean",        media=None)
    rk_H     = _rank_within_program(base, "H시청률", ip_selected, val_H,   mode="mean",        media=None)
    rk_live  = _rank_within_program(base, "시청인구", ip_selected, val_live,  mode="ep_sum_mean", media=["TVING LIVE"])
    rk_quick = _rank_within_program(base, "시청인구", ip_selected, val_quick, mode="ep_sum_mean", media=["TVING QUICK"])
    rk_vod   = _rank_within_program(base, "시청인구", ip_selected, val_vod,   mode="ep_sum_mean", media=["TVING VOD"])
    rk_buzz  = _rank_within_program(base, "언급량",   ip_selected, val_buzz,  mode="sum",        media=None)
    rk_view  = _rank_within_program(base, "조회수",   ip_selected, val_view,  mode="sum",        media=None)
    rk_fmin  = _rank_within_program(base, "F_Total",  ip_selected, val_topic_min, mode="min",   media=None, low_is_good=True)
    rk_fscr  = _rank_within_program(base, "F_score",  ip_selected, val_topic_avg, mode="mean",  media=None, low_is_good=False)

    # --- KPI Render Helpers ---
    def _pct_color(val, base_val):
        if val is None or pd.isna(val) or base_val in (None, 0) or pd.isna(base_val): return "#888"
        pct = (val / base_val) * 100
        return "#d93636" if pct > 100 else ("#2a61cc" if pct < 100 else "#444")

    # [수정] 순위 표시에 '총 N개 중' 및 '1위 왕관' 추가
    def sublines_html(prog_label: str, rank_tuple: tuple, val, base_val):
        rnk, total = rank_tuple if rank_tuple else (None, 0)
        
        if rnk is not None and total > 0:
            prefix = "👑 " if rnk == 1 else ""
            rank_label = f"{prefix}{rnk}위<span style='font-size:11px;font-weight:400;color:#9ca3af;margin-left:2px'>(총{total}개)</span>"
        else:
            rank_label = "–위"

        pct_txt = "–"; col = "#888"
        try:
            if (val is not None) and (base_val not in (None, 0)) and (not (pd.isna(val) or pd.isna(base_val))):
                pct = (float(val) / float(base_val)) * 100.0
                pct_txt = f"{pct:.0f}%"; col = _pct_color(val, base_val)
        except Exception: pass
        return (
            "<div class='kpi-subwrap'>"
            "<span class='kpi-sublabel'>그룹 內</span> "
            f"<span class='kpi-substrong'>{rank_label}</span><br/>"
            "<span class='kpi-sublabel'>그룹 평균比</span> "
            f"<span class='kpi-subpct' style='color:{col};'>{pct_txt}</span>"
            "</div>"
        )

    def sublines_dummy():
        return (
         "<div class='kpi-subwrap' style='visibility:hidden;'>"
         "<span class='kpi-sublabel'>_</span> <span class='kpi-substrong'>_</span><br/>"
         "<span class='kpi-sublabel'>_</span> <span class='kpi-subpct'>_</span>"
          "</div>"
        )

    def kpi_with_rank(col, title, value, base_val, rank_tuple, prog_label, intlike=False, digits=3, value_suffix=""):
        with col:
            main_val = fmt(value, digits=digits, intlike=intlike)
            st.markdown(
                f"<div class='kpi-card'><div class='kpi-title'>{title}</div>"
                f"<div class='kpi-value'>{main_val}{value_suffix}</div>"
                f"{sublines_html(prog_label, rank_tuple, value, base_val)}</div>",
                unsafe_allow_html=True
            )

    # === KPI 배치 (Row 1) ===
    c1, c2, c3, c4, c5 = st.columns(5)
    kpi_with_rank(c1, "🎯 타깃시청률",    val_T, base_T, rk_T, prog_label, digits=3)
    kpi_with_rank(c2, "🏠 가구시청률",    val_H, base_H, rk_H, prog_label, digits=3)
    kpi_with_rank(c3, "📺 티빙 LIVE UV",     val_live, base_live, rk_live, prog_label, intlike=True)
    kpi_with_rank(c4, "⚡ 티빙 당일 VOD UV",  val_quick, base_quick, rk_quick, prog_label, intlike=True)
    kpi_with_rank(c5, "▶️ 티빙 주간 VOD UV", val_vod, base_vod, rk_vod, prog_label, intlike=True)

    # === KPI 배치 (Row 2) ===
    c6, c7, c8, c9, c10 = st.columns(5)
    kpi_with_rank(c6, "👀 디지털 조회수", val_view, base_view, rk_view, prog_label, intlike=True)
    kpi_with_rank(c7, "💬 디지털 언급량", val_buzz, base_buzz, rk_buzz, prog_label, intlike=True)
    with c8:
        v = val_topic_min
        main_val = "–" if (v is None or pd.isna(v)) else f"{int(round(v)):,d}위"
        st.markdown(
            f"<div class='kpi-card'><div class='kpi-title'>🏆 최고 화제성 순위</div>"
            f"<div class='kpi-value'>{main_val}</div>{sublines_dummy()}</div>",
            unsafe_allow_html=True
        )
    kpi_with_rank(c9, "🔥 화제성 점수", val_topic_avg, base_topic_avg, rk_fscr, prog_label, intlike=True)
    with c10:
        # 더미 카드 (레이아웃 맞춤용, 투명 처리)
        st.markdown(
            f"<div class='kpi-card' style='opacity:0; pointer-events:none;'><div class='kpi-title'>-</div>"
            f"<div class='kpi-value'>-</div>{sublines_dummy()}</div>",
            unsafe_allow_html=True
        )

    st.divider()

    # --- Charts ---
    chart_h = 320
    common_cfg = {"scrollZoom": False, "staticPlot": False, "displayModeBar": False}

    # === [Row1] 시청률 | 티빙 ===
    cA, cB = st.columns(2)
    with cA:
        st.markdown("<div class='sec-title'>📈 시청률</div>", unsafe_allow_html=True)
        rsub = f[f["metric"].isin(["T시청률", "H시청률"])].dropna(subset=["회차", "회차_num"]).copy()
        rsub = rsub.sort_values("회차_num")
        if not rsub.empty:
            ep_order = rsub[["회차", "회차_num"]].drop_duplicates().sort_values("회차_num")["회차"].tolist()
            t_series = rsub[rsub["metric"] == "T시청률"].groupby("회차", as_index=False)["value"].mean()
            h_series = rsub[rsub["metric"] == "H시청률"].groupby("회차", as_index=False)["value"].mean()
            ymax = pd.concat([t_series["value"], h_series["value"]]).max()
            y_upper = float(ymax) * 1.4 if pd.notna(ymax) else None

            fig_rate = go.Figure()
            fig_rate.add_trace(go.Scatter(
                x=h_series["회차"], y=h_series["value"], mode="lines+markers+text", name="가구시청률",
                line=dict(color='#90a4ae', width=2), text=[f"{v:.2f}" for v in h_series["value"]], textposition="top center"
            ))
            fig_rate.add_trace(go.Scatter(
                x=t_series["회차"], y=t_series["value"], mode="lines+markers+text", name="타깃시청률",
                line=dict(color='#3949ab', width=3), text=[f"{v:.2f}" for v in t_series["value"]], textposition="top center"
            ))
            fig_rate.update_xaxes(categoryorder="array", categoryarray=ep_order, title=None, fixedrange=True)
            fig_rate.update_yaxes(title=None, fixedrange=True, range=[0, y_upper] if (y_upper and y_upper > 0) else None)
            fig_rate.update_layout(legend_title=None, height=chart_h, margin=dict(l=8, r=8, t=10, b=8), legend=dict(orientation='h', yanchor='bottom', y=1.02))
            st.plotly_chart(fig_rate, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 시청률 데이터가 없습니다.")

    with cB:
        st.markdown("<div class='sec-title'>📱 TVING 시청자수</div>", unsafe_allow_html=True)
        t_keep = ["TVING LIVE", "TVING QUICK", "TVING VOD"]
        tsub = f[(f["metric"] == "시청인구") & (f["매체"].isin(t_keep))].dropna(subset=["회차", "회차_num"]).copy()
        tsub = tsub.sort_values("회차_num")
        
        if not tsub.empty:
            media_map = {"TVING LIVE": "LIVE", "TVING QUICK": "당일 VOD", "TVING VOD": "주간 VOD"}
            tsub["매체_표기"] = tsub["매체"].map(media_map)
            
            pvt = tsub.pivot_table(index="회차", columns="매체_표기", values="value", aggfunc="sum").fillna(0)
            ep_order = tsub[["회차", "회차_num"]].drop_duplicates().sort_values("회차_num")["회차"].tolist()
            pvt = pvt.reindex(ep_order)
            
            stack_order = ["LIVE", "당일 VOD", "주간 VOD"]
            colors = {"LIVE": "#90caf9", "당일 VOD": "#64b5f6", "주간 VOD": "#1565c0"}
            
            fig_tving = go.Figure()
            for m in stack_order:
                if m in pvt.columns:
                    # [수정] 계열별 레이블(text) 제거
                    fig_tving.add_trace(go.Bar(
                        name=m, x=pvt.index, y=pvt[m],
                        marker_color=colors[m],
                        text=None, # 레이블 제거
                        hovertemplate=f"<b>%{{x}}</b><br>{m}: %{{y:,.0f}}<extra></extra>"
                    ))
            
            total_vals = pvt[list(set(pvt.columns) & set(stack_order))].sum(axis=1)
            max_val = total_vals.max()
            total_txt = [fmt_live_kor(v) for v in total_vals]
            
            # 총합 레이블만 유지
            fig_tving.add_trace(go.Scatter(
                x=pvt.index, y=total_vals, mode='text',
                text=total_txt, textposition='top center',
                textfont=dict(size=11, color='#333'),
                showlegend=False, hoverinfo='skip'
            ))

            fig_tving.update_layout(
                barmode='stack', height=chart_h, margin=dict(l=8, r=8, t=10, b=8),
                legend=dict(orientation='h', yanchor='bottom', y=1.02),
                yaxis=dict(showgrid=False, visible=False, range=[0, max_val * 1.2]),
                xaxis=dict(categoryorder="array", categoryarray=ep_order, fixedrange=True)
            )
            st.plotly_chart(fig_tving, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 TVING 시청자 데이터가 없습니다.")

    # === [Row2] 데모 분포 ===
    cG, cH, cI = st.columns(3)

    def _render_pyramid_local(container, title, df_src, height=260):
        if df_src.empty:
            container.info("표시할 데이터가 없습니다."); return

        COLOR_MALE_NEW = "#5B85D9"; COLOR_FEMALE_NEW = "#E66C6C"

        df_demo = df_src.copy()
        df_demo["성별"] = df_demo["데모"].apply(_gender_from_demo)
        df_demo["연령대_대"] = df_demo["데모"].apply(_to_decade_label)
        df_demo = df_demo[df_demo["성별"].isin(["남","여"]) & df_demo["연령대_대"].notna()]

        if df_demo.empty: container.info("데이터 없음"); return

        order = ["60대", "50대", "40대", "30대", "20대", "10대"]

        pvt = df_demo.groupby(["연령대_대","성별"])["value"].sum().unstack("성별").reindex(order).fillna(0)
        male = -pvt.get("남", pd.Series(0, index=pvt.index))
        female = pvt.get("여", pd.Series(0, index=pvt.index))

        total_pop = male.abs().sum() + female.sum()
        if total_pop == 0: total_pop = 1
        
        male_share = (male.abs() / total_pop * 100)
        female_share = (female / total_pop * 100)
        max_abs = float(max(male.abs().max(), female.max()) or 1)

        male_text = [f"{v:.1f}%" if v > 0 else "" for v in male_share]
        female_text = [f"{v:.1f}%" if v > 0 else "" for v in female_share]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=pvt.index, x=male, name="남", orientation="h", marker_color=COLOR_MALE_NEW,
            text=male_text, textposition="inside", insidetextanchor="end",
            textfont=dict(color="#ffffff", size=11),
            hovertemplate="연령대=%{y}<br>남성=%{customdata[0]:,.0f}명<br>전체비중=%{customdata[1]:.1f}%<extra></extra>",
            customdata=np.column_stack([male.abs(), male_share])
        ))
        fig.add_trace(go.Bar(
            y=pvt.index, x=female, name="여", orientation="h", marker_color=COLOR_FEMALE_NEW,
            text=female_text, textposition="inside", insidetextanchor="start",
            textfont=dict(color="#ffffff", size=11),
            hovertemplate="연령대=%{y}<br>여성=%{customdata[0]:,.0f}명<br>전체비중=%{customdata[1]:.1f}%<extra></extra>",
            customdata=np.column_stack([female, female_share])
        ))

        fig.update_layout(
            barmode="overlay", height=height, margin=dict(l=8, r=8, t=48, b=8),
            legend_title=None, bargap=0.15,
            title=dict(text=title, x=0.0, y=0.98, font=dict(size=14))
        )
        fig.update_yaxes(categoryorder="array", categoryarray=order, fixedrange=True)
        fig.update_xaxes(range=[-max_abs*1.1, max_abs*1.1], showticklabels=False, showgrid=False, zeroline=True, fixedrange=True)
        container.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with cG:
        st.markdown("<div class='sec-title' style='font-size:18px;'>👥누적 시청자 분포 - TV</div>", unsafe_allow_html=True)
        tv_demo = f[(f["매체"] == "TV") & (f["metric"] == "시청인구") & f["데모"].notna()].copy()
        _render_pyramid_local(cG, "", tv_demo, height=260)

    with cH:
        st.markdown("<div class='sec-title' style='font-size:18px;'>👥누적 시청자 분포 - TVING LIVE</div>", unsafe_allow_html=True)
        live_demo = f[(f["매체"] == "TVING LIVE") & (f["metric"] == "시청인구") & f["데모"].notna()].copy()
        _render_pyramid_local(cH, "", live_demo, height=260)

    with cI:
        st.markdown("<div class='sec-title' style='font-size:18px;'>👥누적 시청자 분포 - TVING VOD</div>", unsafe_allow_html=True)
        vod_demo = f[(f["매체"].isin(["TVING VOD", "TVING QUICK"])) & (f["metric"] == "시청인구") & f["데모"].notna()].copy()
        _render_pyramid_local(cI, "", vod_demo, height=260)

    # === [Row3] 디지털&화제성 ===
    cC, cD, cE = st.columns(3)
    digital_colors = ['#5c6bc0', '#7e57c2', '#26a69a', '#66bb6a', '#ffa726', '#ef5350']
    
    with cC:
        st.markdown("<div class='sec-title'>💻 디지털 조회수</div>", unsafe_allow_html=True)
        dview = _get_view_data(f) 
        if not dview.empty:
            if has_week_col and dview["주차"].notna().any():
                order = (dview[["주차", "주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
                pvt = dview.pivot_table(index="주차", columns="매체", values="value", aggfunc="sum").fillna(0)
                pvt = pvt.reindex(order)
                x_vals = pvt.index.tolist(); use_category = True
            else:
                pvt = (dview.pivot_table(index="주차시작일", columns="매체", values="value", aggfunc="sum").sort_index().fillna(0))
                x_vals = pvt.index.tolist(); use_category = False

            total_view = pvt.sum(axis=1)
            max_view = total_view.max()
            view_ticks_val, view_ticks_txt = get_axis_ticks(max_view, formatter=fmt_kor)
            total_text = [fmt_kor(v) for v in total_view]

            fig_view = go.Figure()
            for i, col in enumerate(pvt.columns):
                h_texts = [fmt_kor(v) for v in pvt[col]]
                fig_view.add_trace(go.Bar(
                    name=col, x=x_vals, y=pvt[col], marker_color=digital_colors[i % len(digital_colors)],
                    hovertemplate="<b>%{x}</b><br>" + f"{col}: " + "%{text}<extra></extra>",
                    text=h_texts, textposition='none'
                ))
            
            fig_view.add_trace(go.Scatter(
                x=x_vals, y=total_view, mode='text', text=total_text, textposition='top center',
                textfont=dict(size=11, color='#333'), showlegend=False, hoverinfo='skip'
            ))
            fig_view.update_layout(
                barmode="stack", legend_title=None, height=chart_h, margin=dict(l=8, r=8, t=10, b=8),
                yaxis=dict(tickvals=view_ticks_val, ticktext=view_ticks_txt, fixedrange=True, range=[0, max_view * 1.15])
            )
            if use_category: fig_view.update_xaxes(categoryorder="array", categoryarray=x_vals, fixedrange=True)
            st.plotly_chart(fig_view, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 조회수 데이터가 없습니다.")

    with cD:
        st.markdown("<div class='sec-title'>💬 디지털 언급량</div>", unsafe_allow_html=True)
        dbuzz = f[f["metric"] == "언급량"].copy()
        if not dbuzz.empty:
            if has_week_col and dbuzz["주차"].notna().any():
                order = (dbuzz[["주차", "주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
                pvt = dbuzz.pivot_table(index="주차", columns="매체", values="value", aggfunc="sum").fillna(0)
                pvt = pvt.reindex(order)
                x_vals = pvt.index.tolist(); use_category = True
            else:
                pvt = (dbuzz.pivot_table(index="주차시작일", columns="매체", values="value", aggfunc="sum").sort_index().fillna(0))
                x_vals = pvt.index.tolist(); use_category = False

            total_buzz = pvt.sum(axis=1)
            max_buzz = total_buzz.max()
            total_text = [f"{v:,.0f}" for v in total_buzz]

            fig_buzz = go.Figure()
            for i, col in enumerate(pvt.columns):
                h_texts = [f"{v:,.0f}" for v in pvt[col]]
                fig_buzz.add_trace(go.Bar(
                    name=col, x=x_vals, y=pvt[col], marker_color=digital_colors[(i+2) % len(digital_colors)],
                    hovertemplate="<b>%{x}</b><br>" + f"{col}: " + "%{text}<extra></extra>",
                    text=h_texts, textposition='none'
                ))
            
            fig_buzz.add_trace(go.Scatter(
                x=x_vals, y=total_buzz, mode='text', text=total_text, textposition='top center',
                textfont=dict(size=11, color='#333'), showlegend=False, hoverinfo='skip'
            ))
            fig_buzz.update_layout(
                barmode="stack", legend_title=None, height=chart_h, margin=dict(l=8, r=8, t=10, b=8),
                yaxis=dict(fixedrange=True, range=[0, max_buzz * 1.15])
            )
            if use_category: fig_buzz.update_xaxes(categoryorder="array", categoryarray=x_vals, fixedrange=True)
            st.plotly_chart(fig_buzz, use_container_width=True, config=common_cfg)
        else:
            st.info("표시할 언급량 데이터가 없습니다.")

    with cE:
        st.markdown("<div class='sec-title'>🔥 화제성 점수 & 순위</div>", unsafe_allow_html=True)
        fdx = _metric_filter(f, "F_Total").copy(); fs = _metric_filter(f, "F_score").copy()
        if has_week_col and f["주차"].notna().any():
            order = (f[["주차", "주차_num"]].dropna().drop_duplicates().sort_values("주차_num")["주차"].tolist())
            key_col = "주차"; use_category = True
        else:
            key_col = "주차시작일"; order = sorted(f[key_col].dropna().unique()); use_category = False
            
        if not fs.empty:
            fs["val"] = pd.to_numeric(fs["value"], errors="coerce")
            fs_agg = fs.dropna(subset=[key_col]).groupby(key_col, as_index=False)["val"].mean()
        else: fs_agg = pd.DataFrame(columns=[key_col, "val"])
            
        if not fdx.empty:
            fdx["rank"] = pd.to_numeric(fdx["value"], errors="coerce")
            fdx_agg = fdx.dropna(subset=[key_col]).groupby(key_col, as_index=False)["rank"].min()
        else: fdx_agg = pd.DataFrame(columns=[key_col, "rank"])
            
        if not fs_agg.empty:
            merged = pd.merge(fs_agg, fdx_agg, on=key_col, how="left")
            if use_category: merged = merged.set_index(key_col).reindex(order).dropna(subset=["val"]).reset_index()
            else: merged = merged.sort_values(key_col)
            
            if not merged.empty:
                x_vals = merged[key_col].tolist(); y_vals = merged["val"].tolist()
                labels = [f"{int(r['rank'])}위<br>/{int(r['val']):,}점" if pd.notna(r['rank']) else f"{int(r['val']):,}점" for _, r in merged.iterrows()]
                
                fig_comb = go.Figure()
                fig_comb.add_trace(go.Scatter(
                    x=x_vals, y=y_vals, mode="lines+markers+text", name="화제성 점수",
                    text=labels, textposition="top center", textfont=dict(size=11, color="#333"),
                    line=dict(color='#ec407a', width=3), marker=dict(size=7, color='#ec407a')
                ))
                if y_vals: fig_comb.update_yaxes(range=[0, max(y_vals) * 1.25], title=None, fixedrange=True)
                if use_category: fig_comb.update_xaxes(categoryorder="array", categoryarray=x_vals, fixedrange=True)
                fig_comb.update_layout(legend_title=None, height=chart_h, margin=dict(l=8, r=8, t=20, b=8))
                st.plotly_chart(fig_comb, use_container_width=True, config=common_cfg)
            else: st.info("데이터 없음")
        else: st.info("데이터 없음")

    st.divider()

    # === [Row5] 데모분석 상세 표 (AgGrid) ===
    st.markdown("#### 👥 회차별 시청자수 분포")

    def _build_demo_table_numeric(df_src, medias):
        sub = df_src[
            (df_src["metric"] == "시청인구")
            & (df_src["데모"].notna())
            & (df_src["매체"].isin(medias))
        ].copy()

        if sub.empty:
            return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)

        # 데모 → 성별 / 연령대
        sub["성별"] = sub["데모"].apply(_gender_from_demo)
        sub["연령대_대"] = sub["데모"].apply(_decade_label_clamped)
        sub = sub[sub["성별"].isin(["남", "여"]) & sub["연령대_대"].notna()].copy()
        if sub.empty:
            return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)

        # 회차 숫자화
        if "회차_num" not in sub.columns:
            sub["회차_num"] = sub["회차"].str.extract(r"(\d+)", expand=False).astype(float)
        sub = sub.dropna(subset=["회차_num"])
        if sub.empty:
            return pd.DataFrame(columns=["회차"] + DEMO_COLS_ORDER)

        sub["회차_num"] = sub["회차_num"].astype(int)

        # 라벨: "20대남성", "30대여성"
        sub["라벨"] = sub.apply(
            lambda r: f"{r['연령대_대']}{'남성' if r['성별']=='남' else '여성'}",
            axis=1,
        )

        # 피벗: 회차 × 데모 매트릭스
        pvt = (
            sub.pivot_table(
                index="회차_num",
                columns="라벨",
                values="value",
                aggfunc="sum",
            )
            .fillna(0)
        )

        # 없는 데모 컬럼 0으로 채워서 순서 통일
        for c in DEMO_COLS_ORDER:
            if c not in pvt.columns:
                pvt[c] = 0

        pvt = pvt[DEMO_COLS_ORDER].sort_index()
        pvt.insert(0, "회차", pvt.index.map(_fmt_ep))

        return pvt.reset_index(drop=True)

    # === JS 렌더러 (▲/▾ + 행별 그라디언트) ===

    # DiffRenderer: 전 회차 대비 ▲/▾ 표시
    # [수정] DiffRenderer: 주석 스타일 변경(//) 및 특수문자 HTML Entity 사용
    diff_renderer = JsCode("""
    class DiffRenderer {
      init(params) {
        this.eGui = document.createElement('span');

        if (!params) {
          this.eGui.innerText = '';
          return;
        }

        const api = params.api;
        const colId = params.column ? params.column.getColId() : null;
        const rowIndex = params.node ? params.node.rowIndex : 0;
        const rawVal = (params.value === null || params.value === undefined) ? 0 : params.value;
        const val = Number(rawVal) || 0;

        // 1. 숫자 포맷팅
        let displayVal = (colId === "회차")
          ? (params.value || "")
          : Math.round(val).toLocaleString();

        // 2. 화살표 로직
        let arrow = "";
        if (colId !== "회차" && api && typeof api.getDisplayedRowAtIndex === "function" && rowIndex > 0) {
          const prev = api.getDisplayedRowAtIndex(rowIndex - 1);
          if (prev && prev.data && prev.data[colId] != null) {
            const pv = Number(prev.data[colId] || 0);

            if (val > pv) {
              // 상승: 작은 삼각형(Red) -> HTML Entity 사용
              arrow = '<span style="margin-left:4px;">(<span style="color:#d93636;">&#9652;</span>)</span>';
            } else if (val < pv) {
              // 하락: 작은 역삼각형(Blue) -> HTML Entity 사용
              arrow = '<span style="margin-left:4px;">(<span style="color:#2a61cc;">&#9662;</span>)</span>';
            }
          }
        }

        this.eGui.innerHTML = displayVal + arrow;
      }

      getGui() {
        return this.eGui;
      }
    }
    """)

    # 행 내에서 min~max 기준으로 블루 그라디언트
    _js_demo_cols = "[" + ",".join([f'"{c}"' for c in DEMO_COLS_ORDER]) + "]"
    cell_style_renderer = JsCode(f"""
    function(params){{
      const field = params.colDef.field;
      // 회차 열: 좌측 정렬, 흰 배경 고정
      if (field === "회차") {{
        return {{
          'text-align': 'left',
          'font-weight': '600',
          'background-color': '#ffffff'
        }};
      }}

      if (!params || !params.data) {{
        return {{
          'background-color': '#ffffff',
          'text-align': 'right',
          'padding': '2px 4px',
          'font-weight': '500'
        }};
      }}

      const COLS = {_js_demo_cols};
      let rowVals = [];
      for (let k of COLS) {{
        if (params.data.hasOwnProperty(k)) {{
          const v = Number(params.data[k]);
          if (!isNaN(v)) rowVals.push(v);
        }}
      }}

      let bg = '#ffffff';
      if (rowVals.length > 0) {{
        const v = Number(params.value || 0);
        const mn = Math.min.apply(null, rowVals);
        const mx = Math.max.apply(null, rowVals);
        let norm = 0.5;
        if (mx > mn) {{
          norm = (v - mn) / (mx - mn);
        }}
        norm = Math.max(0, Math.min(1, norm));
        const alpha = 0.12 + 0.45 * norm;
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

    def _render_aggrid_table(df_numeric, title):
        st.markdown(f"###### {title}")
        if df_numeric.empty:
            st.info("데이터 없음")
            return

        gb = GridOptionsBuilder.from_dataframe(df_numeric)

        gb.configure_grid_options(
            rowHeight=34,
            suppressMenuHide=True,
            # domLayout는 사용 안 함 (height를 직접 계산해서 지정)
        )

        gb.configure_default_column(
            sortable=False,
            resizable=True,
            filter=False,
            cellStyle={"textAlign": "right"},
            headerClass="centered-header bold-header",
        )

        gb.configure_column(
            "회차",
            header_name="회차",
            cellStyle={"textAlign": "left"},
        )

        # 나머지 컬럼: JS 렌더러 적용
        for c in [col for col in df_numeric.columns if col != "회차"]:
            gb.configure_column(
                c,
                header_name=c,
                cellRenderer=diff_renderer,
                cellStyle=cell_style_renderer,
            )

        # 🔹 행 수에 따라 height 동적 계산
        rows = len(df_numeric)
        base_row_height = 34
        header_height = 34
        max_visible_rows = 17   # 12화까지는 스크롤 없이 한 번에 보이도록

        if rows <= max_visible_rows:
            height = base_row_height * rows + header_height + 24
        else:
            height = base_row_height * max_visible_rows + header_height + 24

        AgGrid(
            df_numeric,
            gridOptions=gb.build(),
            theme="streamlit",
            height=height,
            fit_columns_on_grid_load=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,  # JS 사용
        )

    tv_numeric = _build_demo_table_numeric(f, ["TV"])
    _render_aggrid_table(tv_numeric, "📺 TV (시청자수)")

    tving_numeric = _build_demo_table_numeric(
        f, ["TVING LIVE", "TVING QUICK", "TVING VOD"]
    )
    _render_aggrid_table(tving_numeric, "▶︎ TVING 합산 시청자수")



#endregion


#region [ 9. 페이지 3: IP간 비교분석 (통합) ]
# =====================================================
# [수정] 성과 포지셔닝(레이더차트)에 회차 필터 연동 (백분위 재계산 로직 추가)

# ===== 10.0. 포맷팅 헬퍼 (페이지 4 전용) =====
def _fmt_kor_large(v):
    """N억 NNNN만 단위 포맷팅"""
    if v is None or pd.isna(v): return "–"
    val = float(v)
    if val == 0: return "0"
    
    uk = int(val // 100000000)
    man = int((val % 100000000) // 10000)
    
    if uk > 0:
        return f"{uk}억{man:04d}만"
    elif man > 0:
        return f"{man}만"
    else:
        return f"{int(val)}"

# ===== 10.1. [페이지 4] KPI 백분위 계산 (캐싱) =====
# [수정] max_ep 파라미터 추가 -> 필터 적용된 데이터로 전체 IP 백분위 재산출
@st.cache_data(ttl=600)
def get_kpi_data_for_all_ips(df_all: pd.DataFrame, max_ep: float = None) -> pd.DataFrame:
    """
    모든 IP에 대해 KPI 집계 후 백분위(0~100) 변환
    max_ep가 있으면 해당 회차까지만 잘라서 집계
    """
    df = df_all.copy()
    
    # 1. 회차 필터링 (전체 유니버스 축소)
    if "회차_numeric" not in df.columns:
        df["회차_numeric"] = df["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    
    df = df.dropna(subset=["회차_numeric"])
    
    if max_ep is not None:
        df = df[df["회차_numeric"] <= max_ep]

    # 2. 값 전처리
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.loc[df["value"] == 0, "value"] = np.nan
    df = df.dropna(subset=["value"])

    # 3. 지표별 집계 함수
    def _ip_mean_of_ep_mean(metric_name: str) -> pd.Series:
        sub = df[df["metric"] == metric_name]
        if sub.empty: return pd.Series(dtype=float, name=metric_name)
        ep_mean = sub.groupby(["IP", "회차_numeric"])["value"].mean().reset_index()
        return ep_mean.groupby("IP")["value"].mean().rename(metric_name)

    kpi_t_rating = _ip_mean_of_ep_mean("T시청률")
    kpi_h_rating = _ip_mean_of_ep_mean("H시청률")

    # TVING VOD + QUICK
    sub_vod_all = df[(df["metric"] == "시청인구") & (df["매체"].isin(["TVING VOD", "TVING QUICK"]))]
    if not sub_vod_all.empty:
        vod_ep_sum = sub_vod_all.groupby(["IP", "회차_numeric"])["value"].sum().reset_index()
        kpi_vod = vod_ep_sum.groupby("IP")["value"].mean().rename("TVING VOD")
    else:
        kpi_vod = pd.Series(dtype=float, name="TVING VOD")

    # TVING LIVE
    sub_live = df[(df["metric"] == "시청인구") & (df["매체"] == "TVING LIVE")]
    if not sub_live.empty:
        live_ep_sum = sub_live.groupby(["IP", "회차_numeric"])["value"].sum().reset_index()
        kpi_live = live_ep_sum.groupby("IP")["value"].mean().rename("TVING LIVE")
    else:
        kpi_live = pd.Series(dtype=float, name="TVING LIVE")

    # 디지털 조회수 / 언급량 (총합)
    # 주의: _get_view_data는 global scope 함수이므로 df를 넘김
    view_sub = _get_view_data(df) 
    if not view_sub.empty:
        kpi_view = view_sub.groupby("IP")["value"].sum().rename("디지털 조회수")
    else:
        kpi_view = pd.Series(dtype=float, name="디지털 조회수")

    buzz_sub = df[df["metric"] == "언급량"]
    if not buzz_sub.empty:
        kpi_buzz = buzz_sub.groupby("IP")["value"].sum().rename("디지털 언급량")
    else:
        kpi_buzz = pd.Series(dtype=float, name="디지털 언급량")

    kpi_f_score = _ip_mean_of_ep_mean("F_Score").rename("화제성 점수")

    # 4. 통합 및 백분위 산출
    kpi_df = pd.concat([kpi_t_rating, kpi_h_rating, kpi_vod, kpi_live, kpi_view, kpi_buzz, kpi_f_score], axis=1)
    kpi_percentiles = kpi_df.rank(pct=True) * 100
    return kpi_percentiles.fillna(0)


# ===== 10.2. [페이지 4] 단일 IP/그룹 KPI 계산 =====
def get_agg_kpis_for_ip_page4(df_ip: pd.DataFrame) -> Dict[str, float | None]:
    """
    단일 IP 또는 IP 그룹에 대한 주요 KPI 절대값 계산
    """
    kpis = {}
    kpis["T시청률"] = mean_of_ip_episode_mean(df_ip, "T시청률")
    kpis["H시청률"] = mean_of_ip_episode_mean(df_ip, "H시청률")
    
    kpis["TVING VOD"] = mean_of_ip_episode_sum(df_ip, "시청인구", ["TVING VOD", "TVING QUICK"])
    kpis["TVING LIVE"] = mean_of_ip_episode_sum(df_ip, "시청인구", ["TVING LIVE"])
    
    kpis["디지털 조회수"] = mean_of_ip_sums(df_ip, "조회수")
    kpis["디지털 언급량"] = mean_of_ip_sums(df_ip, "언급량")
    kpis["화제성 점수"] = mean_of_ip_episode_mean(df_ip, "F_Score")

    return kpis


# ===== 10.3. [페이지 4] KPI 카드 렌더링 (상단) =====
def _render_kpi_row_ip_vs_group(kpis_ip, kpis_group, ranks, group_name):
    
    def _calc_delta(ip_val, group_val): 
        ip_val = ip_val or 0
        group_val = group_val or 0
        if group_val is None or group_val == 0: return None
        return (ip_val - group_val) / group_val

    def _kpi_card_html(title, val_str, delta, rank_tuple):
        if delta is None:
            delta_html = "<span style='color:#9ca3af; font-size:13px;'>-</span>"
        else:
            pct = delta * 100
            color = "#d93636" if pct > 0 else ("#2a61cc" if pct < 0 else "#9ca3af")
            symbol = "▲" if pct > 0 else ("▼" if pct < 0 else "-")
            delta_html = f"<span style='color:{color}; font-size:13px; font-weight:600;'>{symbol} {abs(pct):.1f}%</span>"

        if rank_tuple and rank_tuple[1] > 0:
            rnk, total = rank_tuple
            rank_html = f"<span style='color:#6b7280; font-size:12px; margin-left:6px;'>({rnk}위/{total}작품)</span>"
        else:
            rank_html = ""
        
        return f"""
        <div class="kpi-card" style="padding: 14px 10px;">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value" style="font-size: 22px; margin-bottom: 4px;">{val_str}</div>
            <div style="line-height: 1.2;">
                {delta_html}{rank_html}
            </div>
        </div>
        """

    st.markdown(f"#### 1. 주요 성과 ({group_name} 대비)")
    
    keys = ["T시청률", "H시청률", "TVING LIVE", "TVING VOD", "디지털 조회수", "디지털 언급량", "화제성 점수"]
    titles = ["🎯 타깃시청률", "🏠 가구시청률", "⚡ 티빙 LIVE UV", "▶️ 티빙 VOD UV", "👀 디지털 조회", "💬 디지털 언급", "🔥 화제성 점수"]
    
    cols = st.columns(7)
    for i, key in enumerate(keys):
        val = kpis_ip.get(key)
        base_val = kpis_group.get(key)
        delta = _calc_delta(val, base_val)
        rank_info = ranks.get(key, (None, 0))
        
        if key in ["T시청률", "H시청률"]:
            val_str = f"{val:.2f}%" if val is not None else "–"
        elif key == "디지털 조회수":
            val_str = _fmt_kor_large(val)
        else:
            val_str = f"{val:,.0f}" if val is not None else "–"
            
        with cols[i]:
            st.markdown(_kpi_card_html(titles[i], val_str, delta, rank_info), unsafe_allow_html=True)


def _render_kpi_row_ip_vs_ip(kpis1, kpis2, ip1, ip2):
    def _card(title, v1, v2, fmt, higher_good=True):
        v1_disp = fmt.format(v1) if v1 is not None else "–"
        v2_disp = fmt.format(v2) if v2 is not None else "–"
        win = 0
        if v1 is not None and v2 is not None:
            if higher_good: win = 1 if v1 > v2 else (2 if v2 > v1 else 0)
            else: win = 1 if v1 < v2 else (2 if v2 < v1 else 0)
        
        s1 = "color:#d93636;font-weight:700" if win==1 else "color:#333"
        s2 = "color:#aaaaaa;font-weight:700" if win==2 else "color:#888"

        st.markdown(f"""
        <div class="kpi-card" style="padding:10px 10px;">
            <div class="kpi-title" style="margin-bottom:4px;">{title}</div>
            <div style="font-size:14px; line-height:1.4;">
                <span style="{s1}"><span style="font-size:11px;color:#d93636">{ip1}:</span> {v1_disp}</span><br>
                <span style="{s2}"><span style="font-size:11px;color:#aaaaaa">{ip2}:</span> {v2_disp}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("#### 1. 주요 성과 요약")
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    with c1: _card("🎯 타깃시청률", kpis1.get("T시청률"), kpis2.get("T시청률"), "{:.2f}%")
    with c2: _card("🏠 가구시청률", kpis1.get("H시청률"), kpis2.get("H시청률"), "{:.2f}%")
    with c3: _card("⚡ 티빙 LIVE UV", kpis1.get("TVING LIVE"), kpis2.get("TVING LIVE"), "{:,.0f}")
    with c4: _card("▶️ 티빙 VOD UV", kpis1.get("TVING VOD"), kpis2.get("TVING VOD"), "{:,.0f}")
    with c5: _card("👀 디지털 조회", kpis1.get("디지털 조회수"), kpis2.get("디지털 조회수"), "{:,.0f}")
    with c6: _card("💬 디지털 언급", kpis1.get("디지털 언급량"), kpis2.get("디지털 언급량"), "{:,.0f}")
    with c7: _card("🔥 화제성 점수", kpis1.get("화제성 점수"), kpis2.get("화제성 점수"), "{:,.0f}")


# ===== 10.4. [페이지 4] 통합 그래프 섹션 =====
def _render_unified_charts(
    df_target: pd.DataFrame, 
    df_comp: pd.DataFrame, 
    target_name: str, 
    comp_name: str,
    kpi_percentiles: pd.DataFrame,
    comp_color: str = "#aaaaaa"
):
    st.divider()

    # --- 2. 성과 포지셔닝 (Radar) & 시청률 비교 (Line) ---
    st.markdown("#### 2. 성과 포지셔닝 & 시청률")
    col_radar, col_rating = st.columns([1, 1])

    # [좌측] 성과 포지셔닝
    with col_radar:
        st.markdown("###### 성과 백분위 (Positioning)")
        
        radar_map = {
            "T시청률": "타깃시청률", "H시청률": "가구시청률", 
            "TVING LIVE": "티빙 LIVE", "TVING VOD": "티빙 VOD", 
            "디지털 조회수": "조회수", "디지털 언급량": "언급량", "화제성 점수": "화제성"
        }
        radar_metrics = list(radar_map.keys())
        radar_labels = list(radar_map.values())

        # Target Score
        if target_name in kpi_percentiles.index:
            score_t = kpi_percentiles.loc[target_name][radar_metrics]
        else:
            score_t = pd.Series(0, index=radar_metrics)
            
        # Comp Score
        if comp_name in kpi_percentiles.index: # IP vs IP
            score_c = kpi_percentiles.loc[comp_name][radar_metrics]
        else: # IP vs Group (그룹의 평균 백분위)
            group_ips = df_comp["IP"].unique()
            score_c = kpi_percentiles.loc[kpi_percentiles.index.isin(group_ips)].mean()[radar_metrics]

        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=score_t.values, theta=radar_labels,
            fill='toself', name=target_name, line=dict(color="#d93636")
        ))
        fig_radar.add_trace(go.Scatterpolar(
            r=score_c.values, theta=radar_labels,
            fill='toself', name=comp_name, line=dict(color=comp_color)
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            showlegend=True, height=350,
            margin=dict(l=50, r=50, t=30, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.05)
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    # [우측] 시청률 비교
    with col_rating:
        st.markdown(f"###### 시청률")
        
        df_target_rating = df_target[df_target["metric"].isin(["T시청률", "H시청률"])].copy()
        if "회차_numeric" not in df_target_rating.columns:
            df_target_rating["회차_numeric"] = df_target_rating["회차"].str.extract(r"(\d+)", expand=False).astype(float)
            
        max_ep = df_target_rating["회차_numeric"].max()
        if pd.isna(max_ep): max_ep = 999
        
        def _get_trend(df, metric):
            if "회차_numeric" not in df.columns:
                df["회차_numeric"] = df["회차"].str.extract(r"(\d+)", expand=False).astype(float)
            mask = (df["metric"] == metric)
            if pd.notna(max_ep):
                mask = mask & (df["회차_numeric"] <= max_ep)
            sub = df[mask].copy()
            return sub.groupby("회차_numeric")["value"].mean().sort_index()

        t_target = _get_trend(df_target, "T시청률")
        h_target = _get_trend(df_target, "H시청률")
        t_comp   = _get_trend(df_comp,   "T시청률")
        h_comp   = _get_trend(df_comp,   "H시청률")
        
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(x=h_target.index, y=h_target.values, name=f"{target_name}(가구)",
                                      mode='lines+markers', line=dict(color="#90a4ae", width=2)))
        fig_line.add_trace(go.Scatter(x=t_target.index, y=t_target.values, name=f"{target_name}(타깃)",
                                      mode='lines+markers', line=dict(color="#3949ab", width=2)))
        
        fig_line.add_trace(go.Scatter(x=h_comp.index, y=h_comp.values, name=f"{comp_name}(가구)",
                                      mode='lines+markers', line=dict(color="#90a4ae", width=2, dash='dot')))
        fig_line.add_trace(go.Scatter(x=t_comp.index, y=t_comp.values, name=f"{comp_name}(타깃)",
                                      mode='lines+markers', line=dict(color="#3949ab", width=2, dash='dot')))
        
        fig_line.update_layout(height=350, margin=dict(t=30, b=10), 
                               legend=dict(orientation="h", yanchor="bottom", y=1.02),
                               yaxis_title="시청률(%)", xaxis_title="회차")
        st.plotly_chart(fig_line, use_container_width=True)

    st.divider()

    # --- 3. 시청인구 비교 ---
    st.markdown("#### 3. 매체별 평균 시청인구")
    col_pop_tv, col_pop_tving = st.columns(2)

    def _get_demo_pop(df_src, medias):
        sub = df_src[(df_src["metric"]=="시청인구") & (df_src["매체"].isin(medias)) & df_src["데모"].notna()].copy()
        sub["성별"] = sub["데모"].apply(_gender_from_demo)
        sub["연령"] = sub["데모"].apply(_to_decade_label)
        sub = sub[sub["성별"].isin(["남","여"]) & (sub["연령"]!="기타")]
        sub["label"] = sub.apply(lambda r: f"{r['연령']}{'남성' if r['성별']=='남' else '여성'}", axis=1)
        
        if "회차_numeric" not in sub.columns:
             sub["회차_numeric"] = sub["회차"].str.extract(r"(\d+)", expand=False).astype(float)
        
        agg = sub.groupby(["IP","회차_numeric","label"])["value"].sum().reset_index()
        return agg.groupby("label")["value"].mean()

    with col_pop_tv:
        st.markdown("###### 📺 TV (평균 시청인구)")
        pop_t = _get_demo_pop(df_target, ["TV"])
        pop_c = _get_demo_pop(df_comp,   ["TV"])
        df_bar = pd.DataFrame({target_name: pop_t, comp_name: pop_c}).fillna(0).reset_index()
        df_melt = df_bar.melt(id_vars="label", var_name="구분", value_name="인구수")
        
        sort_map = {col: i for i, col in enumerate(DEMO_COLS_ORDER)}
        df_melt["s"] = df_melt["label"].map(sort_map).fillna(999)
        df_melt = df_melt.sort_values("s")
        
        if not df_melt.empty:
            fig_tv = px.bar(df_melt, x="label", y="인구수", color="구분", barmode="group",
                            color_discrete_map={target_name: "#d93636", comp_name: comp_color},
                            text="인구수")
            fig_tv.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig_tv.update_layout(height=300, margin=dict(t=30), legend=dict(title=None, orientation="h", y=1.02),
                                 xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig_tv, use_container_width=True)
        else:
            st.info("데이터 없음")

    with col_pop_tving:
        st.markdown("###### ▶️ TVING (평균 시청인구)")
        tving_ms = ["TVING LIVE", "TVING QUICK", "TVING VOD"]
        pop_t = _get_demo_pop(df_target, tving_ms)
        pop_c = _get_demo_pop(df_comp,   tving_ms)
        df_bar = pd.DataFrame({target_name: pop_t, comp_name: pop_c}).fillna(0).reset_index()
        df_melt = df_bar.melt(id_vars="label", var_name="구분", value_name="인구수")
        
        sort_map = {col: i for i, col in enumerate(DEMO_COLS_ORDER)}
        df_melt["s"] = df_melt["label"].map(sort_map).fillna(999)
        df_melt = df_melt.sort_values("s")
        
        if not df_melt.empty:
            fig_tv = px.bar(df_melt, x="label", y="인구수", color="구분", barmode="group",
                            color_discrete_map={target_name: "#d93636", comp_name: comp_color},
                            text="인구수")
            fig_tv.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig_tv.update_layout(height=300, margin=dict(t=30), legend=dict(title=None, orientation="h", y=1.02),
                                 xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig_tv, use_container_width=True)
        else:
            st.info("데이터 없음")

    st.divider()

    # --- 4. 디지털 비교 (도넛차트) ---
    st.markdown("#### 4. 디지털 반응")
    col_dig_view, col_dig_buzz = st.columns(2)

    def _get_pie_data(df_src, metric):
        if metric == "조회수":
            sub = _get_view_data(df_src) # [3. 공통 함수]
        else:
            sub = df_src[df_src["metric"] == metric].copy()
        
        if sub.empty: return pd.DataFrame(columns=["매체", "val"])
        
        per_ip_media = sub.groupby(["IP", "매체"])["value"].sum().reset_index()
        avg_per_media = per_ip_media.groupby("매체")["value"].mean().reset_index().rename(columns={"value":"val"})
        
        return avg_per_media

    def _draw_scaled_donuts_fixed_color(df_t, df_c, title, t_name, c_name):
        from plotly.subplots import make_subplots
        
        all_media = set(df_t["매체"].unique()) | set(df_c["매체"].unique())
        sorted_media = sorted(list(all_media))
        
        base_colors = ['#5c6bc0', '#7e57c2', '#26a69a', '#66bb6a', '#ffa726', '#ef5350', '#8d6e63', '#78909c']
        color_map = {m: base_colors[i % len(base_colors)] for i, m in enumerate(sorted_media)}
        
        df_t["color"] = df_t["매체"].map(color_map)
        df_c["color"] = df_c["매체"].map(color_map)

        fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]],
                            subplot_titles=[f"{t_name}", f"{c_name}"])
        
        sum_t = df_t["val"].sum() if not df_t.empty else 0
        sum_c = df_c["val"].sum() if not df_c.empty else 0
        
        if not df_t.empty:
            fig.add_trace(go.Pie(
                labels=df_t["매체"], values=df_t["val"], 
                name=t_name, scalegroup='one', hole=0.4,
                title=f"Total<br>{_fmt_kor_large(sum_t)}", title_font=dict(size=14),
                marker=dict(colors=df_t["color"]), 
                domain=dict(column=0),
                sort=False 
            ), 1, 1)
        
        if not df_c.empty:
            fig.add_trace(go.Pie(
                labels=df_c["매체"], values=df_c["val"], 
                name=c_name, scalegroup='one', hole=0.4,
                title=f"Total<br>{_fmt_kor_large(sum_c)}", title_font=dict(size=14),
                marker=dict(colors=df_c["color"]), 
                domain=dict(column=1),
                sort=False
            ), 1, 2)
        
        fig.update_layout(height=320, margin=dict(t=30, b=10, l=10, r=10),
                          legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"))
        return fig

    with col_dig_view:
        st.markdown("###### 👀 디지털 조회수 비교")
        pie_t = _get_pie_data(df_target, "조회수")
        pie_c = _get_pie_data(df_comp,   "조회수")
        
        if pie_t.empty and pie_c.empty:
            st.info("데이터 없음")
        else:
            fig_pie = _draw_scaled_donuts_fixed_color(pie_t, pie_c, "조회수", target_name, comp_name)
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_dig_buzz:
        st.markdown("###### 💬 디지털 언급량 비교")
        pie_t = _get_pie_data(df_target, "언급량")
        pie_c = _get_pie_data(df_comp,   "언급량")
        
        if pie_t.empty and pie_c.empty:
            st.info("데이터 없음")
        else:
            fig_pie = _draw_scaled_donuts_fixed_color(pie_t, pie_c, "언급량", target_name, comp_name)
            st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # --- 5. [통합] 오디언스 히트맵 ---
    st.markdown("#### 5. 👥 IP 오디언스 히트맵")
    st.caption(f"선택하신 **'{target_name}'**과 **'{comp_name}'**의 회차별/데모별 시청자수 격차를 보여줍니다.")
    
    heatmap_media = st.radio("분석 매체", ["TV", "TVING"], index=0, horizontal=True, label_visibility="collapsed", key="heatmap_media_page4")
    media_list = ["TV"] if heatmap_media == "TV" else ["TVING LIVE", "TVING QUICK", "TVING VOD"]
    media_label = "TV" if heatmap_media == "TV" else "TVING"

    if "회차_numeric" not in df_target.columns: 
         df_target["회차_numeric"] = df_target["회차"].str.extract(r"(\d+)", expand=False).astype(float)
    if "회차_numeric" not in df_comp.columns:
         df_comp["회차_numeric"] = df_comp["회차"].str.extract(r"(\d+)", expand=False).astype(float)

    df_base_heat = get_avg_demo_pop_by_episode(df_target, media_list, max_ep=None) 
    df_comp_heat = get_avg_demo_pop_by_episode(df_comp, media_list, max_ep=None)

    if df_base_heat.empty:
        st.warning(f"기준 IP({target_name})의 히트맵 데모 데이터를 생성할 수 없습니다.")
    else:
        if df_comp_heat.empty:
             st.warning(f"비교 대상({comp_name})의 히트맵 데이터가 없어 비교값은 0으로 처리됩니다.")
             df_comp_heat = pd.DataFrame({'회차': df_base_heat['회차']})
             for col in DEMO_COLS_ORDER: df_comp_heat[col] = 0.0

        df_merged = pd.merge(df_base_heat, df_comp_heat, on="회차", suffixes=('_base', '_comp'), how='left')
        df_index = df_merged[["회차"]].copy()

        for col in DEMO_COLS_ORDER: 
            base_col = col + '_base'
            comp_col = col + '_comp'

            df_merged[base_col] = pd.to_numeric(df_merged.get(base_col), errors='coerce').fillna(0.0)
            df_merged[comp_col] = pd.to_numeric(df_merged.get(comp_col), errors='coerce').fillna(0.0)

            base_values = df_merged[base_col].values
            comp_values = df_merged[comp_col].values

            index_values = np.where(
                comp_values != 0,
                ((base_values - comp_values) / comp_values) * 100,
                np.where(base_values == 0, 0.0, 999)
            )
            df_index[col] = index_values

        table_title = f"{media_label} 연령대별 시청자수 차이 ({target_name} vs {comp_name})"
        render_heatmap(df_index, table_title) # [6. 공통 함수]


# ===== 10.5. [페이지 4] 메인 렌더링 함수 =====
def render_comparison():
    df_all = load_data() # [3. 공통 함수]
    
    if "회차_numeric" not in df_all.columns:
        df_all["회차_numeric"] = df_all["회차"].str.extract(r"(\d+)", expand=False).astype(float)

    # [핵심] 백분위 계산시에는 아직 필터값이 없으므로 일단 전체 로드 후, 아래에서 재계산 호출
    # (초기값은 전체로 둠)
    kpi_percentiles = get_kpi_data_for_all_ips(df_all, max_ep=None)

    ip_options = sorted(df_all["IP"].dropna().unique().tolist())
    selected_ip1 = None
    selected_ip2 = None

    current_mode = st.session_state.get("comp_mode_page4", "IP vs 그룹 평균")
    
    if current_mode == "IP vs IP":
        filter_cols = st.columns([3, 2, 2, 2, 3])
    else:
        filter_cols = st.columns([3, 2, 2, 2, 2, 1]) 
    
    # --- 헤더 및 모드 선택 ---
    with filter_cols[0]:
        st.markdown("## ⚖️ 성과 비교분석")
    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("<div class='gd-guideline'>", unsafe_allow_html=True)
        st.markdown(textwrap.dedent("""
            **지표 기준**
        - **시청률** `회차평균`: 전국 기준 가구 & 타깃(2049) 시청률
        - **티빙 LIVE** `회차평균`: 실시간 시청 UV
        - **티빙 당일 VOD** `회차평균`: 본방송 당일 VOD UV
        - **티빙 주간 VOD** `회차평균`: [회차 방영일부터 +6일까지의 7일간 VOD UV] - [티빙 당일 VOD]
        - **디지털 조회** `회차총합`: 방영주간 월~일 발생 총합 / 유튜브,인스타그램,틱톡,네이버TV,페이스북
        - **디지털 언급량** `회차총합`: 방영주차(월~일) 내 총합 / 커뮤니티,트위터,블로그                            
        - **화제성 점수** `회차평균`: 방영기간 주차별 화제성 점수의 평균 (펀덱스)
        """).strip())
        st.markdown("</div>", unsafe_allow_html=True)

    with filter_cols[1]:
        comparison_mode = st.radio(
            "비교 모드", 
            ["IP vs IP", "IP vs 그룹 평균"], 
            index=1, horizontal=True, label_visibility="collapsed",
            key="comp_mode_page4" 
        ) 
    
    selected_max_ep = "전체"

    # --- IP vs IP 모드 ---
    if comparison_mode == "IP vs IP":
        with filter_cols[2]:
            selected_ip1 = st.selectbox(
                "기준 IP", ip_options, index=0 if ip_options else None, 
                label_visibility="collapsed"
            )
        with filter_cols[3]:
            ip_options_2 = [ip for ip in ip_options if ip != selected_ip1]
            selected_ip2 = st.selectbox(
                "비교 IP", ip_options_2, 
                index=1 if len(ip_options_2) > 1 else (0 if len(ip_options_2) > 0 else None), 
                label_visibility="collapsed"
            )
        
        target_rows = df_all[df_all["IP"] == selected_ip1] if selected_ip1 else pd.DataFrame()
        ep_opts = ["전체"] + get_episode_options(target_rows)
        
        with filter_cols[4]:
            selected_max_ep = st.selectbox(
                "회차 범위 (누적)", ep_opts, index=0,
                label_visibility="collapsed"
            )
        
        use_same_prog = False
        selected_years = []

    # --- IP vs 그룹 평균 모드 ---
    else: 
        base_ip_info_rows = df_all[df_all["IP"] == selected_ip1];
        
        # [수정] '편성연도' 컬럼 사용
        all_years = []
        if "편성연도" in df_all.columns:
            unique_vals = df_all["편성연도"].dropna().unique()
            try:
                all_years = sorted(unique_vals, reverse=True)
            except:
                all_years = sorted([str(x) for x in unique_vals], reverse=True)

        # Default selection
        default_year_list = []
        if "편성연도" in base_ip_info_rows.columns:
            y_mode = base_ip_info_rows["편성연도"].dropna().mode()
            if not y_mode.empty:
                default_year_list = [y_mode.iloc[0]]

        with filter_cols[2]:
            selected_ip1 = st.selectbox(
                "기준 IP", ip_options, index=0 if ip_options else None, 
                label_visibility="collapsed"
            )

        with filter_cols[3]:
            comp_type = st.selectbox(
                "동일 편성 기준", ["동일 편성", "전체"], index=0,
                key="comp_prog_page4", label_visibility="collapsed"
            )
            use_same_prog = (comp_type == "동일 편성")

        with filter_cols[4]:
            selected_years = st.multiselect(
                "방영 연도", all_years, default=default_year_list,
                key="comp_year_page4", placeholder="연도 선택", label_visibility="collapsed"
            )
        
        target_rows = df_all[df_all["IP"] == selected_ip1] if selected_ip1 else pd.DataFrame()
        ep_opts = ["전체"] + get_episode_options(target_rows)

        with filter_cols[5]:
            selected_max_ep = st.selectbox(
                "회차 범위", ep_opts, index=0,
                label_visibility="collapsed"
            )

    st.divider()

    # --- 데이터 준비 및 필터링 ---
    if not selected_ip1:
        st.info("기준 IP를 선택해주세요.")
        return

    # [핵심] 회차 필터 숫자 추출 및 백분위 재계산
    ep_limit = None
    if selected_max_ep != "전체":
        try:
            ep_limit = float(re.findall(r'\d+', str(selected_max_ep))[0])
        except:
            ep_limit = None
            
    # 필터된 회차 기준으로 전체 IP 백분위 다시 가져오기
    kpi_percentiles = get_kpi_data_for_all_ips(df_all, max_ep=ep_limit)

    # 기준 IP 데이터 필터링
    df_target = df_all[df_all["IP"] == selected_ip1].copy()
    if ep_limit is not None:
        df_target = df_target[df_target["회차_numeric"] <= ep_limit]
    
    kpis_target = get_agg_kpis_for_ip_page4(df_target)

    # 비교 그룹 데이터 준비
    if comparison_mode == "IP vs 그룹 평균":
        group_name_parts = []
        df_comp = df_all.copy()
        
        ip_prog = df_target["편성"].dropna().mode().iloc[0] if not df_target["편성"].dropna().empty else None
        
        if use_same_prog: 
            if ip_prog:
                df_comp = df_comp[df_comp["편성"] == ip_prog]
                group_name_parts.append(f"'{ip_prog}'")
            else: st.warning("편성 정보 없음 (제외)")
        
        if selected_years:
            # [수정] 값 직접 비교
            df_comp = df_comp[df_comp["편성연도"].isin(selected_years)]

            if len(selected_years) <= 3:
                years_str = ",".join(map(str, sorted(selected_years)))
                group_name_parts.append(f"{years_str}") # '년' 제거 (데이터에 포함됨)
            else:
                try:
                    group_name_parts.append(f"{min(selected_years)}~{max(selected_years)}")
                except:
                    group_name_parts.append("선택연도")
        
        if not group_name_parts: group_name_parts.append("전체")
        comp_name = " & ".join(group_name_parts) + " 평균"

        # 비교 그룹도 회차 필터 적용
        if ep_limit is not None:
             df_comp = df_comp[df_comp["회차_numeric"] <= ep_limit]

        kpis_comp = get_agg_kpis_for_ip_page4(df_comp)
        
        # [추가] 그룹 내 순위 계산 로직
        ranks = {}
        
        def _calc_rank_in_group(df_g, target_val, metric_key, higher_good=True):
            # 1. 그룹 내 모든 IP별 KPI 계산
            if df_g.empty: return (None, 0)
            
            if metric_key in ["T시청률", "H시청률", "화제성 점수"]:
                agg = df_g[df_g["metric"] == (metric_key if metric_key != "화제성 점수" else "F_Score")]
                if agg.empty: return (None, 0)
                ep_agg = agg.groupby(["IP", "회차_numeric"])["value"].mean().reset_index()
                ip_series = ep_agg.groupby("IP")["value"].mean()
                
            elif metric_key in ["TVING VOD", "TVING LIVE"]:
                media_target = ["TVING LIVE"] if metric_key == "TVING LIVE" else ["TVING VOD", "TVING QUICK"]
                agg = df_g[(df_g["metric"] == "시청인구") & (df_g["매체"].isin(media_target))]
                if agg.empty: return (None, 0)
                ep_agg = agg.groupby(["IP", "회차_numeric"])["value"].sum().reset_index()
                ip_series = ep_agg.groupby("IP")["value"].mean()
                
            elif metric_key in ["디지털 조회수", "디지털 언급량"]:
                if metric_key == "디지털 조회수":
                    agg = _get_view_data(df_g)
                else:
                    agg = df_g[df_g["metric"] == "언급량"]
                if agg.empty: return (None, 0)
                ip_series = agg.groupby("IP")["value"].sum()
            else:
                return (None, 0)

            if target_val is not None:
                ip_series[selected_ip1] = target_val
            
            if ip_series.empty: return (None, 0)
            
            ranked = ip_series.rank(method='min', ascending=not higher_good)
            
            try:
                my_rank = int(ranked[selected_ip1])
                total_cnt = len(ip_series)
                return (my_rank, total_cnt)
            except:
                return (None, len(ip_series))

        keys_map = {
            "T시청률": "T시청률", "H시청률": "H시청률", 
            "TVING LIVE": "TVING LIVE", "TVING VOD": "TVING VOD",
            "디지털 조회수": "디지털 조회수", "디지털 언급량": "디지털 언급량",
            "화제성 점수": "화제성 점수"
        }
        
        for k in keys_map:
            val = kpis_target.get(k)
            ranks[k] = _calc_rank_in_group(df_comp, val, k)

        _render_kpi_row_ip_vs_group(kpis_target, kpis_comp, ranks, comp_name)
        _render_unified_charts(df_target, df_comp, selected_ip1, comp_name, kpi_percentiles, comp_color="#aaaaaa")

    else: # IP vs IP
        if not selected_ip2:
            st.warning("비교할 IP를 선택해주세요.")
            return
            
        df_comp = df_all[df_all["IP"] == selected_ip2].copy()

        if ep_limit is not None:
             df_comp = df_comp[df_comp["회차_numeric"] <= ep_limit]

        kpis_comp = get_agg_kpis_for_ip_page4(df_comp)
        comp_name = selected_ip2
        
        _render_kpi_row_ip_vs_ip(kpis_target, kpis_comp, selected_ip1, selected_ip2)
        _render_unified_charts(df_target, df_comp, selected_ip1, comp_name, kpi_percentiles, comp_color="#aaaaaa")
#endregion

#region [ 10. 페이지 4: 성장스코어-방영성과 ]
# =====================================================
# [수정] 2025-11-19: 비교 그룹(동일 편성) 필터 추가 및 레이아웃 조정
def render_growth_score():
    """
    [페이지 6] 성장스코어-방영지표 렌더링 함수
    """
    df_all = load_data().copy() # [3. 공통 함수]

    # ---------- 설정 ----------
    EP_CHOICES = [2, 4, 6, 8, 10, 12, 14, 16]
    ROW_LABELS = ["S","A","B","C","D"]
    COL_LABELS = ["+2","+1","0","-1","-2"]
    ABS_SCORE  = {"S":5,"A":4,"B":3,"C":2,"D":1}
    SLO_SCORE  = {"+2":5,"+1":4,"0":3,"-1":2,"-2":1}
    SLOPE_LABELS = ["+2", "+1", "0", "-1", "-2"]
    NETFLIX_VOD_FACTOR = 1.4
    ABS_NUM = {"S":5, "A":4, "B":3, "C":2, "D":1} # 회차별 추이용

    METRICS = [
        ("가구시청률", "H시청률", None),
        ("타깃시청률", "T시청률", None),
        ("TVING LIVE", "시청인구", "LIVE"),
        ("TVING VOD",  "시청인구", "VOD"),
    ]

    # 전체 IP 리스트 (선택 박스용)
    all_ip_list = sorted(df_all["IP"].dropna().unique().tolist())
    if not all_ip_list:
        st.warning("IP 데이터가 없습니다."); return

    st.markdown("""
    <style>
      div[data-testid="stVerticalBlockBorderWrapper"]:has(.growth-kpi) .kpi-card {
          border-radius:16px;border:1px solid #e7ebf3;background:#fff;padding:12px 14px;
          box-shadow:0 1px 2px rgba(0,0,0,0.04);
      }
      .growth-kpi .kpi-title{font-size:13px;color:#5b6b83;margin-bottom:4px;font-weight:600}
      .growth-kpi .kpi-value{font-weight:800;letter-spacing:-0.2px}
    </style>
    """, unsafe_allow_html=True)

    # ---------- 헤더(타이틀/선택/필터) ----------
    _ep_display = st.session_state.get("growth_ep_cutoff", 4)

    # [수정] 필터 추가를 위해 컬럼 비율 조정 (3개 -> 4개)
    head = st.columns([4, 2, 2, 2])
    
    with head[0]:
        st.markdown(
            f"## 🚀 성장스코어-방영지표 <span style='font-size:20px;color:#6b7b93'>(~{_ep_display}회 기준)</span>",
            unsafe_allow_html=True
        )
    
    with head[1]:
        selected_ip = st.selectbox(
            "IP 선택", all_ip_list, index=0,
            key="growth_ip_select", label_visibility="collapsed"
        )

    # [수정] 비교 그룹 필터 추가
    with head[2]:
        comp_group_mode = st.selectbox(
            "비교 그룹", 
            ["전체 비교", "동일 편성만"], 
            index=0,
            key="growth_comp_mode", 
            label_visibility="collapsed"
        )

    with head[3]:
        ep_cutoff = st.selectbox(
            "회차 기준", EP_CHOICES, index=1,
            key="growth_ep_cutoff", label_visibility="collapsed"
        )

    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("""
    **등급 체계**
    - **절대값 등급**: 각 항목별(시청률,티빙UV) 수치를 비교군 내 순위화→ `S / A / B / C / D`
    - **상승률 등급**: 각 항목별(시청률,티빙UV)의 회차별 증감정도를 비교군 내 순위화 → `+2 / +1 / 0 / -1 / -2`
    - **종합등급**: 절대값과 상승률 등급을 결합해 표기 (예: `A+2`).

    **보정기준**
    - 넷플릭스 편성작품은 넷플릭스 비 편성작 대비 평균적으로 약 40%정도의 TVING VOD수치의 손실이 있으며, 그에 따라 등급산출시 40%보정
            """)

    # ---------- [로직 추가] IP 필터링 (비교군 설정) ----------
    ips = all_ip_list[:] # 기본은 전체
    
    if comp_group_mode == "동일 편성만":
        # 선택된 IP의 편성 정보 조회
        target_info = df_all[df_all["IP"] == selected_ip]
        if not target_info.empty:
            target_prog = target_info["편성"].dropna().mode()
            if not target_prog.empty:
                prog_val = target_prog.iloc[0]
                # 동일 편성 IP만 필터링
                ips = sorted(df_all[df_all["편성"] == prog_val]["IP"].unique().tolist())
                
                # 만약 데이터 오류로 본인이 빠졌다면 강제 추가
                if selected_ip not in ips:
                    ips.append(selected_ip)
                    
                st.markdown(
                    f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기 (비교군: {prog_val} / 총 {len(ips)}작품)</span>",
                    unsafe_allow_html=True
                )
            else:
                st.warning(f"'{selected_ip}'의 편성 정보가 없어 전체 IP와 비교합니다.")
                st.markdown(f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기 (전체 비교)</span>", unsafe_allow_html=True)
        else:
            st.markdown(f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기</span>", unsafe_allow_html=True)
    else:
        st.markdown(
            f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기 (전체 비교 / 총 {len(ips)}작품)</span>",
            unsafe_allow_html=True
        )

    # ---------- [최적화] 데이터 전처리 및 계산 로직 분리 ----------
    
    # 1. 전체 IP에 대해 회차별 숫자형 컬럼 생성 (Loop 밖에서 처리)
    if "회차_numeric" not in df_all.columns:
        df_all["회차_numeric"] = df_all["회차"].astype(str).str.extract(r"(\d+)", expand=False).astype(float)
    
    # 2. IP별 데이터프레임 딕셔너리 생성 (필터링 비용 절감) - 필터링된 ips만 사용
    ip_dfs = {ip: df_all[df_all["IP"] == ip].copy() for ip in ips}

    # 3. [Helper] 전체 데이터를 Numpy Array로 추출하는 함수
    def _get_full_series(ip_df, metric, media):
        """특정 IP, Metric의 전체 회차 데이터를 (x, y) Numpy Array로 반환"""
        sub = ip_df[ip_df["metric"] == metric].copy()
        
        if media == "LIVE":
            sub = sub[sub["매체"] == "TVING LIVE"]
        elif media == "VOD":
            sub = sub[sub["매체"] == "TVING VOD"]
            # 넷플릭스 보정
            if "넷플릭스편성작" in sub.columns:
                is_netflix = (sub["넷플릭스편성작"] == 1)
                if is_netflix.any():
                    sub.loc[is_netflix, "value"] = pd.to_numeric(sub.loc[is_netflix, "value"], errors="coerce") * NETFLIX_VOD_FACTOR

        sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
        sub = sub.dropna(subset=["value", "회차_numeric"])
        
        if sub.empty: return None
        
        if metric in ["H시청률", "T시청률"]:
            s = sub.groupby("회차_numeric")["value"].mean().reset_index()
        else:
            s = sub.groupby("회차_numeric")["value"].sum().reset_index()
            
        s = s.sort_values("회차_numeric")
        return s["회차_numeric"].values.astype(float), s["value"].values.astype(float)

    # 4. [Pre-Calculation] 필터링된 IP들의 Metric별 전체 (x, y) 데이터를 미리 추출
    ip_metric_cache = {}
    for ip in ips:
        ip_metric_cache[ip] = {}
        curr_df = ip_dfs[ip]
        for disp, metric, media in METRICS:
            ip_metric_cache[ip][disp] = _get_full_series(curr_df, metric, media)

    # 5. [Calculation] Numpy Slicing을 이용한 통계 계산
    def _calc_stats_from_cache(xy_tuple, n_cutoff, metric_type):
        if xy_tuple is None: return np.nan, np.nan
        
        x, y = xy_tuple
        mask = x <= float(n_cutoff)
        x_sub, y_sub = x[mask], y[mask]
        
        if len(x_sub) == 0: return np.nan, np.nan
        
        # Abs Value
        if metric_type in ["가구시청률", "타깃시청률"]:
            abs_val = np.mean(y_sub)
        else:
            abs_val = np.mean(y_sub)
            
        # Slope
        if len(x_sub) < 2:
            slope = np.nan
        else:
            try:
                slope = np.polyfit(x_sub, y_sub, 1)[0]
            except:
                slope = np.nan
        return abs_val, slope

    def _quintile_grade(series, labels):
        s = pd.Series(series).astype(float)
        valid = s.dropna()
        if valid.empty: return pd.Series(index=s.index, data=np.nan)
        ranks = valid.rank(method="average", ascending=False, pct=True)
        bins = [0, .2, .4, .6, .8, 1.0000001]
        idx = np.digitize(ranks.values, bins, right=True) - 1
        idx = np.clip(idx, 0, 4)
        out = pd.Series([labels[i] for i in idx], index=valid.index)
        return out.reindex(s.index)

    def _to_percentile(s):
        s = pd.Series(s).astype(float)
        return s.rank(pct=True) * 100

    # ---------- [메인 로직] 회차별 등급 산출 (Loop Optimized) ----------
    
    # 선택된 IP의 최대 회차 확인
    sel_ip_df = df_all[df_all["IP"] == selected_ip]
    if "회차_numeric" in sel_ip_df.columns:
        _max_ep_val = pd.to_numeric(sel_ip_df["회차_numeric"], errors="coerce").max()
    else:
        _max_ep_val = 0
    
    if pd.isna(_max_ep_val) or _max_ep_val == 0:
        _Ns = [min(EP_CHOICES)]
    else:
        _Ns = [n for n in EP_CHOICES if n <= _max_ep_val]
    
    needed_cutoffs = set(_Ns)
    needed_cutoffs.add(ep_cutoff)
    sorted_cutoffs = sorted(list(needed_cutoffs))

    evo_rows = []
    base_for_current_cutoff = None 

    # 통합 Loop
    for n in sorted_cutoffs:
        tmp_rows = []
        for ip in ips: # 필터링된 IP들만 순회
            row = {"IP": ip}
            for disp, _, _ in METRICS:
                xy = ip_metric_cache[ip][disp]
                abs_v, slope_v = _calc_stats_from_cache(xy, n, disp)
                row[f"{disp}_절대"] = abs_v
                row[f"{disp}_기울기"] = slope_v
            tmp_rows.append(row)
        
        tmp_df = pd.DataFrame(tmp_rows)
        
        # 등급 산정 (필터링된 그룹 내에서의 상대평가)
        for disp, _, _ in METRICS:
            tmp_df[f"{disp}_절대등급"] = _quintile_grade(tmp_df[f"{disp}_절대"], ["S","A","B","C","D"])
            tmp_df[f"{disp}_상승등급"] = _quintile_grade(tmp_df[f"{disp}_기울기"], SLOPE_LABELS)
            tmp_df[f"{disp}_종합"] = tmp_df[f"{disp}_절대등급"].astype(str) + tmp_df[f"{disp}_상승등급"].astype(str).replace("nan", "")
        
        tmp_df["_ABS_PCT_MEAN"] = pd.concat([_to_percentile(tmp_df[f"{d}_절대"]) for d,_,_ in METRICS], axis=1).mean(axis=1)
        tmp_df["_SLOPE_PCT_MEAN"] = pd.concat([_to_percentile(tmp_df[f"{d}_기울기"]) for d,_,_ in METRICS], axis=1).mean(axis=1)
        tmp_df["종합_절대등급"] = _quintile_grade(tmp_df["_ABS_PCT_MEAN"], ["S","A","B","C","D"])
        tmp_df["종합_상승등급"] = _quintile_grade(tmp_df["_SLOPE_PCT_MEAN"], SLOPE_LABELS)
        tmp_df["종합등급"] = tmp_df["종합_절대등급"].astype(str) + tmp_df["종합_상승등급"].astype(str).replace("nan", "")

        # 현재 Cutoff(상단 카드용) 데이터 저장
        if n == ep_cutoff:
            base = tmp_df.copy() 

        # 그래프용 데이터 수집
        if n in _Ns:
            row = tmp_df[tmp_df["IP"] == selected_ip]
            if not row.empty and pd.notna(row.iloc[0]["종합_절대등급"]):
                ag = str(row.iloc[0]["종합_절대등급"])
                sg = str(row.iloc[0]["종합_상승등급"]) if pd.notna(row.iloc[0]["종합_상승등급"]) else ""
                evo_rows.append({
                    "N": n,
                    "회차라벨": f"{n}회차",
                    "ABS_GRADE": ag,
                    "SLOPE_GRADE": sg,
                    "ABS_NUM": ABS_NUM.get(ag, np.nan)
                })

    if 'base' not in locals(): base = tmp_df.copy()

    # ---------- [선택작품 요약카드] ----------
    try:
        focus = base[base["IP"] == selected_ip].iloc[0]
    except IndexError:
        st.error(f"선택된 IP({selected_ip})의 데이터를 계산할 수 없습니다.")
        return

    st.markdown("<div class='growth-kpi'>", unsafe_allow_html=True)
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
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    
    # ===== [회차별 등급 추이: 선택 IP] =====
    evo = pd.DataFrame(evo_rows)
    if evo.empty:
        st.info("회차별 등급 추이를 표시할 데이터가 부족합니다.")
    else:
        fig_e = go.Figure()
        fig_e.add_vrect(
            x0=ep_cutoff - 0.5, x1=ep_cutoff + 0.5,
            fillcolor="rgba(0,90,200,0.12)", line_width=0
        )
        fig_e.add_trace(go.Scatter(
            x=evo["N"], y=evo["ABS_NUM"],
            mode="lines+markers",
            line=dict(shape="spline", width=3),
            marker=dict(size=8),
            name=selected_ip,
            hoverinfo="skip"
        ))
        for xi, yi, ag, sg in zip(evo["N"], evo["ABS_NUM"], evo["ABS_GRADE"], evo["SLOPE_GRADE"]):
            label = f"{ag}{sg}" if isinstance(ag, str) and sg else ag
            fig_e.add_annotation(
                x=xi, y=yi, text=label, showarrow=False,
                font=dict(size=12, color="#333", family="sans-serif"), yshift=14
            )
        fig_e.update_xaxes(
            tickmode="array",
            tickvals=evo["N"].tolist(),
            ticktext=[f"{int(n)}회차" for n in evo["N"].tolist()],
            showgrid=False, zeroline=False, showline=False
        )
        fig_e.update_yaxes(
            tickmode="array",
            tickvals=[5,4,3,2,1],
            ticktext=["S","A","B","C","D"],
            range=[0.7, 5.3],
            showgrid=False, zeroline=False, showline=False
        )
        fig_e.update_layout(
            height=200,
            margin=dict(l=8, r=8, t=8, b=8),
            showlegend=False
        )
        
        c_evo, = st.columns(1)
        with c_evo:
            st.plotly_chart(fig_e, use_container_width=True, config={"displayModeBar": False})

    st.divider()

    # ---------- [포지셔닝맵] ----------
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

    fig = px.imshow(
        z,
        x=COL_LABELS, y=ROW_LABELS,
        origin="upper",
        color_continuous_scale="Blues",
        range_color=[1, 5],
        text_auto=False,
        aspect="auto"
    ).update_traces(xgap=0.0, ygap=0.0)

    fig.update_xaxes(showticklabels=False, title=None, ticks="")
    fig.update_yaxes(showticklabels=False, title=None, ticks="")
    fig.update_layout(
        height=760,
        margin=dict(l=2, r=2, t=2, b=2),
        coloraxis_showscale=False
    )
    fig.update_traces(hovertemplate="<extra></extra>")

    def _font_color(val: float) -> str:
        return "#FFFFFF" if val >= 3.3 else "#111111"

    for r_idx, rr in enumerate(ROW_LABELS):
        for c_idx, cc in enumerate(COL_LABELS):
            cell_val = z[r_idx][c_idx]
            names = pos_map[(rr, cc)]
            color = _font_color(cell_val)

            fig.add_annotation(
                x=cc, y=rr, xref="x", yref="y",
                text=f"<b style='letter-spacing:0.5px'>{rr}{cc}</b>",
                showarrow=False,
                font=dict(size=22, color=color, family="sans-serif"),
                xanchor="center", yanchor="top",
                xshift=0, yshift=80, align="left"
            )

            if names:
                fig.add_annotation(
                    x=cc, y=rr, xref="x", yref="y",
                    text=f"<span style='line-height:1.04'>{'<br>'.join(names)}</span>",
                    showarrow=False,
                    font=dict(size=12, color=color, family="sans-serif"),
                    xanchor="center", yanchor="middle",
                    yshift=6
                )
    
    c_posmap, = st.columns(1)
    with c_posmap:
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ---------- [전체표] ----------
    table = base[[
        "IP","종합_절대등급","종합_상승등급","종합등급",
        "가구시청률_종합","타깃시청률_종합","TVING LIVE_종합","TVING VOD_종합"
    ]].copy()

    table["_abs_key"]   = table["종합_절대등급"].map(ABS_SCORE).fillna(0)
    table["_slope_key"] = table["종합_상승등급"].map(SLO_SCORE).fillna(0)
    table = table.sort_values(["_abs_key","_slope_key","IP"], ascending=[False, False, True])

    table_view = table[[
        "IP","종합등급","가구시청률_종합","타깃시청률_종합","TVING LIVE_종합","TVING VOD_종합"
    ]].rename(columns={
        "종합등급":"종합",
        "가구시청률_종합":"가구시청률",
        "타깃시청률_종합":"타깃시청률",
        "TVING LIVE_종합":"TVING LIVE",
        "TVING VOD_종합":"TVING VOD"
    })

    grade_cell = JsCode("""
    function(params){
      try{
        const raw = params.value;
        if (raw === null || raw === undefined) {
          return {'text-align':'center'};
        }
        const v = String(raw);
        let bg=null, color=null, fw='700';
        if (/^[SABCD]/.test(v)) {
          if (v.startsWith('S')) { bg='rgba(0,91,187,0.14)'; color='#003d80'; }
          else if (v.startsWith('A')) { bg='rgba(0,91,187,0.08)'; color='#004a99'; }
          else if (v.startsWith('B')) { bg='rgba(0,0,0,0.03)'; color='#333'; fw='600'; }
          else if (v.startsWith('C')) { bg='rgba(42,97,204,0.08)'; color='#2a61cc'; }
          else if (v.startsWith('D')) { bg='rgba(42,97,204,0.14)'; color='#1a44a3'; }
          return {'background-color':bg,'color':color,'font-weight':fw,'text-align':'center'};
        }
        return {'text-align':'center'};
      } catch (e) {
        return {'text-align':'center'};
      }
    }""")

    gb = GridOptionsBuilder.from_dataframe(table_view.fillna("–"))
    gb.configure_default_column(resizable=True, sortable=True, filter=False,
                                headerClass='centered-header bold-header',
                                cellStyle={'textAlign':'center'})
    gb.configure_column("IP", pinned='left', cellStyle={'textAlign':'left','fontWeight':'700'})
    for colname in ["종합","가구시청률","타깃시청률","TVING LIVE","TVING VOD"]:
        gb.configure_column(colname, cellStyle=grade_cell, width=120)
    grid_options = gb.build()

    st.markdown("#### 📋 IP전체")
    AgGrid(
        table_view.fillna("–"),
        gridOptions=grid_options,
        theme="streamlit",
        height=420,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True
    )
#endregion


#region [ 11. 페이지 5: 성장스코어-디지털 ]
# =====================================================
# [수정] 2025-11-13: 회차별 등급 추이 계산 로직 최적화 (Pre-fetch + Numpy Slicing)
def render_growth_score_digital():
    """
    [페이지 7] 성장스코어-디지털 렌더링 함수
    """
    df_all = load_data().copy() # [3. 공통 함수]

    # ---------- 설정 ----------
    EP_CHOICES = [2, 4, 6, 8, 10, 12, 14, 16]
    ROW_LABELS = ["S","A","B","C","D"]
    COL_LABELS = ["+2","+1","0","-1","-2"]
    ABS_SCORE  = {"S":5,"A":4,"B":3,"C":2,"D":1}
    SLO_SCORE  = {"+2":5,"+1":4,"0":3,"-1":2,"-2":1}
    ABS_NUM    = {"S":5, "A":4, "B":3, "C":2, "D":1}
    SLOPE_LABELS = ["+2", "+1", "0", "-1", "-2"]

    METRICS = [
        ("조회수", "조회수", "sum", True),
        ("화제성", "F_Score", "mean", True),
    ]

    ips = sorted(df_all["IP"].dropna().unique().tolist())
    if not ips:
        st.warning("IP 데이터가 없습니다."); return

    # ---------- 헤더(타이틀/선택) ----------
    _ep_display = st.session_state.get("growth_d_ep_cutoff", 4)
    head = st.columns([5, 3, 2])
    with head[0]:
        st.markdown(
            f"## 🛰️ 성장스코어-디지털 <span style='font-size:20px;color:#6b7b93'>(~{_ep_display}회 기준)</span>",
            unsafe_allow_html=True
        )
    with head[1]:
        selected_ip = st.selectbox("IP 선택", ips, index=0,
                                   key="growth_d_ip_select", label_visibility="collapsed")
    with head[2]:
        ep_cutoff = st.selectbox("회차 기준", EP_CHOICES, index=1,
                                 key="growth_d_ep_cutoff", label_visibility="collapsed")

    with st.expander("ℹ️ 지표 기준 안내", expanded=False):
        st.markdown("""

**등급 체계**
- **절대값 등급**: 각 항목별(디지털조회, 화제성점수) 수치를 비교군 내 순위화→ `S / A / B / C / D`
- **상승률 등급**: 각 항목별(디지털조회, 화제성점수)의 주차별 증감정도를 비교군 내 순위화 → `+2 / +1 / 0 / -1 / -2`
- **종합등급**: 절대값과 상승률 등급을 결합해 표기 (예: A+2).  
        """)

    st.markdown(
        f"#### {selected_ip} <span style='font-size:16px;color:#6b7b93'>자세히보기</span>",
        unsafe_allow_html=True
    )

    # ---------- [최적화] 데이터 전처리 및 계산 로직 분리 ----------

    # 1. 전체 IP에 대해 회차별 숫자형 컬럼 생성 (Loop 밖에서 처리)
    if "회차_numeric" not in df_all.columns:
        df_all["회차_numeric"] = df_all["회차"].astype(str).str.extract(r"(\d+)", expand=False).astype(float)
    
    # 2. IP별 데이터프레임 딕셔너리 생성 (필터링 비용 절감)
    ip_dfs = {ip: df_all[df_all["IP"] == ip].copy() for ip in ips}
    
    # 3. [Helper] 전체 데이터를 Numpy Array로 추출하는 함수
    def _get_full_series_digital(ip_df, metric_name, mtype):
        """특정 IP, Metric의 전체 회차 데이터를 (x, y) Numpy Array로 반환"""
        if metric_name == "조회수":
            sub = _get_view_data(ip_df) # [3. 공통 함수]
        else:
            sub = ip_df[ip_df["metric"] == metric_name].copy()
            
        sub["value"] = pd.to_numeric(sub["value"], errors="coerce").replace(0, np.nan)
        sub = sub.dropna(subset=["value", "회차_numeric"])
        
        if sub.empty: return None
        
        if mtype == "sum":
            s = sub.groupby("회차_numeric", as_index=False)["value"].sum()
        elif mtype == "rank_inv": # 참고용 (현재 미사용)
            s = sub.groupby("회차_numeric", as_index=False)["value"].mean()
            s["value"] = -1 * s["value"]
        else:
            s = sub.groupby("회차_numeric", as_index=False)["value"].mean()
            
        s = s.sort_values("회차_numeric")
        return s["회차_numeric"].values.astype(float), s["value"].values.astype(float)
    
    # 4. [Pre-Calculation] 모든 IP의 Metric별 전체 (x, y) 데이터를 미리 추출
    ip_metric_cache = {}
    for ip in ips:
        ip_metric_cache[ip] = {}
        curr_df = ip_dfs[ip]
        for disp, metric_name, mtype, _ in METRICS:
            ip_metric_cache[ip][disp] = _get_full_series_digital(curr_df, metric_name, mtype)

    # 5. [Calculation] Numpy Slicing을 이용한 통계 계산
    def _calc_stats_from_cache_digital(xy_tuple, n_cutoff, use_slope):
        if xy_tuple is None: return np.nan, np.nan
        
        x, y = xy_tuple
        mask = (x >= 1) & (x <= float(n_cutoff))
        x_sub, y_sub = x[mask], y[mask]
        
        if len(x_sub) == 0: return np.nan, np.nan
        
        # Abs Value (Mean of the time series)
        abs_val = float(np.nanmean(y_sub))
        
        # Slope
        if not use_slope or len(x_sub) < 2:
            slope = np.nan
        else:
            try:
                slope = float(np.polyfit(x_sub, y_sub, 1)[0])
            except:
                slope = np.nan
                
        return abs_val, slope

    def _quintile_grade(series, labels):
        s = pd.Series(series).astype(float)
        valid = s.dropna()
        if valid.empty: return pd.Series(index=s.index, data=np.nan)
        ranks = valid.rank(method="average", ascending=False, pct=True)
        bins = [0, .2, .4, .6, .8, 1.0000001]
        idx = np.digitize(ranks.values, bins, right=True) - 1
        idx = np.clip(idx, 0, 4)
        out = pd.Series([labels[i] for i in idx], index=valid.index)
        return out.reindex(s.index)

    def _to_percentile(s):
        s = pd.Series(s).astype(float)
        return s.rank(pct=True) * 100

    # ---------- [메인 로직] 회차별 등급 산출 (Loop Optimized) ----------
    
    sel_ip_df = ip_dfs[selected_ip]
    if "회차_numeric" in sel_ip_df.columns:
        _max_ep_val = pd.to_numeric(sel_ip_df["회차_numeric"], errors="coerce").max()
    else:
        _max_ep_val = 0

    if pd.isna(_max_ep_val) or _max_ep_val == 0:
        _Ns = [min(EP_CHOICES)]
    else:
        _Ns = [n for n in EP_CHOICES if n <= _max_ep_val]
    
    needed_cutoffs = set(_Ns)
    needed_cutoffs.add(ep_cutoff)
    sorted_cutoffs = sorted(list(needed_cutoffs))

    evo_rows = []
    base_for_current_cutoff = None

    for n in sorted_cutoffs:
        tmp_rows = []
        for ip in ips:
            row = {"IP": ip}
            for disp, _, _, use_slope in METRICS:
                xy = ip_metric_cache[ip][disp]
                abs_v, slope_v = _calc_stats_from_cache_digital(xy, n, use_slope)
                row[f"{disp}_절대"] = abs_v
                row[f"{disp}_기울기"] = slope_v
            tmp_rows.append(row)
        
        tmp_df = pd.DataFrame(tmp_rows)
        
        for disp, _, _, _ in METRICS:
            tmp_df[f"{disp}_절대등급"] = _quintile_grade(tmp_df[f"{disp}_절대"], ["S","A","B","C","D"])
            tmp_df[f"{disp}_상승등급"] = _quintile_grade(tmp_df[f"{disp}_기울기"], SLOPE_LABELS)
            tmp_df[f"{disp}_종합"] = tmp_df[f"{disp}_절대등급"].astype(str) + tmp_df[f"{disp}_상승등급"].astype(str).replace("nan", "")

        tmp_df["_ABS_PCT_MEAN"] = pd.concat([_to_percentile(tmp_df[f"{d}_절대"]) for d,_,_,_ in METRICS], axis=1).mean(axis=1)
        tmp_df["_SLOPE_PCT_MEAN"] = pd.concat([_to_percentile(tmp_df[f"{d}_기울기"]) for d,_,_,_ in METRICS], axis=1).mean(axis=1)
        tmp_df["종합_절대등급"] = _quintile_grade(tmp_df["_ABS_PCT_MEAN"], ["S","A","B","C","D"])
        tmp_df["종합_상승등급"] = _quintile_grade(tmp_df["_SLOPE_PCT_MEAN"], SLOPE_LABELS)
        tmp_df["종합등급"] = tmp_df["종합_절대등급"].astype(str) + tmp_df["종합_상승등급"].astype(str).replace("nan", "")

        if n == ep_cutoff:
            base = tmp_df.copy()

        if n in _Ns:
            row = tmp_df[tmp_df["IP"] == selected_ip]
            if not row.empty and pd.notna(row.iloc[0]["종합_절대등급"]):
                ag = str(row.iloc[0]["종합_절대등급"])
                sg = str(row.iloc[0]["종합_상승등급"]) if pd.notna(row.iloc[0]["종합_상승등급"]) else ""
                evo_rows.append({
                    "N": n,
                    "ABS_GRADE": ag,
                    "SLOPE_GRADE": sg,
                    "ABS_NUM": ABS_NUM.get(ag, np.nan)
                })
                
    if 'base' not in locals(): base = tmp_df.copy()

    # ---------- [선택작품 요약카드] ----------
    focus = base[base["IP"] == selected_ip].iloc[0]

    st.markdown("<div class='growth-kpi'>", unsafe_allow_html=True) # [수정] kpi-card 래퍼
    card_cols = st.columns([2, 1, 1, 1, 1])
    with card_cols[0]:
        st.markdown(
            f"""
            <div class="kpi-card" style="height:110px;border:2px solid #004a99;background:linear-gradient(180deg,#e8f0ff, #ffffff);">
              <div class="kpi-title" style="font-size:15px;color:#003d80;">종합등급</div>
              <div class="kpi-value" style="font-size:40px;color:#003d80;">{focus['종합등급'] if pd.notna(focus['종합등급']) else '–'}</div>
            </div>
            """, unsafe_allow_html=True
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
    _grade_card(card_cols[1], "조회수 등급", focus["조회수_종합"])
    _grade_card(card_cols[2], "화제성 등급", focus["화제성_종합"])
    _grade_card(card_cols[3], " ",  " ") # 빈칸
    _grade_card(card_cols[4], " ",  " ")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ===== [회차별 등급 추이: 선택 IP] =====
    # 선택된 IP의 유효 회차 확인 (그래프 끊김 방지 등)
    _v_view = _get_view_data(df_all[df_all["IP"] == selected_ip]) # [3. 공통 함수]
    _v_view["ep"] = pd.to_numeric(
        _v_view["회차_numeric"] if "회차_numeric" in _v_view.columns
        else _v_view["회차"].astype(str).str.extract(r"(\d+)", expand=False),
        errors="coerce"
    )
    _v_view["val"] = pd.to_numeric(_v_view["value"], errors="coerce").replace(0, np.nan)
    has_ep1 = bool(_v_view.loc[_v_view["ep"] == 1, "val"].notna().any())
    has_ep2 = bool(_v_view.loc[_v_view["ep"] == 2, "val"].notna().any())

    evo = pd.DataFrame(evo_rows)
    if evo.empty:
        st.info("회차별 등급 추이를 표시할 데이터가 부족합니다.")
    else:
        fig_e = go.Figure()
        fig_e.add_vrect(x0=ep_cutoff - 0.5, x1=ep_cutoff + 0.5,
                        fillcolor="rgba(0,90,200,0.12)", line_width=0)

        fig_e.add_trace(go.Scatter(
            x=evo["N"], y=evo["ABS_NUM"],
            mode="lines+markers",
            line=dict(shape="spline", width=3),
            marker=dict(size=8),
            name=selected_ip,
            hoverinfo="skip"
        ))
        for xi, yi, ag, sg in zip(evo["N"], evo["ABS_NUM"], evo["ABS_GRADE"], evo["SLOPE_GRADE"]):
            label = f"{ag}{sg}" if isinstance(ag, str) and sg else ag
            if int(xi) == 2 and (not has_ep1 or not has_ep2):
                label = "-"
            fig_e.add_annotation(
                x=xi, y=yi, text=label,
                showarrow=False, font=dict(size=12, color="#333", family="sans-serif"),
                yshift=14
            )
        fig_e.update_xaxes(
            tickmode="array",
            tickvals=evo["N"].tolist(),
            ticktext=[f"{int(n)}회차" for n in evo["N"].tolist()],
            showgrid=False, zeroline=False, showline=False
        )
        fig_e.update_yaxes(
            tickmode="array",
            tickvals=[5,4,3,2,1],
            ticktext=["S","A","B","C","D"],
            range=[0.7, 5.3],
            showgrid=False, zeroline=False, showline=False
        )
        fig_e.update_layout(height=200, margin=dict(l=8, r=8, t=8, b=8), showlegend=False)
        
        c_evo_d, = st.columns(1)
        with c_evo_d:
            st.plotly_chart(fig_e, use_container_width=True, config={"displayModeBar": False})

    st.divider()

    # ---------- [포지셔닝맵] ----------
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

    fig = px.imshow(
        z, x=COL_LABELS, y=ROW_LABELS, origin="upper",
        color_continuous_scale="Blues", range_color=[1, 5],
        text_auto=False, aspect="auto"
    ).update_traces(xgap=0.0, ygap=0.0)

    fig.update_xaxes(showticklabels=False, title=None, ticks="")
    fig.update_yaxes(showticklabels=False, title=None, ticks="")
    fig.update_layout(height=760, margin=dict(l=2, r=2, t=2, b=2), coloraxis_showscale=False)
    fig.update_traces(hovertemplate="<extra></extra>")

    def _font_color(val: float) -> str:
        return "#FFFFFF" if val >= 3.3 else "#111111"

    for r_idx, rr in enumerate(ROW_LABELS):
        for c_idx, cc in enumerate(COL_LABELS):
            cell_val = z[r_idx][c_idx]
            names = pos_map[(rr, cc)]
            color = _font_color(cell_val)

            fig.add_annotation(
                x=cc, y=rr, xref="x", yref="y",
                text=f"<b style='letter-spacing:0.5px'>{rr}{cc}</b>",
                showarrow=False, font=dict(size=22, color=color, family="sans-serif"),
                xanchor="center", yanchor="top",
                xshift=0, yshift=80, align="left"
            )
            if names:
                fig.add_annotation(
                    x=cc, y=rr, xref="x", yref="y",
                    text=f"<span style='line-height:1.04'>{'<br>'.join(names)}</span>",
                    showarrow=False, font=dict(size=12, color=color, family="sans-serif"),
                    xanchor="center", yanchor="middle",
                    yshift=6
                )

    c_posmap_d, = st.columns(1)
    with c_posmap_d:
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ---------- [전체표] ----------
    table = base[[
        "IP","종합_절대등급","종합_상승등급","종합등급",
        "조회수_종합","화제성_종합"
    ]].copy()

    table["_abs_key"]   = table["종합_절대등급"].map(ABS_SCORE).fillna(0)
    table["_slope_key"] = table["종합_상승등급"].map(SLO_SCORE).fillna(0)
    table = table.sort_values(["_abs_key","_slope_key","IP"], ascending=[False, False, True])

    table_view = table[[
        "IP","종합등급","조회수_종합","화제성_종합"
    ]].rename(columns={
        "종합등급":"종합",
        "조회수_종합":"조회수",
        "화제성_종합":"화제성",
    })

    grade_cell = JsCode("""
    function(params){
      try{
        const raw = params.value;
        if (raw === null || raw === undefined) {
          return {'text-align':'center'};
        }
        const v = String(raw);
        let bg=null, color=null, fw='700';
        if (/^[SABCD]/.test(v)) {
          if (v.startsWith('S')) { bg='rgba(0,91,187,0.14)'; color='#003d80'; }
          else if (v.startsWith('A')) { bg='rgba(0,91,187,0.08)'; color='#004a99'; }
          else if (v.startsWith('B')) { bg='rgba(0,0,0,0.03)'; color='#333'; fw='600'; }
          else if (v.startsWith('C')) { bg='rgba(42,97,204,0.08)'; color:'#2a61cc'; }
          else if (v.startsWith('D')) { bg='rgba(42,97,204,0.14)'; color:'#1a44a3'; }
          return {'background-color':bg,'color':color,'font-weight':fw,'text-align':'center'};
        }
        return {'text-align':'center'};
      } catch (e) {
        return {'text-align':'center'};
      }
    }""")

    gb = GridOptionsBuilder.from_dataframe(table_view.fillna("–"))
    gb.configure_default_column(resizable=True, sortable=True, filter=False,
                                headerClass='centered-header bold-header',
                                cellStyle={'textAlign':'center'})
    gb.configure_column("IP", pinned='left', cellStyle={'textAlign':'left','fontWeight':'700'})
    for colname in ["종합","조회수","화제성"]:
        gb.configure_column(colname, cellStyle=grade_cell, width=120)
    grid_options = gb.build()

    st.markdown("#### 📋 IP전체-디지털")
    AgGrid(
        table_view.fillna("–"),
        gridOptions=grid_options,
        theme="streamlit",
        height=420,
        fit_columns_on_grid_load=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True
    )
#endregion


#region [ 12. 메인 라우터 ]
# =====================================================
# [수정] 삭제된 페이지(데모그래픽, 회차별) 라우팅 제거
if st.session_state["page"] == "Overview":
    render_overview() # [ 7. 페이지 1 ]
elif st.session_state["page"] == "IP 성과":
    render_ip_detail() # [ 8. 페이지 2 ]
elif st.session_state["page"] == "비교분석":
    render_comparison() # [ 10. 페이지 4 (통합됨) ]
elif st.session_state["page"] == "성장스코어-방영지표":
    render_growth_score() # [ 12. 페이지 6 ]
elif st.session_state["page"] == "성장스코어-디지털":
    render_growth_score_digital() # [ 13. 페이지 7 ]
else:
    render_overview() # 기본값으로 Overview 렌더링
    
#endregion
