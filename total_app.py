import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import re
import os
import io
import plotly.express as px


CACHE_FILE = "weather_data_storage.csv"
AUTH_KEY = "HEXnlNDyTVKF55TQ8i1SNg"
STN_DICT = {'전주': '146', '군산': '140', '부안': '243', '임실': '244',
            '정읍': '245', '남원': '247', '장수': '248', '순창': '254', '고창': '251'}

st.set_page_config(
    page_title="전북특별자치도 농업기술원 기상시스템",
    page_icon="logo.png", # 파일명을 직접 써주세요!
    layout="wide",
    initial_sidebar_state="expanded"
)

# [데이터 보정] 시간별 데이터를 가져와서 일별 데이터를 정확히 보정하는 함수
def get_hourly_filling_mean(target_date, stn_id):
    url = f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php?tm1={target_date}0000&tm2={target_date}2300&stn={stn_id}&authKey={AUTH_KEY}&help=0"
    try:
        res = requests.get(url, timeout=5)
        # 데이터 라인 추출 (숫자 12자리로 시작하는 라인)
        lines = [l.strip() for l in res.text.split('\n') if re.match(r'^\d{12}', l.strip())]
        if not lines: return None
        df_h = pd.DataFrame([l.split() for l in lines])
        print(f"DEBUG [{target_date} {stn_id}]: 데이터 {len(df_h)}줄 발견, 11번 항목들 -> {df_h[11].tolist()}")

        # 수정된 인덱스: 11(기온), 13(습도), 15(강수), 32(일조), 3(풍속)
        # 연구원님 말씀대로 강수량은 [15]번 인덱스입니다!
        for idx in [11, 13, 15, 33, 3]:
            df_h[idx] = pd.to_numeric(df_h[idx], errors='coerce')
            # 결측치(-9.0 이하)는 계산에서 제외하도록 처리
            df_h.loc[df_h[idx] <= -9.0, idx] = None

        # 항목별 계산 로직 적용
        res_data = {
            '평균기온': df_h[11].mean(skipna=True),
            '최고기온': df_h[11].max(),
            '최저기온': df_h[11].min(),
            '평균습도': df_h[13].mean(skipna=True),
            '강수량': df_h[15].sum(skipna=True),  # [15]번 인덱스 합계
            '일조시간': df_h[33].sum(skipna=True),  # [33]번 인덱스 합계
            '최대풍속': df_h[3].max()
        }

        # 최종 보정: 소수점 1자리 제한 및 장비 완전 결측 시 방어
        for key, value in res_data.items():
            if pd.isna(value):
                # 강수와 일조는 데이터 없으면 0.0, 나머지는 None(빈칸) 유지
                res_data[key] = 0.0 if key in ['강수량', '일조시간'] else None
            else:
                # 모든 수치는 소수점 첫째 자리까지 반올림
                res_data[key] = round(float(value), 1)

        return res_data
    except:
        return None


@st.cache_data(ttl=3600)
def get_weather_data(tm1, tm2):
    local_df = pd.read_csv(CACHE_FILE) if os.path.exists(CACHE_FILE) else pd.DataFrame()
    if not local_df.empty: local_df['날짜'] = pd.to_datetime(local_df['날짜'])

    all_data = []
    target_dates = pd.date_range(start=tm1, end=tm2).strftime('%Y%m%d').tolist()

    for name, stn_id in STN_DICT.items():
        existing_dates = local_df[local_df['지역'] == name]['날짜'].dt.strftime(
            '%Y%m%d').tolist() if not local_df.empty else []
        missing_dates = [d for d in target_dates if d not in existing_dates]

        if not missing_dates: continue

        url = f"https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?tm1={min(missing_dates)}&tm2={max(missing_dates)}&stn={stn_id}&authKey={AUTH_KEY}&help=0"
        try:
            time.sleep(0.3)
            res = requests.get(url, timeout=10)
            data_lines = [l.strip() for l in res.text.split('\n') if re.match(r'^\d{8}', l.strip())]

            for line in data_lines:
                # 1. strip()으로 양끝 공백 제거 후, split()으로 깔끔하게 쪼개기
                val = line.strip().split()

                # 데이터 개수가 부족하면 무시 (인덱스 에러 방지)
                if len(val) < 40: continue

                # 수치 변환 함수
                def to_f(v):
                    try:
                        f_v = float(v)
                        # 결측치(-9, -99, -999 등) 처리
                        return None if f_v <= -9.0 else f_v
                    except:
                        return None

                # [정확한 일자료 인덱스] 0날짜, 10평균, 11최고, 13최저, 18습도, 32일조, 38강수, 5최대풍속
                row = {
                    '날짜': pd.to_datetime(val[0], format='%Y%m%d'),
                    '평균기온': to_f(val[10]),
                    '최고기온': to_f(val[11]),
                    '최저기온': to_f(val[13]),
                    '평균습도': to_f(val[18]),
                    '일조시간': to_f(val[32]) if to_f(val[32]) is not None else 0.0,
                    '강수량': to_f(val[38]) if to_f(val[38]) is not None else 0.0,
                    '최대풍속': to_f(val[5]),
                    '지역': name
                }
                # 감시할 항목 리스트 (기온, 습도, 일조, 강수, 풍속 중 하나라도 비면 보정!)
                check_cols = ['평균기온', '최고기온', '최저기온', '평균습도', '일조시간', '강수량', '최대풍속']

                # 하나라도 None이거나, 강수/일조가 0인 경우(보통 결측시 0으로 올 수 있음) 보정 실행
                if any(row[col] is None for col in check_cols):
                    filling = get_hourly_filling_mean(val[0], stn_id)
                    if filling:
                        row.update(filling)

                # 모든 수치 데이터를 소수점 1자리로 통일 (라운딩 처리)
                cols_to_round = ['평균기온', '최고기온', '최저기온', '평균습도', '일조시간', '강수량', '최대풍속']
                for col in cols_to_round:
                    if row[col] is not None:
                        row[col] = round(float(row[col]), 1)

                all_data.append(pd.DataFrame([row]))
        except:
            continue

    if all_data:
        new_df = pd.concat(all_data, ignore_index=True)
        combined_df = pd.concat([local_df, new_df]).drop_duplicates(['날짜', '지역']).sort_values(['날짜', '지역'])
        combined_df.to_csv(CACHE_FILE, index=False, encoding='utf-8-sig')
        return combined_df
    return local_df

# UI 부분

col1, col2 = st.columns([0.3, 0.7]) # 로고가 가로로 기니까 공간을 좀 더 줬습니다.
with col1:
    st.image("logo.png") # 쌀 아이콘 대신 진짜 로고가 들어갑니다!
with col2:
    st.markdown("<h1 style='margin-top: 10px;'>기상 분석 시스템</h1>", unsafe_allow_html=True)

# [시스템 UI 설정] 영문 툴바 제거 및 요일 한글화
st.markdown("""
    <style>
    [data-testid="stDataFrameToolbar"], .modebar { display: none !important; }
    div[data-baseweb="calendar"] [role="columnheader"] { color: transparent !important; position: relative !important; }
    div[data-baseweb="calendar"] [role="columnheader"]::after {
        position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
        visibility: visible !important; font-size: 0.8rem; font-weight: bold;
    }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(1)::after { content: "일"; color: red !important; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(2)::after { content: "월"; color: white; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(3)::after { content: "화"; color: white; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(4)::after { content: "수"; color: white; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(5)::after { content: "목"; color: white; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(6)::after { content: "금"; color: white; }
    div[data-baseweb="calendar"] [role="columnheader"]:nth-child(7)::after { content: "토"; color: #00bfff !important; }
    div[data-baseweb="select"] [data-testid="stMarkdownContainer"] p { display: none; }
    </style>
    """, unsafe_allow_html=True)

st.sidebar.header("⚙️ 시스템 관리")
pwd = st.sidebar.text_input("관리자 인증", type="password", placeholder="비밀번호를 입력하세요")
if pwd == "6226":
    if st.sidebar.button("🔄 서버 데이터 갱신"):
        st.cache_data.clear()
        if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.header("🗓️ 조회 기간")
start_date_raw = st.sidebar.date_input("조회 시작 날짜", value=datetime(2026, 3, 1), format="YYYY/MM/DD")
end_date_raw = st.sidebar.date_input("조회 종료 날짜", value=datetime.now() - timedelta(days=1), format="YYYY/MM/DD")

start_date = pd.to_datetime(start_date_raw)
end_date = pd.to_datetime(end_date_raw)

if start_date <= end_date:
    df = get_weather_data(start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))

    if df is not None and not df.empty:
        df['날짜'] = pd.to_datetime(df['날짜'])
        df = df[(df['날짜'] >= start_date) & (df['날짜'] <= end_date)]

        if not df.empty:

            # [KeyError 방지] 존재 여부 확인 후 안전하게 컬럼 선택
            # 연구원님이 요청하신 순서: 평균, 최고, 최저, 습도, 일조, 강수, 풍속
            available_cols = ['날짜', '지역', '평균기온', '최고기온', '최저기온', '평균습도', '일조시간', '강수량', '최대풍속']
            # 만약 없는 컬럼이 있다면 제외하고 선택 (보안책)
            final_cols = [c for c in available_cols if c in df.columns]

            final_df = df[final_cols].rename(columns={
                '날짜': '관측날짜', '지역': '지역명',
                '평균기온': '평균기온(℃)', '최고기온': '최고기온(℃)', '최저기온': '최저기온(℃)',
                '평균습도': '평균습도(%)', '일조시간': '일조시간합(hr)', '강수량': '강수량(mm)',
                '최대풍속': '최대풍속(m/s)'
            })

            st.markdown("### 📊 조회 대상 지역")
            sel = st.multiselect("조회할 지역을 선택하세요", list(STN_DICT.keys()), default=['전주', '군산'])

            if sel:
                v_df = final_df[final_df['지역명'].isin(sel)]
                st.subheader(f"📍 {start_date.date()} ~ {end_date.date()} 기상 분석 보고")

                # 그래프
                st.markdown("### 📈 항목별 그래프")
                tab1, tab2, tab3, tab4, tab5 = st.tabs(["평균기온", "최고기온", "최저기온", "일조시간", "강수량"])

                with tab1:
                    st.line_chart(v_df, x='관측날짜', y='평균기온(℃)', color='지역명')
                with tab2:
                    st.line_chart(v_df, x='관측날짜', y='최고기온(℃)', color='지역명')
                with tab3:
                    st.line_chart(v_df, x='관측날짜', y='최저기온(℃)', color='지역명')
                with tab4:
                    # 일조시간 막대 그래프 (지역별로 옆으로 나란히)
                    fig4 = px.bar(v_df, x='관측날짜', y='일조시간합(hr)', color='지역명',
                                  barmode='group',  # 이 설정이 막대기를 옆으로 나란히 세워줍니다!
                                  title='지역별 일조시간 비교')
                    st.plotly_chart(fig4, use_container_width=True)

                with tab5:
                    # 강수량 막대 그래프 (지역별로 옆으로 나란히)
                    fig5 = px.bar(v_df, x='관측날짜', y='강수량(mm)', color='지역명',
                                  barmode='group',
                                  title='지역별 강수량 비교')
                    st.plotly_chart(fig5, use_container_width=True)

                # 상세 표 출력
                st.dataframe(
                    v_df.sort_values(['관측날짜', '지역명'], ascending=[False, True]),
                    width='stretch',
                    hide_index=True
                )


                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    v_df.to_excel(writer, index=False, sheet_name='기상데이터')

                st.download_button(
                    label="📥 데이터 저장 (Excel)",
                    data=buffer.getvalue(),
                    file_name=f"전북기상분석_{start_date.strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("지역을 선택해 주세요.")
else:
    st.error("종료일이 시작일보다 빠를 수 없습니다.")




st.caption("ⓒ 2026 전북특별자치도 농업기술원 | 자료출처: 기상청 오픈 API")

