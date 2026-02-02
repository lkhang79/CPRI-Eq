import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, date

# ==========================================
# 1. 설정 및 초기화
# ==========================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource(ttl=300)  # 5분간 캐싱
def get_client():
    """Google Sheets 클라이언트 초기화 - Streamlit Secrets 사용"""
    try:
        # ✅ Streamlit secrets에서 인증 정보 가져오기
        credentials_dict = dict(st.secrets["gcp_service_account"])
        
        creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=SCOPES
        )
        client = gspread.authorize(creds)
        return client
        
    except KeyError as e:
        st.error(f"⚠️ Streamlit Secrets 설정이 필요합니다!")
        st.info("""
        **설정 방법:**
        1. Streamlit Cloud 대시보드에서 앱 선택
        2. Settings → Secrets 클릭
        3. 아래 형식으로 인증 정보 입력:
        
        ```toml
        [gcp_service_account]
        type = "service_account"
        project_id = "your-project-id"
        private_key_id = "your-private-key-id"
        private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
        client_email = "your-service-account@...iam.gserviceaccount.com"
        client_id = "your-client-id"
        auth_uri = "https://accounts.google.com/o/oauth2/auth"
        token_uri = "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
        client_x509_cert_url = "your-cert-url"
        ```
        """)
        return None
        
    except Exception as e:
        st.error(f"⚠️ 인증 실패: {e}")
        st.info("Settings → Secrets에서 인증 정보를 확인하세요.")
        return None

# ==========================================
# 2. 데이터 로딩 (장비목록 & 사용자관리 & 기업목록)
# ==========================================

def get_master_data(_client):
    """마스터 데이터 로딩 - 매번 새로 조회"""
    try:
        doc = _client.open("장비관리시스템")
        
        # 모든 시트 이름 확인 (디버깅용)
        all_sheets = [ws.title for ws in doc.worksheets()]
        
        # [1] 기업 목록 가져오기
        try:
            sheet_company = None
            possible_names = ['기업목록', '기업 목록', '기업리스트', 'company']
            
            for name in possible_names:
                try:
                    sheet_company = doc.worksheet(name)
                    st.sidebar.success(f"✅ 기업목록 시트 찾음: '{name}'")
                    break
                except:
                    continue
            
            if sheet_company:
                # 중복 헤더 문제 해결: 직접 값을 읽어서 처리
                all_values = sheet_company.get_all_values()
                
                if len(all_values) > 1:
                    # 실제 데이터가 시작되는 행 찾기 (보통 안내문 이후)
                    data_start_row = 0
                    for i, row in enumerate(all_values):
                        # '기업명' 헤더가 있는 행 찾기
                        if any('기업명' in str(cell) for cell in row):
                            data_start_row = i
                            break
                    
                    if data_start_row > 0:
                        headers = all_values[data_start_row]
                        data_rows = all_values[data_start_row + 1:]
                    else:
                        headers = all_values[0]
                        data_rows = all_values[1:]
                    
                    # 빈 헤더는 '미지정1', '미지정2' 등으로 변경
                    cleaned_headers = []
                    empty_count = 0
                    for h in headers:
                        if not h or str(h).strip() == '':
                            empty_count += 1
                            cleaned_headers.append(f'미지정{empty_count}')
                        else:
                            cleaned_headers.append(str(h).strip())
                    
                    # DataFrame 생성
                    df_company = pd.DataFrame(data_rows, columns=cleaned_headers)
                    
                    # 기업명 -> (기업규모, 사업자번호) 매핑
                    company_map = {}
                    company_list = []
                    company_biznum = {}
                    
                    # 컬럼명 찾기 (유연하게)
                    name_col = None
                    size_col = None
                    biznum_col = None
                    
                    for col in df_company.columns:
                        col_lower = str(col).lower().replace(' ', '')
                        if '기업명' in col_lower or '회사명' in col_lower:
                            name_col = col
                        elif '기업규모' in col_lower or '구분' in col_lower:
                            size_col = col
                        elif '사업자' in col_lower or '등록번호' in col_lower:
                            biznum_col = col
                    
                    if name_col:
                        for _, row in df_company.iterrows():
                            company_name = str(row.get(name_col, '')).strip()
                            company_size = str(row.get(size_col, '기타')).strip() if size_col else '기타'
                            biz_num = str(row.get(biznum_col, '')).strip() if biznum_col else ''
                            
                            # 빈 행이나 안내문 제외
                            if company_name and company_name != '' and not company_name.startswith('※'):
                                company_map[company_name] = company_size
                                company_list.append(company_name)
                                if biz_num:
                                    company_biznum[company_name] = biz_num
                        
                        st.sidebar.info(f"📊 기업 {len(company_list)}개 로드 완료")
                    else:
                        st.sidebar.warning("⚠️ '기업명' 컬럼을 찾을 수 없습니다.")
                else:
                    company_map = {}
                    company_list = []
                    company_biznum = {}
            else:
                st.sidebar.warning(f"⚠️ 기업목록 시트를 찾을 수 없습니다.")
                company_map = {}
                company_list = []
                company_biznum = {}
                
        except Exception as e:
            st.sidebar.error(f"⚠️ 기업목록 로딩 오류: {e}")
            company_map = {}
            company_list = []
            company_biznum = {}
        
        # [2] 장비 목록 가져오기
        try:
            sheet_equip = None
            possible_names = ['장비목록', '장비 목록', '장비리스트', 'equipment']
            
            for name in possible_names:
                try:
                    sheet_equip = doc.worksheet(name)
                    st.sidebar.success(f"✅ 장비목록 시트 찾음: '{name}'")
                    break
                except:
                    continue
            
            if not sheet_equip:
                st.sidebar.error(f"⚠️ 장비목록 시트를 찾을 수 없습니다.")
                dept_map = {}
                info_map = {}
            else:
                equip_records = sheet_equip.get_all_records()
                
                dept_map = {}
                info_map = {}
                
                for row in equip_records:
                    dept = row.get('부서명')
                    eq_name = row.get('장비명')
                    eq_no = row.get('장비번호')
                    eq_type = row.get('장비구분')
                    
                    if not dept or not eq_name: continue

                    if dept not in dept_map:
                        dept_map[dept] = []
                    dept_map[dept].append(eq_name)
                    
                    info_map[eq_name] = {"no": eq_no, "type": eq_type}
                    
        except Exception as e:
            st.sidebar.error(f"⚠️ 장비목록 로딩 오류: {e}")
            dept_map = {}
            info_map = {}
            
        # [3] 사용자 목록 가져오기
        sheet_user = doc.worksheet("사용자관리")
        user_records = sheet_user.get_all_records()
        user_db = {str(row['아이디']): row for row in user_records if row.get('아이디')}
        
        return dept_map, info_map, user_db, company_map, company_list, company_biznum
        
    except Exception as e:
        st.error(f"⚠️ 데이터 로딩 실패! 시트 이름이나 제목 행을 확인하세요.\n에러: {e}")
        return {}, {}, {}, {}, [], {}

def load_log_data(_sheet):
    """장비일지 불러오기 (동적 컬럼 처리) - 매번 새로 조회"""
    rows = _sheet.get_all_values()
    
    if len(rows) == 0:
        # 기본 컬럼 구조
        cols = [
            "사용목적", "활용유형", "사용기관 기업명", "사용기관 사업자등록번호", "내부부서명",
            "업종", "품목", "세부품목", "제품명", "시료수/시험수",
            "세부지원공개여부", "세부지원내용", "장비명", "장비번호", "장비구분",
            "사용시작일", "사용종료일", "휴무일자포함", "사용시간", "사용료", 
            "사용목적기타", "기타(공정구분)"
        ]
        return pd.DataFrame(columns=cols)
    
    # 첫 번째 행을 헤더로 사용
    header = rows[0]
    data_rows = rows[1:]
    
    if len(data_rows) == 0:
        return pd.DataFrame(columns=header)
    
    # 데이터프레임 생성 (실제 컬럼 수에 맞춤)
    df = pd.DataFrame(data_rows, columns=header)
    
    return df

# ==========================================
# 3. 로그인 페이지
# ==========================================

def login_page():
    st.set_page_config(page_title="로그인", layout="centered")
    st.title("🔒 장비관리시스템 로그인")
    
    # 로그인 폼
    with st.form("login_form"):
        st.subheader("로그인 정보 입력")
        username = st.text_input("아이디", placeholder="아이디를 입력하세요")
        password = st.text_input("비밀번호", type="password", placeholder="비밀번호를 입력하세요")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            submit = st.form_submit_button("🔐 로그인", use_container_width=True)
        
        if submit:
            if not username or not password:
                st.error("❌ 아이디와 비밀번호를 모두 입력해주세요.")
            else:
                client = get_client()
                if not client:
                    st.error("❌ 시스템 연결 실패. Streamlit Secrets 설정을 확인하세요.")
                    return
                    
                _, _, user_db, _, _, _ = get_master_data(client)
                
                if username in user_db:
                    sheet_pw = str(user_db[username]["비밀번호"]).strip()
                    input_pw = str(password).strip()
                    
                    if sheet_pw == input_pw:
                        st.session_state["logged_in"] = True
                        st.session_state["username"] = user_db[username]["이름"]
                        st.session_state["user_dept"] = user_db[username]["부서"]
                        st.session_state["user_id"] = username  # 아이디 저장 추가
                        st.success("✅ 로그인 성공! 잠시 후 이동합니다.")
                        st.rerun()
                    else:
                        st.error("❌ 비밀번호가 일치하지 않습니다.")
                else:
                    st.error("❌ 등록되지 않은 아이디입니다.")
    
    st.markdown("---")
    
    # 진단 모드 - 마스터 계정만 표시
    MASTER_ACCOUNTS = ['master', 'admin', 'superuser']  # 마스터 계정 목록
    
    if 'user_id' in st.session_state and st.session_state.get('user_id') in MASTER_ACCOUNTS:
        if st.checkbox("🔧 연결 진단 모드 (관리자 전용)", value=False):
            st.info("👇 아래 버튼을 누르면 구글 시트 상태를 확인합니다.")
            
            if st.button("구글 시트 연결 테스트"):
                try:
                    client = get_client()
                    if not client:
                        st.error("❌ 1단계 실패: Streamlit Secrets 설정이 없거나 인증에 실패했습니다.")
                        return
                    
                    doc = client.open("장비관리시스템")
                    st.success("✅ 1단계 성공: '장비관리시스템' 파일을 찾았습니다.")
                    
                    try:
                        sheet = doc.worksheet("사용자관리")
                        st.success("✅ 2단계 성공: '사용자관리' 탭을 찾았습니다.")
                    except:
                        st.error("❌ 2단계 실패: '사용자관리' 탭이 없습니다. 띄어쓰기가 있는지 확인하세요.")
                        return

                    records = sheet.get_all_records()
                    
                    if not records:
                        st.warning("⚠️ 데이터가 비어있습니다. 2번째 줄부터 아이디를 입력했나요?")
                    else:
                        keys = list(records[0].keys())
                        st.write(f"🔑 **인식된 제목열:** {keys}")
                        st.write(f"📊 **총 사용자 수:** {len(records)}명")
                        
                        required = ['아이디', '비밀번호', '이름', '부서']
                        missing = [k for k in required if k not in keys]
                        
                        if missing:
                            st.error(f"❌ 3단계 실패: 제목 줄에 {missing} 항목이 없습니다. 오타를 확인하세요!")
                        else:
                            st.success("✅ 3단계 성공: 데이터 구조가 완벽합니다!")

                except Exception as e:
                    st.error(f"⚠️ 에러 발생: {e}")

# ==========================================
# 4. 메인 앱 (나머지 코드는 동일)
# ==========================================
def main_app():
    st.set_page_config(page_title="장비가동일지", layout="wide")
    
    client = get_client()
    if not client: return
    
    # 기초 데이터 로딩 (기업 리스트 및 사업자번호 포함)
    dept_equip_map, equip_info_db, _, company_map, company_list, company_biznum = get_master_data(client)
    
    try:
        doc = client.open("장비관리시스템")
    except Exception as e:
        st.error(f"'장비관리시스템' 파일을 찾을 수 없습니다: {e}")
        return

    my_name = st.session_state["username"]
    my_dept = st.session_state["user_dept"]
    
    # [사이드바]
    st.sidebar.title(f"👤 {my_name}님")
    st.sidebar.caption(f"소속: {my_dept if my_dept != 'ALL' else '통합관리자'}")
    
    if st.sidebar.button("로그아웃"):
        st.session_state["logged_in"] = False
        st.rerun()
    
    st.sidebar.caption("💡 데이터는 항상 최신 상태로 자동 조회됩니다")
    
    # 시트 정보 확인 (접기 가능)
    with st.sidebar.expander("🔧 시트 정보 확인"):
        try:
            all_sheets = [(i, ws.title) for i, ws in enumerate(doc.worksheets())]
            st.write("**사용 가능한 시트 목록:**")
            for idx, name in all_sheets:
                st.write(f"{idx}: {name}")
        except Exception as e:
            st.error(f"시트 정보를 가져올 수 없습니다: {e}")
    
    st.sidebar.markdown("---")
    
    # 부서 선택
    st.sidebar.header("1. 장비 선택")
    
    if my_dept == "ALL":
        dept_list = list(dept_equip_map.keys())
    else:
        if my_dept in dept_equip_map:
            dept_list = [my_dept]
        else:
            st.error(f"오류: '{my_dept}'팀의 등록된 장비가 없습니다. 엑셀을 확인하세요.")
            dept_list = []
            
    sel_dept = st.sidebar.selectbox("부서", dept_list)
    equip_list = dept_equip_map.get(sel_dept, [])
    sel_equip = st.sidebar.selectbox("장비", equip_list)
    
    # 장비 정보 가져오기
    curr_info = equip_info_db.get(sel_equip, {"no": "", "type": ""})
    
    # ★ 선택된 장비명으로 해당 시트 찾기
    log_sheet = None
    if sel_equip:
        try:
            # 정확한 장비명으로 시트 찾기
            log_sheet = doc.worksheet(sel_equip)
            st.sidebar.success(f"✅ '{sel_equip}' 시트 연결됨")
        except Exception as e:
            st.sidebar.error(f"⚠️ '{sel_equip}' 시트를 찾을 수 없습니다.")
            st.sidebar.info("시트 이름이 장비명과 정확히 일치하는지 확인하세요.")
            
            # 사용 가능한 시트 목록 표시
            available_sheets = [ws.title for ws in doc.worksheets()]
            with st.sidebar.expander("📋 사용 가능한 시트 목록"):
                for sheet_name in available_sheets:
                    st.write(f"- {sheet_name}")

    # 메인 화면
    if sel_equip:
        st.title(f"📝 {sel_equip} 가동일지")
        
        if not log_sheet:
            st.error(f"⚠️ '{sel_equip}' 시트를 찾을 수 없습니다.")
            st.info("왼쪽 사이드바에서 '사용 가능한 시트 목록'을 확인하세요.")
            st.stop()
    else:
        st.title("👈 왼쪽에서 장비를 선택해주세요.")
        st.stop()

    # ✅ 탭 개수 수정: tab4 제거 (4개 -> 4개)
    tab1, tab2, tab3, tab4 = st.tabs(["📝 입력하기", "📊 조회하기(활용율)", "📈 장비서비스분석", "📋 장비정보"])

    # [탭1] 입력 + 엑셀 업로드
    with tab1:
        # Form 밖에서 기업명 먼저 선택 (사업자번호 자동완성을 위해)
        st.subheader("🏢 기업 정보 선택")
        
        col_company1, col_company2 = st.columns(2)
        
        with col_company1:
            if company_list:
                selected_company = st.selectbox(
                    "사용기관 기업명",
                    ["직접입력"] + sorted(company_list),
                    key="company_selector"
                )
                
                if selected_company == "직접입력":
                    manual_company = st.text_input("기업명 직접 입력", key="manual_company_input")
                    final_company = manual_company
                    final_biznum = ""
                else:
                    final_company = selected_company
                    final_biznum = company_biznum.get(selected_company, "")
            else:
                final_company = st.text_input("사용기관 기업명")
                final_biznum = ""
        
        with col_company2:
            if final_biznum:
                st.text_input("사업자등록번호 (자동)", value=final_biznum, disabled=True, key="auto_biznum_display")
                st.caption("✅ 자동으로 입력됨")
            else:
                st.info("← 왼쪽에서 기업을 선택하면 자동 입력됩니다")
        
        st.markdown("---")
        
        # 실제 입력 폼
        with st.form("main_form"):
            st.subheader("📝 상세 정보 입력")
            
            c1, c2 = st.columns(2)
            with c1:
                f01_purpose = st.selectbox("1. 사용목적", ["시험", "분석", "계측", "생산", "교육", "기타"])
                f03_biz_name = st.text_input("3. 사용기관 기업명", value=final_company)
                f05_dept = st.text_input("5. 내부부서명", value=sel_dept)
                f06_industry = st.selectbox("6. 업종", ["기계", "전기전자", "화학", "바이오", "기타", "해당없음"])
                
                # 품목 드롭다운
                industry_items = {
                    "기계": ["금형", "공구", "부품", "소재", "기타"],
                    "전기전자": ["반도체", "디스플레이", "배터리", "센서", "PCB", "기타"],
                    "화학": ["촉매", "고분자", "나노소재", "코팅", "첨가제", "기타"],
                    "바이오": ["의료기기", "진단", "바이오소재", "제약", "기타"],
                    "기타": ["기타"],
                    "해당없음": ["해당없음"]
                }
                
                selected_items = industry_items.get(f06_industry, ["기타"])
                f07_item = st.selectbox("7. 품목", selected_items)
                
                # 세부품목 드롭다운
                item_sub_items = {
                    "금형": ["사출금형", "프레스금형", "다이캐스팅", "기타"],
                    "반도체": ["웨이퍼", "패키징", "테스트", "공정장비", "기타"],
                    "디스플레이": ["OLED", "LCD", "LED", "QD", "기타"],
                    "배터리": ["2차전지", "전극소재", "전해질", "분리막", "기타"],
                    "센서": ["온도센서", "압력센서", "광센서", "가스센서", "기타"],
                    "나노소재": ["탄소나노", "금속나노", "세라믹나노", "복합소재", "기타"],
                    "코팅": ["박막", "표면처리", "기능성코팅", "방수코팅", "기타"],
                    "의료기기": ["진단기기", "치료기기", "수술기기", "기타"]
                }
                
                if f07_item in item_sub_items:
                    f08_sub_item = st.selectbox("8. 세부품목", item_sub_items[f07_item])
                else:
                    f08_sub_item = st.text_input("8. 세부품목")
                f09_prod_name = st.text_input("9. 제품명")
                f11_public = st.radio("11. 세부지원공개여부", ["Y", "N"], horizontal=True)
                f13_eq_name = st.text_input("13. 장비명", value=sel_equip, disabled=True)
                f14_eq_no = st.text_input("14. 장비번호", value=curr_info['no'])
            with c2:
                f02_type = st.selectbox("2. 활용유형", ["내부", "내부타부서", "외부", "간접지원"])
                f04_biz_num = st.text_input("4. 사업자등록번호", value=final_biznum)
                st.write("")
                f10_sample_cnt = st.number_input("10. 시료수/시험수", min_value=0, step=1)
                st.write("")
                st.write("")
                st.write("")
                f12_content = st.text_area("12. 세부지원내용", height=100)
                f15_eq_type = st.text_input("15. 장비구분", value=curr_info['type'])
            
            c3, c4 = st.columns(2)
            with c3:
                f16_start = st.date_input("16. 사용시작일", value=date.today())
                f17_end = st.date_input("17. 사용종료일", value=date.today())
                f18_holiday = st.checkbox("18. 휴무일자포함")
            with c4:
                f19_hours = st.number_input("19. 사용시간", min_value=0.0, step=0.5)
                f20_fee = st.number_input("20. 사용료", min_value=0, step=1000)
            
            c5, c6 = st.columns(2)
            with c5:
                f21_etc = st.text_input("21. 사용목적기타")
            with c6:
                f22_process = st.selectbox("22. 기타(공정구분)", ["단위공정", "모듈공정", "측정분석"])

            st.markdown("---")
            if st.form_submit_button("💾 일지 저장하기", use_container_width=True, type="primary"):
                val_holiday = "Y" if f18_holiday else "N"
                
                # 데이터 준비 - 타입별로 처리
                row_data = [
                    str(f01_purpose).strip() if f01_purpose else "",  # 텍스트
                    str(f02_type).strip() if f02_type else "",  # 텍스트
                    str(f03_biz_name).strip() if f03_biz_name else "",  # 텍스트
                    str(f04_biz_num).strip() if f04_biz_num else "",  # 텍스트
                    str(f05_dept).strip() if f05_dept else "",  # 텍스트
                    str(f06_industry).strip() if f06_industry else "",  # 텍스트
                    str(f07_item).strip() if f07_item else "",  # 텍스트
                    str(f08_sub_item).strip() if f08_sub_item else "",  # 텍스트
                    str(f09_prod_name).strip() if f09_prod_name else "",  # 텍스트
                    int(f10_sample_cnt) if f10_sample_cnt else 0,  # 숫자
                    str(f11_public).strip() if f11_public else "",  # 텍스트
                    str(f12_content).strip() if f12_content else "",  # 텍스트
                    str(sel_equip).strip() if sel_equip else "",  # 텍스트
                    str(f14_eq_no).strip() if f14_eq_no else "",  # 텍스트
                    str(f15_eq_type).strip() if f15_eq_type else "",  # 텍스트
                    str(f16_start) if f16_start else "",  # 날짜
                    str(f17_end) if f17_end else "",  # 날짜
                    str(val_holiday),  # 텍스트
                    float(f19_hours) if f19_hours else 0.0,  # 숫자 (소수)
                    int(f20_fee) if f20_fee else 0,  # 숫자 (정수)
                    str(f21_etc).strip() if f21_etc else "",  # 텍스트
                    str(f22_process).strip() if f22_process else ""  # 텍스트
                ]
                
                try:
                    # 구글 시트에 추가 (타입이 자동으로 유지됨)
                    log_sheet.append_row(row_data, value_input_option='USER_ENTERED')
                    st.success(f"✅ 저장 완료!")
                    st.balloons()
                except Exception as e:
                    st.error(f"저장 실패: {e}")
        
        # ===== ✅ 엑셀 업로드 기능 추가 =====
        st.markdown("---")
        st.markdown("---")
        st.subheader("📤 i-Tube 엑셀 파일 일괄 업로드")
        st.info("💡 i-Tube 템플릿(4번째 행이 헤더)을 업로드하세요")
        
        upload_dept = st.selectbox("업로드 부서 선택", sorted(dept_equip_map.keys()), key="upload_dept_new")
        upload_equip = st.selectbox("업로드 장비 선택", dept_equip_map.get(upload_dept, []), key="upload_equip_new")
        
        if upload_equip:
            uploaded_file = st.file_uploader("i-Tube 템플릿 파일 선택", type=['xlsx', 'xls'])
            
            if uploaded_file:
                try:
                    # ✅ 1. i-Tube 템플릿 구조 반영 (4행이 헤더)
                    df_up = pd.read_excel(uploaded_file, header=3)  # 4번째 행을 헤더로
                    
                    # 빈 행 제거
                    df_up = df_up.dropna(how='all')
                    
                    st.success(f"✅ {len(df_up)}건의 데이터를 읽었습니다.")
                    
                    # ✅ 2. 필수 컬럼 검증
                    required_columns = [
                        "사용목적", "활용유형", "사용기관 기업명", "사용기관 사업자등록번호",
                        "내부부서명", "업종", "품목", "세부품목", "제품명", "시료수/시험수",
                        "세부지원공개여부", "세부지원내용", "장비명", "장비번호", "장비구분",
                        "사용시작일", "사용종료일", "휴무일자포함", "사용시간", "사용료",
                        "사용목적기타", "기타(공정구분)"
                    ]
                    
                    missing_cols = [col for col in required_columns if col not in df_up.columns]
                    
                    if missing_cols:
                        st.error(f"❌ 필수 컬럼 누락: {', '.join(missing_cols)}")
                        st.info("💡 i-Tube 템플릿의 4번째 행에 컬럼 헤더가 있는지 확인하세요")
                        
                        with st.expander("🔍 현재 읽은 컬럼"):
                            st.write(list(df_up.columns))
                    else:
                        st.success("✅ 컬럼 구조 확인 완료")
                        
                        # ✅ 3. 데이터 미리보기
                        with st.expander("📋 데이터 미리보기 (처음 10행)", expanded=True):
                            st.dataframe(df_up.head(10), use_container_width=True)
                        
                        # ✅ 4. 날짜 형식 변환
                        st.info("📅 날짜 데이터 변환 중...")
                        for date_col in ['사용시작일', '사용종료일']:
                            if date_col in df_up.columns:
                                df_up[date_col] = pd.to_datetime(df_up[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
                        
                        # ✅ 5. 업로드 버튼
                        if st.button("🚀 구글 시트로 일괄 저장", type="primary", use_container_width=True):
                            with st.spinner("데이터 업로드 중..."):
                                # NaN 처리
                                df_up = df_up.fillna('')
                                
                                # 데이터만 추출 (컬럼 순서 유지)
                                upload_values = []
                                for _, row in df_up.iterrows():
                                    row_data = []
                                    for col in required_columns:
                                        value = row.get(col, '')
                                        
                                        # 숫자 컬럼 타입 변환
                                        if col == '시료수/시험수':
                                            try:
                                                row_data.append(int(float(value)) if value != '' else 0)
                                            except:
                                                row_data.append(0)
                                        elif col == '사용시간':
                                            try:
                                                row_data.append(float(value) if value != '' else 0.0)
                                            except:
                                                row_data.append(0.0)
                                        elif col == '사용료':
                                            try:
                                                row_data.append(int(float(value)) if value != '' else 0)
                                            except:
                                                row_data.append(0)
                                        else:
                                            row_data.append(str(value).strip() if value != '' else '')
                                    
                                    upload_values.append(row_data)
                                
                                # 구글 시트 업로드
                                target_sheet = doc.worksheet(upload_equip)
                                
                                # 진행률 표시
                                progress_bar = st.progress(0)
                                status_text = st.empty()
                                batch_size = 50  # 50건씩 배치 업로드
                                
                                for i in range(0, len(upload_values), batch_size):
                                    batch = upload_values[i:i+batch_size]
                                    target_sheet.append_rows(batch, value_input_option='USER_ENTERED')
                                    
                                    progress = min((i + batch_size) / len(upload_values), 1.0)
                                    progress_bar.progress(progress)
                                    status_text.text(f"업로드 중... {min(i+batch_size, len(upload_values))}/{len(upload_values)} 건")
                                
                                progress_bar.empty()
                                status_text.empty()
                                
                                # 서식 최적화
                                try:
                                    st.info("✨ 구글 시트 최적화 중...")
                                    
                                    # 헤더 스타일
                                    target_sheet.format('1:1', {
                                        "backgroundColor": {"red": 0.2, "green": 0.5, "blue": 0.8},
                                        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                                        "horizontalAlignment": "CENTER",
                                        "verticalAlignment": "MIDDLE"
                                    })
                                    
                                    # 헤더 고정
                                    target_sheet.freeze(rows=1)
                                    
                                    # 숫자 포맷
                                    last_row = len(upload_values) + 1
                                    target_sheet.format(f'J2:J{last_row}', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
                                    target_sheet.format(f'S2:S{last_row}', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.0"}})
                                    target_sheet.format(f'T2:T{last_row}', {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
                                    
                                    # 필터 추가
                                    target_sheet.set_basic_filter()
                                    
                                    st.success("✨ 최적화 완료")
                                except Exception as opt_error:
                                    st.warning(f"⚠️ 최적화 중 일부 오류 (데이터는 정상 업로드됨)")
                                
                                st.success(f"✅ {len(upload_values)}건 업로드 완료!")
                                st.balloons()
                                
                except Exception as e:
                    st.error(f"❌ 오류 발생: {e}")
                    import traceback
                    with st.expander("📋 상세 에러 로그"):
                        st.code(traceback.format_exc())

    # [탭2] 조회하기 (활용율) - ✅ 업로드 창 제거, 다운로드 기능 복구
    with tab2:
        st.subheader("📊 장비 활용 현황 및 활용율")
        
        col_refresh, col_period = st.columns([1, 3])
        with col_refresh:
            if st.button("🔄 새로고침", use_container_width=True):
                st.rerun()
        
        # ✅ 기간 선택 추가
        with col_period:
            date_range = st.date_input(
                "조회 기간 선택",
                value=(date.today().replace(day=1), date.today()),
                help="시작일과 종료일을 선택하세요"
            )
        
        df = load_log_data(log_sheet)
        
        if not df.empty and "장비명" in df.columns:
            # 현재 장비만 필터링
            filtered = df[df["장비명"] == sel_equip].copy()
            
            # ✅ 기간 필터링 추가
            if len(date_range) == 2 and '사용시작일' in filtered.columns:
                filtered['사용시작일_dt'] = pd.to_datetime(filtered['사용시작일'], errors='coerce')
                start_date, end_date = date_range
                mask = (filtered['사용시작일_dt'] >= pd.Timestamp(start_date)) & \
                       (filtered['사용시작일_dt'] <= pd.Timestamp(end_date))
                filtered = filtered[mask]
            
            if not filtered.empty:
                st.markdown("### 📌 활용율 계산")
                
                # 활용율 계산을 위한 기준 시간 입력
                col_calc1, col_calc2, col_calc3 = st.columns(3)
                
                with col_calc1:
                    target_hours = st.number_input(
                        "목표 가동시간 (시간/월)", 
                        min_value=100, 
                        max_value=5000, 
                        value=1000, 
                        step=100,
                        help="이 장비의 월별 목표 가동시간을 입력하세요"
                    )
                
                # 사용시간 숫자 변환
                if '사용시간' in filtered.columns:
                    filtered['사용시간_num'] = pd.to_numeric(filtered['사용시간'], errors='coerce').fillna(0)
                    total_hours = filtered['사용시간_num'].sum()
                    total_count = len(filtered)
                    avg_hours = total_hours / total_count if total_count > 0 else 0
                    
                    # 활용율 계산
                    utilization_rate = (total_hours / target_hours * 100) if target_hours > 0 else 0
                    
                    with col_calc2:
                        st.metric(
                            "실제 가동시간", 
                            f"{total_hours:,.1f} 시간",
                            delta=f"{total_hours - target_hours:+,.1f}h"
                        )
                    
                    with col_calc3:
                        st.metric(
                            "활용율", 
                            f"{utilization_rate:.1f}%",
                            delta="목표 대비"
                        )
                    
                    # 활용율 계산식 표시
                    st.info(f"""
                    **📐 활용율 계산식:**
                    ```
                    활용율 = (실제 가동시간 ÷ 목표 가동시간) × 100
                           = ({total_hours:,.1f}h ÷ {target_hours:,}h) × 100
                           = {utilization_rate:.1f}%
                    ```
                    """)
                    
                    # 진행바로 시각화
                    progress_value = min(utilization_rate / 100, 1.0)
                    st.progress(progress_value)
                    
                    if utilization_rate >= 100:
                        st.success("✅ 목표 달성!")
                    elif utilization_rate >= 80:
                        st.warning("⚠️ 목표에 근접했습니다.")
                    else:
                        st.error("❌ 추가 활용이 필요합니다.")
                
                st.markdown("---")
                st.markdown("### 📊 요약 통계")
                
                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("총 사용시간", f"{total_hours:,.1f} 시간")
                with col_s2:
                    st.metric("총 사용건수", f"{total_count:,} 건")
                with col_s3:
                    st.metric("평균 사용시간", f"{avg_hours:,.1f} 시간/건")
                
                st.markdown("---")
                st.markdown("### 📋 상세 기록")
                
                # 표시할 컬럼 선택
                display_columns = []
                for col in ['사용시작일', '사용종료일', '활용유형', '사용기관 기업명', 
                           '사용시간', '사용료', '기타(공정구분)']:
                    if col in filtered.columns:
                        display_columns.append(col)
                
                if display_columns:
                    # 날짜 기준 내림차순 정렬
                    if '사용시작일' in filtered.columns:
                        filtered_sorted = filtered[display_columns].sort_values('사용시작일', ascending=False)
                    else:
                        filtered_sorted = filtered[display_columns]
                    
                    st.dataframe(filtered_sorted, use_container_width=True, height=400)
                    
                # ✅ 다운로드 버튼 섹션 수정
                st.markdown("---")
                st.subheader("📥 데이터 내보내기")
                st.caption(f"'{sel_equip}' 장비의 구글 시트 전체 데이터를 다운로드합니다.")
                
                # 1. 다운로드용 전체 데이터 준비 (날짜 정렬만 수행)
                df_full_download = df.copy()
                if '사용시작일' in df_full_download.columns:
                    df_full_download = df_full_download.sort_values('사용시작일', ascending=False)

                col_down1, col_down2 = st.columns(2)
                with col_down1:
                    # CSV 다운로드 (전체 내용)
                    csv_data = df_full_download.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📄 전체 기록 CSV 다운로드",
                        data=csv_data,
                        file_name=f"{sel_equip}_전체기록_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col_down2:
                    # Excel 다운로드 (전체 내용)
                    import io
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df_full_download.to_excel(writer, index=False, sheet_name='전체사용기록')
                    
                    st.download_button(
                        label="📊 전체 기록 Excel 다운로드",
                        data=excel_buffer.getvalue(),
                        file_name=f"{sel_equip}_전체기록_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            else:
                st.info(f"'{sel_equip}' 장비의 선택한 기간에 사용 기록이 없습니다.")
        else:
            st.info("데이터가 없습니다.")

    # [탭3] 장비 서비스 분석
    with tab3:
        st.subheader("📊 장비별 서비스 분석")
        st.info("여러 장비를 선택하여 기간별 사용 현황을 비교 분석합니다.")
        
        # 기간 선택
        st.markdown("### 📅 분석 기간 설정")
        date_range = st.date_input(
            "분석 기간 선택",
            value=(date.today().replace(day=1), date.today()),
            help="시작일과 종료일을 선택하세요"
        )
        
        st.markdown("---")
        
        # 장비 선택 (부서별로 구분)
        st.markdown("### 🔧 분석할 장비 선택 (체크)")
        
        # session_state 초기화
        if 'selected_equipments' not in st.session_state:
            st.session_state.selected_equipments = []
        
        # 부서별로 장비 표시
        equipment_counter = 0  # 전역 카운터 추가
        for dept_name, dept_equipments in sorted(dept_equip_map.items()):
            with st.expander(f"📁 **{dept_name}** ({len(dept_equipments)}개 장비)", expanded=False):
                # 장비 체크박스 (3열)
                num_cols = 3
                cols = st.columns(num_cols)
                
                for idx, equipment in enumerate(sorted(dept_equipments)):
                    col_idx = idx % num_cols
                    with cols[col_idx]:
                        is_checked = equipment in st.session_state.selected_equipments
                        
                        # 체크박스 상태 변경 처리 - 고유 key 생성
                        checked = st.checkbox(
                            equipment, 
                            value=is_checked, 
                            key=f"eq_{equipment_counter}_{dept_name}_{idx}"
                        )
                        
                        # 선택 상태 업데이트
                        if checked and equipment not in st.session_state.selected_equipments:
                            st.session_state.selected_equipments.append(equipment)
                        elif not checked and equipment in st.session_state.selected_equipments:
                            st.session_state.selected_equipments.remove(equipment)
                        
                        equipment_counter += 1  # 카운터 증가
        
        st.markdown("---")
        
        selected_equipments = st.session_state.selected_equipments
        
        if not selected_equipments:
            st.warning("⚠️ 분석할 장비를 1개 이상 선택해주세요.")
        else:
            st.success(f"✅ {len(selected_equipments)}개 장비 선택됨: {', '.join(selected_equipments[:3])}{'...' if len(selected_equipments) > 3 else ''}")
            
            if st.button("🔍 분석 실행", type="primary", use_container_width=True):
                # 선택된 각 장비의 데이터 수집
                all_data = []
                
                for equipment in selected_equipments:
                    try:
                        # 해당 장비 시트 찾기
                        eq_sheet = doc.worksheet(equipment)
                        df_eq = load_log_data(eq_sheet)
                        
                        if not df_eq.empty:
                            all_data.append(df_eq)
                    except:
                        st.warning(f"⚠️ '{equipment}' 시트를 찾을 수 없습니다.")
                        continue
                
                if not all_data:
                    st.error("❌ 선택한 장비들의 데이터를 찾을 수 없습니다.")
                else:
                    # 모든 데이터 합치기
                    df_combined = pd.concat(all_data, ignore_index=True)
                    
                    # 필수 컬럼 확인
                    required_cols = ['장비명', '사용시작일', '사용시간', '사용기관 기업명']
                    missing_cols = [col for col in required_cols if col not in df_combined.columns]
                    
                    if missing_cols:
                        st.error(f"❌ 필수 컬럼이 없습니다: {missing_cols}")
                    else:
                        # 날짜 변환 및 필터링
                        df_combined['사용시작일_dt'] = pd.to_datetime(df_combined['사용시작일'], errors='coerce')
                        df_combined['사용시간_num'] = pd.to_numeric(df_combined['사용시간'], errors='coerce').fillna(0)
                        
                        # 기간 필터
                        if len(date_range) == 2:
                            start_date, end_date = date_range
                            mask = (df_combined['사용시작일_dt'] >= pd.Timestamp(start_date)) & \
                                   (df_combined['사용시작일_dt'] <= pd.Timestamp(end_date))
                            df_filtered = df_combined[mask]
                        else:
                            df_filtered = df_combined
                        
                        # 선택된 장비만 필터링
                        df_filtered = df_filtered[df_filtered['장비명'].isin(selected_equipments)]
                        
                        if df_filtered.empty:
                            st.warning(f"⚠️ 선택한 기간에 사용 기록이 없습니다.")
                        else:
                            # 기업규모 매핑
                            df_filtered['기업규모'] = df_filtered['사용기관 기업명'].apply(
                                lambda x: company_map.get(str(x).strip(), '기타')
                            )
                            
                            # 사용료 숫자 변환
                            if '사용료' in df_filtered.columns:
                                df_filtered['사용료_num'] = pd.to_numeric(df_filtered['사용료'], errors='coerce').fillna(0)
                            
                            st.success(f"✅ 총 {len(df_filtered)}건의 사용 기록을 찾았습니다.")
                            
                            # === 장비별 요약 ===
                            st.markdown("### 📌 장비별 사용 현황")
                            
                            if '사용료_num' in df_filtered.columns:
                                equip_stats = df_filtered.groupby('장비명').agg({
                                    '사용시간_num': ['sum', 'count'],
                                    '사용료_num': 'sum'
                                }).reset_index()
                                equip_stats.columns = ['장비명', '총 사용시간', '사용건수', '총 사용료']
                            else:
                                equip_stats = df_filtered.groupby('장비명').agg({
                                    '사용시간_num': ['sum', 'count']
                                }).reset_index()
                                equip_stats.columns = ['장비명', '총 사용시간', '사용건수']
                            
                            equip_stats = equip_stats.sort_values('총 사용시간', ascending=False)
                            
                            if '총 사용료' in equip_stats.columns:
                                st.dataframe(
                                    equip_stats.style.format({
                                        '총 사용시간': '{:,.1f}',
                                        '사용건수': '{:,.0f}',
                                        '총 사용료': '{:,.0f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                            else:
                                st.dataframe(
                                    equip_stats.style.format({
                                        '총 사용시간': '{:,.1f}',
                                        '사용건수': '{:,.0f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                            
                            st.markdown("---")
                            
                            # === 분석 1: 기업규모별 집계 ===
                            st.markdown("### 📌 기업규모별 사용 현황")
                            
                            if '사용료_num' in df_filtered.columns:
                                company_stats = df_filtered.groupby('기업규모').agg({
                                    '사용시간_num': ['sum', 'count'],
                                    '사용료_num': 'sum'
                                }).reset_index()
                                company_stats.columns = ['기업규모', '총 사용시간', '사용건수', '총 사용료']
                            else:
                                company_stats = df_filtered.groupby('기업규모').agg({
                                    '사용시간_num': ['sum', 'count']
                                }).reset_index()
                                company_stats.columns = ['기업규모', '총 사용시간', '사용건수']
                            
                            # 원하는 순서로 정렬
                            target_companies = ['대기업', '중소기업', '학교', '연구원', '기타']
                            company_stats['기업규모'] = pd.Categorical(
                                company_stats['기업규모'], 
                                categories=target_companies, 
                                ordered=True
                            )
                            company_stats = company_stats.sort_values('기업규모').reset_index(drop=True)
                            
                            if '총 사용료' in company_stats.columns:
                                st.dataframe(
                                    company_stats.style.format({
                                        '총 사용시간': '{:,.1f}',
                                        '사용건수': '{:,.0f}',
                                        '총 사용료': '{:,.0f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                            else:
                                st.dataframe(
                                    company_stats.style.format({
                                        '총 사용시간': '{:,.1f}',
                                        '사용건수': '{:,.0f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                            
                            st.markdown("---")
                            
                            # === 분석 2: 공정구분별 집계 ===
                            st.markdown("### 📌 공정구분별 사용 현황")
                            
                            # 공정구분 컬럼이 있는지 확인
                            process_col = None
                            for col in ['기타', '공정구분', 'V']:
                                if col in df_filtered.columns:
                                    process_col = col
                                    break
                            
                            # 디버깅: 컬럼 내용 확인
                            if process_col:
                                # 빈 값 제거 후 데이터 확인
                                df_filtered[process_col] = df_filtered[process_col].astype(str).str.strip()
                                valid_data = df_filtered[
                                    (df_filtered[process_col].notna()) & 
                                    (df_filtered[process_col] != '') & 
                                    (df_filtered[process_col] != 'nan')
                                ]
                                
                                if len(valid_data) > 0:
                                    # 집계 수행
                                    if '사용료_num' in df_filtered.columns:
                                        process_stats = valid_data.groupby(process_col).agg({
                                            '사용시간_num': ['sum', 'count'],
                                            '사용료_num': 'sum'
                                        }).reset_index()
                                        process_stats.columns = ['기타', '총 사용시간', '사용건수', '총 사용료']
                                    else:
                                        process_stats = valid_data.groupby(process_col).agg({
                                            '사용시간_num': ['sum', 'count']
                                        }).reset_index()
                                        process_stats.columns = ['기타', '총 사용시간', '사용건수']
                                    
                                    # 원하는 순서로 정렬
                                    target_processes = ['단위공정', '모듈공정', '측정분석']
                                    process_stats['기타'] = pd.Categorical(
                                        process_stats['기타'], 
                                        categories=target_processes, 
                                        ordered=True
                                    )
                                    process_stats = process_stats.sort_values('기타').reset_index(drop=True)
                                    
                                    if '총 사용료' in process_stats.columns:
                                        st.dataframe(
                                            process_stats.style.format({
                                                '총 사용시간': '{:,.1f}',
                                                '사용건수': '{:,.0f}',
                                                '총 사용료': '{:,.0f}'
                                            }),
                                            use_container_width=True,
                                            hide_index=True
                                        )
                                    else:
                                        st.dataframe(
                                            process_stats.style.format({
                                                '총 사용시간': '{:,.1f}',
                                                '사용건수': '{:,.0f}'
                                            }),
                                            use_container_width=True,
                                            hide_index=True
                                        )
                                else:
                                    st.warning(f"⚠️ '{process_col}' 컬럼에 유효한 데이터가 없습니다.")
                            else:
                                st.warning("⚠️ 공정구분 컬럼을 찾을 수 없습니다.")
                            
                            st.markdown("---")
                            
                            # === 상세 데이터 테이블 ===
                            with st.expander("📋 상세 데이터 보기"):
                                display_cols = []
                                for col in ['장비명', '사용시작일', '활용유형', '사용기관 기업명', '기업규모', 
                                           process_col, '사용시간', '사용료']:
                                    if col and col in df_filtered.columns:
                                        display_cols.append(col)
                                
                                if display_cols:
                                    st.dataframe(
                                        df_filtered[display_cols].sort_values('사용시작일', ascending=False),
                                        use_container_width=True
                                    )

    # [탭4] 장비정보
    with tab4:
        st.subheader("📋 전체 장비 정보")
        st.info("모든 장비의 상세 정보를 조회하고 다운로드할 수 있습니다.")
        
        try:
            # 장비정보 시트 읽기
            sheet_info = None
            possible_names = ['장비정보', '장비 정보', 'Equipment Info']
            
            for name in possible_names:
                try:
                    sheet_info = doc.worksheet(name)
                    st.success(f"✅ '{name}' 시트를 찾았습니다.")
                    break
                except:
                    continue
            
            if not sheet_info:
                st.error("❌ '장비정보' 시트를 찾을 수 없습니다.")
                st.info("💡 엑셀에 '장비정보' 시트가 있는지 확인해주세요.")
                
                # 사용 가능한 시트 목록 표시
                available_sheets = [ws.title for ws in doc.worksheets()]
                with st.expander("📋 사용 가능한 시트 목록"):
                    for sheet_name in available_sheets:
                        st.write(f"- {sheet_name}")
            else:
                # 장비정보 데이터 로드
                info_data = sheet_info.get_all_values()
                
                if len(info_data) > 1:
                    headers = info_data[0]
                    data_rows = info_data[1:]
                    
                    df_info = pd.DataFrame(data_rows, columns=headers)
                    
                    # 빈 행 제거 (첫 번째 컬럼 기준)
                    if len(headers) > 0:
                        df_info = df_info[df_info.iloc[:, 0].notna() & (df_info.iloc[:, 0] != '')]
                    
                    st.markdown(f"### 📊 총 {len(df_info)}개 장비 정보")
                    
                    # 필터링 옵션
                    col_filter1, col_filter2 = st.columns([1, 3])
                    
                    with col_filter1:
                        # 부서명이나 구분 컬럼으로 필터링
                        filter_col = None
                        for col_name in ['부서명', '구분', '분류', 'Category']:
                            if col_name in df_info.columns:
                                filter_col = col_name
                                break
                        
                        if filter_col:
                            unique_values = ['전체'] + sorted(df_info[filter_col].unique().tolist())
                            selected_filter = st.selectbox(f"{filter_col} 필터", unique_values)
                            
                            if selected_filter != '전체':
                                df_display = df_info[df_info[filter_col] == selected_filter]
                            else:
                                df_display = df_info
                        else:
                            df_display = df_info
                            st.info("필터 컬럼 없음")
                    
                    with col_filter2:
                        # 장비명으로 검색
                        search_keyword = st.text_input("🔍 검색", placeholder="장비명, 모델명 등으로 검색")
                        if search_keyword:
                            # 모든 컬럼에서 검색
                            mask = df_display.apply(lambda row: row.astype(str).str.contains(search_keyword, case=False, na=False).any(), axis=1)
                            df_display = df_display[mask]
                    
                    # 데이터 표시
                    st.markdown(f"**표시 중: {len(df_display)}개 장비 정보**")
                    st.dataframe(df_display, use_container_width=True, height=500)
                    
                    st.markdown("---")
                    
                    # 다운로드 옵션
                    st.markdown("### 📥 데이터 다운로드")
                    
                    col_down1, col_down2, col_down3 = st.columns(3)
                    
                    with col_down1:
                        # CSV 다운로드
                        csv_data = df_display.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📄 CSV 다운로드",
                            data=csv_data,
                            file_name=f"장비정보_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col_down2:
                        # Excel 다운로드
                        import io
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            df_display.to_excel(writer, index=False, sheet_name='장비정보')
                        
                        st.download_button(
                            label="📊 Excel 다운로드",
                            data=excel_buffer.getvalue(),
                            file_name=f"장비정보_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    
                    with col_down3:
                        # 통계 정보
                        st.metric("조회된 장비", f"{len(df_display)}개")
                    
                    # 추가 통계 (필터 컬럼이 있는 경우)
                    if filter_col and filter_col in df_info.columns:
                        st.markdown("---")
                        st.markdown(f"### 📊 {filter_col}별 장비 현황")
                        
                        stats = df_info.groupby(filter_col).size().reset_index(name='장비 수')
                        stats = stats.sort_values('장비 수', ascending=False)
                        
                        col_stat1, col_stat2 = st.columns([2, 1])
                        
                        with col_stat1:
                            st.dataframe(stats, use_container_width=True, hide_index=True)
                        
                        with col_stat2:
                            st.write(f"**{filter_col}별 비율**")
                            for _, row in stats.iterrows():
                                pct = (row['장비 수'] / len(df_info)) * 100
                                st.write(f"{row[filter_col]}: {pct:.1f}%")
                
                else:
                    st.warning("⚠️ 장비정보 데이터가 없습니다.")
                    
        except Exception as e:
            st.error(f"❌ 장비정보 로딩 실패: {e}")
            import traceback
            st.code(traceback.format_exc())

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if st.session_state["logged_in"]:
    main_app()
else:
    login_page()

# 푸터
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #95a5a6; font-size: 1.1rem; padding: 30px;'>
    <b>철원 플라즈마 산업기술 연구원 장비 관리 플랫폼<br>
    <b>(CPRI Equipment Management Platform_v1.0)</b><br>
     @ 2026 New Business Strategy Department K.H Lee. All Rights Reserved.
</div>
""", unsafe_allow_html=True)
