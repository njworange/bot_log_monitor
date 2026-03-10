from .setup import *
import threading
import time
import re
import json
import subprocess
import requests
import select
import ast
from datetime import datetime, timezone, timedelta


class ModuleMonitor(PluginModuleBase):

    def __init__(self, P):
        super(ModuleMonitor, self).__init__(P, name='monitor', first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version': '1',
            'monitor_log_file_path': '/data/log/gds_tool.log',
            'monitor_auto_start': 'True',
            'monitor_scan_past_lines': '100',
            'monitor_tmdb_api_key': '',  # TMDB API 키
            'monitor_tmdb_use_meta': 'True',  # TMDB 메타데이터 사용 여부
            'monitor_discord_webhook_url': '',  # 디스코드 웹훅 URL
            'monitor_discord_use_notify': 'False',  # 디스코드 알림 사용 여부
        }
        self.monitor_thread = None
        self.monitor_running = False
        
        # 영화 폴더 중복 방지용 캐시 {tmdb_id: {'path': gds_path, 'time': timestamp}}
        self.movie_cache = {}
        self.movie_cache_ttl = 60  # 60초 동안만 캐시 유지


    def plugin_load(self):
        """플러그인 로드 시 자동 시작"""
        if P.ModelSetting.get_bool('monitor_auto_start'):
            self.start_monitor()


    def plugin_unload(self):
        """플러그인 언로드 시 중지"""
        self.stop_monitor()


    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        
        if command == 'start':
            if self.monitor_running:
                ret['ret'] = 'warning'
                ret['msg'] = '이미 실행 중입니다.'
            else:
                self.start_monitor()
                ret['msg'] = '모니터링을 시작했습니다.'
        
        elif command == 'stop':
            if not self.monitor_running:
                ret['ret'] = 'warning'
                ret['msg'] = '실행 중이 아닙니다.'
            else:
                self.stop_monitor()
                ret['msg'] = '모니터링을 중지했습니다.'
        
        elif command == 'status':
            ret['running'] = self.monitor_running
            ret['log_file'] = P.ModelSetting.get('monitor_log_file_path')
        
        return jsonify(ret)


    def start_monitor(self):
        """모니터링 시작"""
        if self.monitor_running:
            return
        
        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor_thread.start()
        P.logger.info('로그 모니터링 시작')


    def stop_monitor(self):
        """모니터링 중지"""
        self.monitor_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        P.logger.info('로그 모니터링 중지')


    @staticmethod
    def is_vod_file(file_path: str) -> bool:
        """
        VOD 파일인지 확인 (영상 + 자막)
        영상: .mkv, .mp4, .avi, .ts
        자막: .srt, .ass, .sub
        """
        video_exts = ['.mkv', '.mp4', '.avi', '.ts']
        subtitle_exts = ['.srt', '.ass', '.sub']
        
        for ext in video_exts + subtitle_exts:
            if file_path.endswith(ext):
                return True
        return False


    @staticmethod
    def normalize_title(s: str) -> str:
        """
        제목 정규화: 공백, 특수문자 제거
        예: "포레스트 검프" → "포레스트검프"
        예: "포레스트-검프" → "포레스트검프"
        """
        if not s:
            return ""
        # 한글, 영문, 숫자만 남기고 모두 제거
        return re.sub(r'[^가-힣A-Za-z0-9]', '', s)


    @staticmethod
    def convert_country_to_korean(country_name: str) -> str:
        """영어 국가명을 한글로 변환"""
        country_map = {
            'United States': '미국',
            'United States of America': '미국',
            'South Korea': '한국',
            'Korea': '한국',
            'Japan': '일본',
            'China': '중국',
            'Hong Kong': '홍콩',
            'Taiwan': '대만',
            'United Kingdom': '영국',
            'France': '프랑스',
            'Germany': '독일',
            'Italy': '이탈리아',
            'Spain': '스페인',
            'Canada': '캐나다',
            'Australia': '호주',
            'India': '인도',
            'Russia': '러시아',
            'Thailand': '태국',
            'Vietnam': '베트남',
            'Singapore': '싱가포르',
            'Malaysia': '말레이시아',
            'Indonesia': '인도네시아',
            'Philippines': '필리핀',
            'Mexico': '멕시코',
            'Brazil': '브라질',
            'Argentina': '아르헨티나',
            'Netherlands': '네덜란드',
            'Belgium': '벨기에',
            'Sweden': '스웨덴',
            'Norway': '노르웨이',
            'Denmark': '덴마크',
            'Finland': '핀란드',
            'Poland': '폴란드',
            'Czech Republic': '체코',
            'Austria': '오스트리아',
            'Switzerland': '스위스',
            'Turkey': '터키',
            'Greece': '그리스',
            'Portugal': '포르투갈',
            'Ireland': '아일랜드',
            'New Zealand': '뉴질랜드',
            'South Africa': '남아프리카공화국',
            'Egypt': '이집트',
            'Israel': '이스라엘',
            'Saudi Arabia': '사우디아라비아',
            'United Arab Emirates': '아랍에미리트',
        }
        return country_map.get(country_name, country_name)


    def run_rclone_with_retry(self, cmd, max_retries=2, timeout=15):
        """rclone 명령어를 재시도 로직과 함께 실행"""
        for attempt in range(max_retries):
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                if result.returncode == 0:
                    return result
                P.logger.debug(f'rclone 실패 (시도 {attempt+1}/{max_retries}): {result.stderr}')
            except subprocess.TimeoutExpired:
                P.logger.warning(f'rclone 타임아웃 (시도 {attempt+1}/{max_retries}, {timeout}초)')
                if attempt == max_retries - 1:
                    raise
            time.sleep(1)  # 재시도 전 1초 대기
        return None


    def get_gdrive_id_from_path(self, gds_path, is_folder=False):
        """rclone으로 경로에서 Google Drive ID 조회"""
        try:
            # GDS 경로를 rclone 경로로 변환
            # /ROOT/GDRIVE/VIDEO/... → gds2:GDRIVE/VIDEO/...
            rclone_path = gds_path.replace('/ROOT/', 'gds2:')
            
            if is_folder:
                # 폴더: 부모 디렉토리를 조회해서 이름으로 매칭
                folder_name = gds_path.split('/')[-1]
                parent_path = '/'.join(gds_path.split('/')[:-1]).replace('/ROOT/', 'gds2:')
                
                cmd = ['rclone', '--config', '/mnt/jaewoo/rclone/rclone.conf', 'lsjson', parent_path, '--dirs-only']
                
                result = self.run_rclone_with_retry(cmd)
                
                if not result or result.returncode != 0:
                    P.logger.debug(f'rclone 실패 (폴더)')
                    return None
                
                # 폴더 목록에서 이름 매칭
                items = json.loads(result.stdout)
                folder_id = None
                for item in items:
                    if item.get('Name') == folder_name or item.get('Path') == folder_name:
                        if 'ID' in item:
                            folder_id = item['ID']
                            break
                
                if not folder_id:
                    P.logger.debug(f'폴더 ID 찾기 실패: {folder_name}')
                    return None
                
                return folder_id
            
            else:
                # 파일: --stat 사용
                cmd = ['rclone', '--config', '/mnt/jaewoo/rclone/rclone.conf', 'lsjson', rclone_path, '--stat']
                
                result = self.run_rclone_with_retry(cmd)
                
                if not result or result.returncode != 0:
                    P.logger.debug(f'rclone 실패 (파일)')
                    return None
                
                # JSON 파싱
                data = json.loads(result.stdout)
                
                # ID 추출
                if 'ID' in data:
                    return data['ID']
                else:
                    P.logger.debug(f'파일 ID 없음: {data}')
                    return None
        
        except subprocess.TimeoutExpired:
            P.logger.error(f'rclone 최종 타임아웃 (재시도 모두 실패): {gds_path}')
            return None
        except Exception as e:
            P.logger.error(f'ID 조회 에러: {e}')
            return None


    def find_all_movie_folders(self, parent_gds_path, parent_folder_name):
        """영화 폴더 내에서 실제 파일이 있는 모든 하위 폴더 찾기 (여러 버전 지원)"""
        try:
            # rclone으로 하위 폴더 목록 조회
            rclone_path = parent_gds_path.replace('/ROOT/', 'gds2:')
            cmd = ['rclone', '--config', '/mnt/jaewoo/rclone/rclone.conf', 'lsjson', rclone_path, '--dirs-only']
            
            result = self.run_rclone_with_retry(cmd)
            
            if not result or result.returncode != 0:
                P.logger.debug(f'하위 폴더 조회 실패')
                return []
            
            subfolders = json.loads(result.stdout)
            
            if not subfolders:
                # 하위 폴더 없음
                P.logger.debug(f'하위 폴더 없음: {parent_folder_name}')
                return []
            
            # 부모 폴더에서 제목 추출 (연도 제거)
            # 예: "포레스트 검프 (1994)" → "포레스트 검프"
            # 예: "포레스트 검프 (1994) {tmdb-123}" → "포레스트 검프"
            title_match = re.match(r'(.+?)\s*\((\d{4})\)', parent_folder_name)
            parent_title = title_match.group(1).strip() if title_match else parent_folder_name
            parent_year = title_match.group(2) if title_match else ''
            
            # 정규화된 제목 (공백, 특수문자 제거)
            parent_title_norm = self.normalize_title(parent_title)
            
            # 부모 폴더의 TMDB ID 추출 (있을 수도, 없을 수도)
            tmdb_match = re.search(r'\{tmdb-(\d+)\}', parent_folder_name)
            parent_tmdb_id = tmdb_match.group(1) if tmdb_match else None
            
            # 하위 폴더 필터링: 같은 제목/연도를 가진 것들
            candidates = []
            for folder in subfolders:
                folder_name = folder.get('Name', '')
                
                # 하위 폴더 제목 추출 및 정규화
                folder_title_match = re.match(r'(.+?)\s*\((\d{4})\)', folder_name)
                folder_title = folder_title_match.group(1).strip() if folder_title_match else folder_name.split('{')[0].strip()
                if '[' in folder_title:
                    folder_title = folder_title.split('[')[0].strip()
                folder_title_norm = self.normalize_title(folder_title)
                
                # 1) 정규화된 제목으로 비교 + 연도 확인
                if parent_title_norm == folder_title_norm and (not parent_year or parent_year in folder_name):
                    # 2) TMDB ID가 있다면 같은 TMDB ID인지 확인
                    if parent_tmdb_id:
                        if f'{{tmdb-{parent_tmdb_id}}}' in folder_name:
                            candidates.append({
                                'id': folder.get('ID'),
                                'name': folder_name
                            })
                    else:
                        # TMDB ID 없으면 제목/연도 일치만으로 판단
                        candidates.append({
                            'id': folder.get('ID'),
                            'name': folder_name
                        })
            
            if candidates:
                P.logger.info(f'하위 폴더 {len(candidates)}개 발견: {parent_folder_name}')
                for c in candidates:
                    P.logger.debug(f'  - {c["name"]}')
            else:
                P.logger.debug(f'매칭되는 하위 폴더 없음: {parent_folder_name}')
            
            return candidates
        
        except subprocess.TimeoutExpired:
            P.logger.error(f'하위 폴더 조회 타임아웃 (재시도 실패): {parent_gds_path}')
            return []
        except Exception as e:
            P.logger.error(f'하위 폴더 조회 에러: {e}')
            return []


    def get_file_size_from_path(self, gds_path):
        """rclone으로 파일 크기 조회"""
        try:
            rclone_path = gds_path.replace('/ROOT/', 'gds2:')
            cmd = ['rclone', '--config', '/mnt/jaewoo/rclone/rclone.conf', 'lsjson', rclone_path, '--stat']
            
            result = self.run_rclone_with_retry(cmd)
            
            if result and result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get('Size', 0)
            
            return 0
        
        except subprocess.TimeoutExpired:
            P.logger.warning(f'파일 크기 조회 타임아웃 (재시도 실패): {gds_path}')
            return 0
        except Exception as e:
            P.logger.debug(f'파일 크기 조회 실패: {e}')
            return 0


    def get_folder_info_from_path(self, gds_path):
        """rclone으로 폴더 정보 조회 (크기, 파일 개수)"""
        try:
            rclone_path = gds_path.replace('/ROOT/', 'gds2:')
            
            # 폴더 내 파일 목록 조회 (재귀적, 시간이 더 걸릴 수 있음)
            cmd = ['rclone', '--config', '/mnt/jaewoo/rclone/rclone.conf', 'lsjson', rclone_path, '-R']
            
            result = self.run_rclone_with_retry(cmd, max_retries=2, timeout=30)
            
            if result and result.returncode == 0:
                items = json.loads(result.stdout)
                total_size = sum(item.get('Size', 0) for item in items if not item.get('IsDir', False))
                file_count = sum(1 for item in items if not item.get('IsDir', False))
                
                return {'size': total_size, 'count': file_count}
            
            return {'size': 0, 'count': 1}
        
        except Exception as e:
            P.logger.debug(f'폴더 정보 조회 실패: {e}')
            return {'size': 0, 'count': 1}


    def get_tmdb_metadata(self, tmdb_id):
        """TMDB API로 영화 메타데이터 조회"""
        try:
            # TMDB 사용 여부 확인
            if not P.ModelSetting.get_bool('monitor_tmdb_use_meta'):
                P.logger.debug('TMDB 메타데이터 사용 안 함')
                return None
            
            api_key = P.ModelSetting.get('monitor_tmdb_api_key')
            if not api_key:
                P.logger.warning('TMDB API 키가 설정되지 않았습니다')
                return None
            
            # TMDB API 호출
            url = f'https://api.themoviedb.org/3/movie/{tmdb_id}'
            params = {
                'api_key': api_key,
                'language': 'ko-KR'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # 장르 추출
                genres = [g['name'] for g in data.get('genres', [])]
                
                # 국가 추출 (첫 번째 국가만, 한글로 변환)
                production_countries = data.get('production_countries', [])
                if production_countries:
                    first_country = production_countries[0]['name']
                    countries = [self.convert_country_to_korean(first_country)]
                else:
                    countries = []
                
                # 포스터 URL 생성
                poster = ''
                if data.get('poster_path'):
                    poster = f"https://image.tmdb.org/t/p/w500{data['poster_path']}"
                
                metadata = {
                    'title': data.get('title', ''),
                    'originaltitle': data.get('original_title', ''),
                    'genre': genres,
                    'country': countries,
                    'year': int(data.get('release_date', '0000')[:4]) if data.get('release_date') else 0,
                    'poster': poster
                }
                
                P.logger.debug(f'TMDB 메타데이터 획득: {metadata["title"]} (장르: {genres}, 국가: {countries})')
                return metadata
            
            elif response.status_code == 404:
                P.logger.warning(f'TMDB에서 영화를 찾을 수 없습니다: {tmdb_id}')
                return None
            else:
                P.logger.warning(f'TMDB API 오류 (상태: {response.status_code}): {response.text}')
                return None
        
        except requests.exceptions.Timeout:
            P.logger.warning(f'TMDB API 타임아웃: {tmdb_id}')
            return None
        except Exception as e:
            P.logger.error(f'TMDB 메타데이터 조회 실패: {e}')
            P.logger.error(traceback.format_exc())
            return None


    def get_tmdb_movie_metadata_by_title(self, movie_title, year=None):
        """TMDB API로 영화 제목으로 검색 후 메타데이터 조회"""
        try:
            # TMDB 사용 여부 확인
            if not P.ModelSetting.get_bool('monitor_tmdb_use_meta'):
                P.logger.debug('TMDB 메타데이터 사용 안 함')
                return None
            
            api_key = P.ModelSetting.get('monitor_tmdb_api_key')
            if not api_key:
                P.logger.debug('TMDB API 키가 설정되지 않았습니다')
                return None
            
            # TMDB 검색 API 호출
            search_url = 'https://api.themoviedb.org/3/search/movie'
            params = {
                'api_key': api_key,
                'language': 'ko-KR',
                'query': movie_title,
                'include_adult': 'true'
            }
            
            # 연도 정보가 있으면 추가
            if year:
                params['year'] = year
            
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    P.logger.debug(f'TMDB에서 영화를 찾을 수 없습니다: {movie_title}')
                    return None
                
                # 첫 번째 결과 사용
                first_result = results[0]
                tmdb_id = first_result.get('id')
                
                # 상세 정보 조회 (get_tmdb_metadata 재사용)
                return self.get_tmdb_metadata(tmdb_id)
            
            else:
                P.logger.warning(f'TMDB 검색 API 오류 (상태: {response.status_code}): {response.text}')
                return None
        
        except requests.exceptions.Timeout:
            P.logger.warning(f'TMDB 검색 API 타임아웃: {movie_title}')
            return None
        except Exception as e:
            P.logger.error(f'TMDB 영화 검색 실패: {e}')
            P.logger.error(traceback.format_exc())
            return None


    def get_tmdb_tv_metadata(self, program_title):
        """TMDB TV API로 TV 프로그램 메타데이터 조회 (검색)"""
        try:
            # TMDB 사용 여부 확인
            if not P.ModelSetting.get_bool('monitor_tmdb_use_meta'):
                P.logger.debug('TMDB 메타데이터 사용 안 함')
                return None
            
            api_key = P.ModelSetting.get('monitor_tmdb_api_key')
            if not api_key:
                P.logger.debug('TMDB API 키가 설정되지 않았습니다')
                return None
            
            # TMDB TV 검색 API 호출
            search_url = 'https://api.themoviedb.org/3/search/tv'
            params = {
                'api_key': api_key,
                'language': 'ko-KR',
                'query': program_title,
                'page': 1
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    P.logger.debug(f'TMDB에서 TV 프로그램을 찾을 수 없습니다: {program_title}')
                    return None
                
                # 첫 번째 결과 사용
                tv_show = results[0]
                tv_id = tv_show.get('id')
                
                # 상세 정보 조회 (장르 정보 포함)
                detail_url = f'https://api.themoviedb.org/3/tv/{tv_id}'
                detail_params = {
                    'api_key': api_key,
                    'language': 'ko-KR'
                }
                
                detail_response = requests.get(detail_url, params=detail_params, timeout=10)
                
                if detail_response.status_code == 200:
                    detail_data = detail_response.json()
                    
                    # 장르 추출
                    genres = [g['name'] for g in detail_data.get('genres', [])]
                    
                    # 포스터 URL 생성
                    poster = ''
                    if detail_data.get('poster_path'):
                        poster = f"https://image.tmdb.org/t/p/w500{detail_data['poster_path']}"
                    
                    metadata = {
                        'title': detail_data.get('name', program_title),
                        'genre': ', '.join(genres) if genres else '예능',
                        'poster': poster,
                        'code': f'tmdb-{tv_id}'
                    }
                    
                    P.logger.debug(f'TMDB TV 메타데이터 획득: {metadata["title"]} (장르: {genres}, 포스터: {"있음" if poster else "없음"})')
                    return metadata
                
                return None
            
            elif response.status_code == 404:
                P.logger.debug(f'TMDB에서 TV 프로그램을 찾을 수 없습니다: {program_title}')
                return None
            else:
                P.logger.warning(f'TMDB TV API 오류 (상태: {response.status_code})')
                return None
        
        except requests.exceptions.Timeout:
            P.logger.warning(f'TMDB TV API 타임아웃: {program_title}')
            return None
        except Exception as e:
            P.logger.error(f'TMDB TV 메타데이터 조회 실패: {e}')
            P.logger.error(traceback.format_exc())
            return None


    def check_watch_paths(self, gds_path, watch_paths_setting):
        """감시 경로 규칙 체크 (화이트리스트 방식)"""
        try:
            if not watch_paths_setting:
                # 감시 경로가 설정되지 않으면 모든 경로 허용
                return True
            
            # 줄바꿈으로 구분된 감시 경로 목록
            watch_paths = [path.strip() for path in watch_paths_setting.split('\n') if path.strip()]
            
            for watch_path in watch_paths:
                # 경로가 감시 규칙에 포함되는지 확인
                if watch_path in gds_path:
                    P.logger.debug(f'감시 경로 매칭: {gds_path} (규칙: {watch_path})')
                    return True
            
            # 어떤 감시 경로에도 매칭되지 않으면 무시
            P.logger.debug(f'감시 경로 밖: {gds_path}')
            return False
        except Exception as e:
            P.logger.error(f'경로 무시 규칙 체크 실패: {e}')
            return False


    def monitor_loop(self):
        """로그 파일 모니터링 루프 (tail -F 방식)"""
        try:
            log_file = P.ModelSetting.get('monitor_log_file_path')
            scan_lines = int(P.ModelSetting.get('monitor_scan_past_lines'))
            
            P.logger.info(f'로그 파일 모니터링 시작: {log_file} (tail -F 방식)')
            
            # 과거 로그 스캔
            if scan_lines > 0:
                self.scan_past_logs(log_file, scan_lines)
            
            # tail -F를 사용한 실시간 모니터링
            # -F: 파일 로테이션 자동 추적 (gds_tool.log → gds_tool.log.1)
            # -n 0: 파일 끝부터 시작 (과거 로그는 이미 스캔함)
            tail_proc = subprocess.Popen(
                ['tail', '-F', '-n', '0', log_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding='utf-8',
                bufsize=1  # 라인 버퍼링
            )
            
            # select.poll()로 non-blocking 읽기
            poller = select.poll()
            poller.register(tail_proc.stdout, select.POLLIN)
            
            P.logger.info('tail -F 프로세스 시작 완료')
            
            while self.monitor_running:
                # 1초 대기 (1000ms)
                events = poller.poll(1000)
                
                if events:
                    line = tail_proc.stdout.readline()
                    if line:
                        self.process_log_line(line.strip())
                
                # tail 프로세스가 종료되었는지 확인
                if tail_proc.poll() is not None:
                    P.logger.warning('tail 프로세스가 종료됨. 모니터링 중단.')
                    break
            
            # 정리
            tail_proc.terminate()
            try:
                tail_proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                tail_proc.kill()
            
            P.logger.info('로그 모니터링 종료')
        
        except Exception as e:
            P.logger.error(f'모니터링 에러: {e}')
            P.logger.error(traceback.format_exc())
            self.monitor_running = False


    def scan_past_logs(self, log_file, num_lines):
        """과거 로그 스캔"""
        try:
            P.logger.info(f'과거 로그 스캔 시작: 최근 {num_lines}줄')
            
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                start_idx = max(0, len(lines) - num_lines)
                
                for line in lines[start_idx:]:
                    self.process_log_line(line.strip())
            
            P.logger.info('과거 로그 스캔 완료')
        
        except Exception as e:
            P.logger.error(f'과거 로그 스캔 에러: {e}')
            P.logger.error(traceback.format_exc())


    def process_log_line(self, line):
        """로그 라인 처리 - Discord JSON 로그 파싱"""
        try:
            # GDS Tool 로그에서 Discord 메시지 JSON 추출
            # 형식: "2026-02-08 12:43:12,732  DEBUGgds_tool mod_fp.py:175 {'type': 'FF', ...}"
            
            # JSON 부분만 추출 (중괄호로 시작)
            if '{' not in line:
                return
            
            json_start = line.index('{')
            json_str = line[json_start:]
            
            # Python dict 형식을 JSON으로 변환 (작은따옴표 -> 큰따옴표)
            json_str = json_str.replace("'", '"')
            json_str = json_str.replace('True', 'true').replace('False', 'false').replace('None', 'null')
            
            # JSON 파싱
            data = json.loads(json_str)
            
            # Discord bot_downloader 형식 확인
            if data.get('type') != 'FF' or 'msg' not in data:
                return
            
            msg = data['msg']
            
            # GDS Tool의 파일/폴더 변경 메시지 확인
            if msg.get('t1') == 'gds_tool' and msg.get('t2') == 'fp':
                # 'data' 필드에 실제 정보가 있음
                if 'data' not in msg or 'gds_path' not in msg['data']:
                    return
                
                gds_path = msg['data']['gds_path']
                scan_mode = msg['data'].get('scan_mode', '')
                
                # ADD와 REFRESH 처리 (REMOVE_FILE, REMOVE_FOLDER 등은 무시)
                if scan_mode not in ['ADD', 'REFRESH']:
                    return
                
                P.logger.debug(f'GDS 경로 감지: {gds_path} (모드: {scan_mode})')
                
                # 경로 기반 우선 판단 (영화 경로는 파일이든 폴더든 영화로 처리)
                if '/ROOT/GDRIVE/VIDEO/영화/' in gds_path:
                    # 영화 경로 처리
                    movie_path = gds_path
                    
                    # 파일 경로인지 확인 (확장자가 있으면 파일)
                    filename = gds_path.split('/')[-1]
                    if '.' in filename:
                        # 파일이면 부모 폴더로 거슬러 올라가서 영화 폴더 찾기
                        path_parts = gds_path.split('/')
                        movie_path = None
                        
                        # 연도 패턴 (YYYY) 찾을 때까지 거슬러 올라가기
                        for i in range(len(path_parts)-1, -1, -1):
                            if re.search(r'\(\d{4}\)', path_parts[i]):
                                movie_path = '/'.join(path_parts[:i+1])
                                P.logger.debug(f'영화 폴더 찾음: {movie_path}')
                                break
                        
                        if not movie_path:
                            P.logger.debug(f'영화 폴더를 찾을 수 없음: {gds_path}')
                            return
                    
                    # 영화 폴더 처리
                    self.process_movie_from_path(movie_path, data)
                elif self.is_vod_file(gds_path):
                    # VOD 파일 (영상 + 자막): 외국 VOD인지 국내 VOD인지 구분
                    if '/방송중/외국/' in gds_path or '/외국TV' in gds_path:
                        # 외국 VOD
                        self.process_foreign_vod_from_path(gds_path, data)
                    else:
                        # 국내 VOD
                        self.process_vod_from_path(gds_path, data)
        
        except json.JSONDecodeError as e:
            # JSON 파싱 실패는 조용히 무시 (일반 로그 라인일 수 있음)
            pass
        except Exception as e:
            P.logger.error(f'로그 처리 에러: {e}')
            P.logger.error(traceback.format_exc())


    def process_vod_from_path(self, gds_path, original_data):
        """GDS 경로에서 VOD 처리"""
        try:
            # 감시 경로 규칙 체크 (화이트리스트 방식)
            watch_paths = P.ModelSetting.get('vod_watch_paths')
            if not self.check_watch_paths(gds_path, watch_paths):
                P.logger.info(f'VOD 무시 (감시 경로 밖): {gds_path}')
                return
            
            # 경로에서 파일명 추출
            # 예1: /ROOT/GDRIVE/VIDEO/방송중/교양/비밀서고 트라이앵글 (2026) [MBN]/비밀서고 트라이앵글.E06.260208.1080p-ST.mp4
            # 예2: /ROOT/GDRIVE/VIDEO/방송중/OTT 애니메이션/고양이 피터 (2017)/Season 2/Pete.the.Cat.S02E10.mkv
            path_parts = gds_path.split('/')
            filename = path_parts[-1]  # 파일명
            
            # 프로그램명과 카테고리 추출 (외국 VOD와 동일한 로직)
            import re
            program_title = ''
            program_folder = ''
            parent_folder = ''  # 초기화 추가
            category = '미분류'
            
            # watch_paths 경로 이후에서 (연도) 패턴이 있는 폴더를 찾아 프로그램명으로 사용
            try:
                # 감시 경로 중 매칭되는 경로 찾기
                watch_paths = P.ModelSetting.get('vod_watch_paths')
                matching_prefix = None
                for watch_path in watch_paths.strip().split('\n'):
                    watch_path = watch_path.strip()
                    if watch_path and gds_path.startswith(watch_path):
                        matching_prefix = watch_path
                        break
                
                if matching_prefix:
                    # 감시 경로 이후의 부분에서 찾기
                    relative_path = gds_path[len(matching_prefix):]
                    relative_parts = relative_path.split('/')
                    
                    # 카테고리는 감시 경로 바로 다음 폴더 (예: 교양, 예능, 드라마)
                    if len(relative_parts) > 1:
                        category = relative_parts[1]
                    
                    # (연도) 패턴 찾기
                    for i, folder in enumerate(relative_parts):
                        # (4자리 숫자) 패턴 찾기 - 예: (2025), (2026), (2017)
                        if re.search(r'\(\d{4}\)', folder):
                            program_folder = folder
                            # 괄호 앞 부분만 추출하여 프로그램명으로 사용
                            program_title = folder.split('(')[0].strip()
                            break
                
                # 프로그램 폴더를 못 찾았으면 파일의 부모 폴더 사용 (fallback)
                if not program_title:
                    parent_folder = path_parts[-2] if len(path_parts) > 1 else ''
                    # Season 폴더인지 확인
                    if parent_folder and re.match(r'(?i)^Season\s+\d+$', parent_folder):
                        # Season 폴더면 그 상위 폴더 사용
                        parent_folder = path_parts[-3] if len(path_parts) > 2 else parent_folder
                        category = path_parts[-4] if len(path_parts) > 3 else category
                    
                    program_folder = parent_folder
                    if '(' in parent_folder:
                        program_title = parent_folder.split('(')[0].strip()
                    elif '[' in parent_folder:
                        program_title = parent_folder.split('[')[0].strip()
                    else:
                        program_title = parent_folder
                else:
                    # (연도) 패턴으로 program_folder를 찾은 경우, parent_folder도 설정
                    parent_folder = program_folder
            
            except Exception as e:
                P.logger.warning(f'프로그램명 추출 실패: {e}, fallback 사용')
                parent_folder = path_parts[-2] if len(path_parts) > 1 else ''
                program_folder = parent_folder
                program_title = parent_folder.split('(')[0].strip() if '(' in parent_folder else parent_folder
                category = path_parts[-3] if len(path_parts) > 2 else '미분류'
            
            P.logger.info(f'VOD 발견: {filename}')
            P.logger.debug(f'  → 프로그램: {program_title}')
            P.logger.debug(f'  → 카테고리: {category}')
            P.logger.debug(f'  → 프로그램 폴더: {program_folder if program_folder else "N/A"}')
            
            # rclone으로 Google Drive ID 조회 (파일)
            file_id = self.get_gdrive_id_from_path(gds_path, is_folder=False)
            if not file_id:
                P.logger.warning(f'VOD ID 조회 실패: {filename}')
                return
            
            P.logger.debug(f'VOD ID: {file_id}')
            
            # rclone으로 파일 크기 조회
            file_size = self.get_file_size_from_path(gds_path)
            
            # 메타데이터 구성 (우선순위: bot_downloader → TMDB → 경로 추출)
            meta = None
            
            # 1. bot_downloader DB에서 메타데이터 조회 시도 (KD 코드 포함)
            try:
                bot_downloader_plugin = F.PluginManager.get_plugin_instance('bot_downloader')
                if bot_downloader_plugin:
                    ModelVodItem_BD = bot_downloader_plugin.logic.get_module('vod').web_list_model
                    bd_item = ModelVodItem_BD.get_by_filename(filename)
                    if bd_item and bd_item.meta_code:
                        # bot_downloader에 정보가 있으면 사용 (KD 코드 포함)
                        P.logger.debug(f'bot_downloader에서 메타데이터 획득: {bd_item.meta_title} (code: {bd_item.meta_code})')
                        meta = {
                            'title': bd_item.meta_title or program_title,
                            'code': bd_item.meta_code or '',
                            'genre': bd_item.meta_genre or category,
                            'poster': bd_item.meta_poster or ''
                        }
            except Exception as e:
                P.logger.debug(f'bot_downloader DB 조회 실패 또는 설치 안 됨: {e}')
            
            # 2. bot_downloader에 없으면 TMDB 조회
            if not meta:
                tmdb_meta = self.get_tmdb_tv_metadata(program_title)
                if tmdb_meta:
                    # TMDB에서 포스터만 가져오고, 장르는 GDS 경로에서 추출한 것 사용
                    P.logger.debug(f'TMDB에서 메타데이터 획득: {program_title}')
                    meta = {
                        'title': program_title,
                        'code': tmdb_meta.get('code', ''),
                        'genre': category,  # GDS 경로의 카테고리 우선
                        'poster': tmdb_meta.get('poster', '')
                    }
                else:
                    # 3. 모두 실패 시 기본 정보 사용 (경로에서 추출한 정보)
                    P.logger.debug(f'메타데이터 없음, 경로 정보 사용: {program_title}')
                    meta = {
                        'title': program_title,
                        'code': '',
                        'genre': category,
                        'poster': ''
                    }
            
            # Discord bot_downloader 형식으로 변환
            data = {
                'msg': {
                    'data': {
                        'id': file_id,
                        'f': filename,
                        's': file_size,
                        'gds_path': gds_path,  # 폴더 복사를 위한 전체 경로
                        'program_folder': parent_folder,  # 프로그램 폴더명
                        'vod': {
                            'name': filename.split('.')[0],
                            'no': 0,
                            'release': '',
                            'date': '',
                            'quality': '1080p' if '1080p' in filename else '720p'
                        },
                        'meta': meta if program_title else None
                    }
                }
            }
            
            # VOD 모듈에 전달
            vod_module = P.module_list[0]
            vod_module.process_log_data(data)
        
        except Exception as e:
            P.logger.error(f'VOD 처리 에러: {e}')
            P.logger.error(traceback.format_exc())


    def process_foreign_vod_from_path(self, gds_path, original_data):
        """GDS 경로에서 외국 VOD 처리"""
        try:
            # 감시 경로 규칙 체크 (화이트리스트 방식)
            watch_paths = P.ModelSetting.get('foreign_vod_watch_paths')
            if not self.check_watch_paths(gds_path, watch_paths):
                P.logger.info(f'외국 VOD 무시 (감시 경로 밖): {gds_path}')
                return
            
            # 경로에서 파일명 추출
            # 예1: /ROOT/GDRIVE/VIDEO/방송중/외국/중드/승천중, 방해 사절 (2025)/물요비승.S01E13.260207.1080p-SW.mp4
            # 예2: /ROOT/GDRIVE/VIDEO/방송중/외국/중드/당궁기안 (2026)/Season 1/Unveil.Jadewind.S01E03.mkv
            # 예3: /ROOT/GDRIVE/VIDEO/외국TV/프로그램명 (2025)/파일.mkv
            path_parts = gds_path.split('/')
            filename = path_parts[-1]  # 파일명
            
            # /방송중/외국/ 또는 /외국TV 이후의 경로에서 (연도) 패턴이 있는 폴더를 찾아 프로그램명으로 사용
            import re
            program_title = ''
            program_folder = ''
            category = '미분류'
            
            # /방송중/외국/ 또는 /외국TV 인덱스 찾기
            try:
                foreign_idx = -1
                for i, part in enumerate(path_parts):
                    if part == '외국' or part == '외국TV':
                        foreign_idx = i
                        break
                
                if foreign_idx >= 0:
                    # 카테고리는 /외국/ 바로 다음 폴더 (예: 중드, 미드, 일드)
                    # 외국TV의 경우 외국TV 자체가 카테고리가 될 수도 있음
                    if path_parts[foreign_idx] == '외국TV':
                        # /외국TV/프로그램명/ 구조인 경우
                        category = '외국TV'
                    elif foreign_idx + 1 < len(path_parts):
                        category = path_parts[foreign_idx + 1]
                    
                    # /외국/ 또는 /외국TV/ 이후의 모든 폴더에서 (연도) 패턴 찾기
                    for i in range(foreign_idx + 1, len(path_parts)):
                        folder = path_parts[i]
                        # (4자리 숫자) 패턴 찾기 - 예: (2025), (2026)
                        if re.search(r'\(\d{4}\)', folder):
                            program_folder = folder
                            # 괄호 앞 부분만 추출하여 프로그램명으로 사용
                            program_title = folder.split('(')[0].strip()
                            break
                
                # 프로그램 폴더를 못 찾았으면 파일의 부모 폴더 사용 (fallback)
                if not program_title:
                    parent_folder = path_parts[-2] if len(path_parts) > 1 else ''
                    program_folder = parent_folder  # 폴더 복사를 위해 설정
                    if '(' in parent_folder:
                        program_title = parent_folder.split('(')[0].strip()
                    elif '[' in parent_folder:
                        program_title = parent_folder.split('[')[0].strip()
                    else:
                        program_title = parent_folder
            
            except Exception as e:
                P.logger.warning(f'프로그램명 추출 실패: {e}, fallback 사용')
                parent_folder = path_parts[-2] if len(path_parts) > 1 else ''
                program_folder = parent_folder  # 폴더 복사를 위해 설정
                program_title = parent_folder.split('(')[0].strip() if '(' in parent_folder else parent_folder
            
            P.logger.info(f'외국 VOD 발견: {filename}')
            P.logger.debug(f'  → 프로그램: {program_title}')
            P.logger.debug(f'  → 카테고리: {category}')
            P.logger.debug(f'  → 프로그램 폴더: {program_folder if program_folder else "N/A"}')
            
            # rclone으로 Google Drive ID 조회 (파일)
            file_id = self.get_gdrive_id_from_path(gds_path, is_folder=False)
            if not file_id:
                P.logger.warning(f'외국 VOD ID 조회 실패: {filename}')
                return
            
            P.logger.debug(f'외국 VOD ID: {file_id}')
            
            # rclone으로 파일 크기 조회
            file_size = self.get_file_size_from_path(gds_path)
            
            # TMDB TV API로 메타데이터 조회 (포스터만 사용)
            tmdb_meta = self.get_tmdb_tv_metadata(program_title)
            
            # 메타데이터 구성
            if tmdb_meta:
                # TMDB에서 포스터만 가져오고, 장르는 GDS 경로에서 추출한 것 사용
                meta = {
                    'title': program_title,  # 프로그램명은 폴더명에서 추출한 것 사용
                    'code': tmdb_meta.get('code', ''),
                    'genre': category,  # GDS 경로의 카테고리 사용 (예: 해외 드라마, 해외 예능)
                    'poster': tmdb_meta.get('poster', '')
                }
            else:
                # TMDB 실패 시 기본 정보 사용 (포스터 없음)
                meta = {
                    'title': program_title,
                    'code': '',
                    'genre': category,  # GDS 경로의 카테고리 사용
                    'poster': ''
                }
            
            # Discord bot_downloader 형식으로 변환
            data = {
                'msg': {
                    'data': {
                        'id': file_id,
                        'f': filename,
                        's': file_size,
                        'gds_path': gds_path,  # 폴더 복사를 위한 전체 경로
                        'program_folder': program_folder,  # 프로그램 폴더명 (예: "프렌즈 (1994) [NBC]")
                        'vod': {
                            'name': filename.split('.')[0],
                            'no': 0,
                            'release': '',
                            'date': '',
                            'quality': '1080p' if '1080p' in filename else '720p'
                        },
                        'meta': meta if program_title else None
                    }
                }
            }
            
            # 외국 VOD 모듈에 전달
            foreign_vod_module = P.module_list[1]  # ModuleForeignVod
            foreign_vod_module.process_log_data(data)
        
        except Exception as e:
            P.logger.error(f'외국 VOD 처리 에러: {e}')
            P.logger.error(traceback.format_exc())


    def process_movie_from_path(self, gds_path, original_data):
        """GDS 경로에서 영화 처리"""
        try:
            # 감시 경로 규칙 체크 (화이트리스트 방식)
            watch_paths = P.ModelSetting.get('share_movie_watch_paths')
            if not self.check_watch_paths(gds_path, watch_paths):
                P.logger.info(f'영화 무시 (감시 경로 밖): {gds_path}')
                return
            
            # 경로에서 폴더명 추출
            foldername = gds_path.split('/')[-1]
            
            P.logger.info(f'영화 폴더 발견: {foldername}')
            
            # 캐시 키 결정: TMDB ID 우선, 없으면 정규화된 폴더명
            tmdb_match = re.search(r'\{tmdb-(\d+)\}', foldername)
            if tmdb_match:
                cache_key = f"tmdb-{tmdb_match.group(1)}"
            else:
                # TMDB ID 없으면 정규화된 제목+연도로 캐시
                # 예: "포레스트 검프 (1994)" → "포레스트검프(1994)"
                # 예: "포레스트-검프 (1994)" → "포레스트검프(1994)" (같은 캐시 키!)
                title_match = re.match(r'(.+?)\s*\((\d{4})\)', foldername)
                if title_match:
                    title_norm = self.normalize_title(title_match.group(1))
                    year = title_match.group(2)
                    cache_key = f"{title_norm}({year})"
                else:
                    cache_key = self.normalize_title(foldername)
            
            # 캐시 체크 (중복 처리 방지)
            current_time = time.time()
            if cache_key in self.movie_cache:
                cached_time = self.movie_cache[cache_key]
                if current_time - cached_time < self.movie_cache_ttl:
                    P.logger.debug(f'영화 스킵 (이미 처리됨, 캐시: {cache_key}): {foldername}')
                    return
            
            # 캐시 저장
            self.movie_cache[cache_key] = current_time
            P.logger.debug(f'영화 캐시 저장 (키: {cache_key})')
            
            # 하위 폴더 탐색 (여러 버전이 있을 수 있음)
            subfolders = self.find_all_movie_folders(gds_path, foldername)
            
            if subfolders:
                # 하위 폴더가 있으면 각각 처리 (부모 폴더는 처리하지 않음)
                P.logger.info(f'하위 폴더 {len(subfolders)}개 발견')
                for subfolder in subfolders:
                    subfolder_name = subfolder['name']
                    subfolder_gds_path = f"{gds_path.rstrip('/')}/{subfolder_name}"
                    
                    # 각 하위 폴더를 개별 영화로 처리
                    self.process_single_movie_folder(
                        subfolder_gds_path,
                        subfolder_name,
                        subfolder['id']
                    )
            else:
                # 하위 폴더가 없으면 현재 폴더를 처리
                P.logger.info(f'하위 폴더 없음. 현재 폴더를 처리합니다: {foldername}')
                folder_id = self.get_gdrive_id_from_path(gds_path, is_folder=True)
                if folder_id:
                    self.process_single_movie_folder(gds_path, foldername, folder_id)
                else:
                    P.logger.warning(f'폴더 ID 조회 실패: {foldername}')
        
        except Exception as e:
            P.logger.error(f'영화 처리 에러: {e}')
            P.logger.error(traceback.format_exc())


    def process_single_movie_folder(self, gds_path, foldername, folder_id):
        """단일 영화 폴더 처리 (실제 DB 저장)"""
        try:
            current_time = time.time()
            
            # 1. 캐시 체크 (중복 방지) - 전체 경로를 키로 사용
            cache_key = gds_path
            if cache_key in self.movie_cache:
                cached_time = self.movie_cache[cache_key]
                if current_time - cached_time < self.movie_cache_ttl:
                    P.logger.debug(f'영화 스킵 (캐시됨): {foldername}')
                    return
            
            # 2. DB 중복 체크 (rclone 호출 전에 먼저 확인)
            from .mod_share_movie import ModelShareMovieItem
            existing = ModelShareMovieItem.get_by_folderid(folder_id)
            if existing is not None:
                P.logger.info(f'영화 스킵 (이미 DB에 있음): {foldername}')
                # 캐시에 저장 (다음번에는 DB 체크도 안 함)
                self.movie_cache[cache_key] = current_time
                return
            
            # 3. 캐시 저장
            self.movie_cache[cache_key] = current_time
            
            P.logger.info(f'영화 처리 중: {foldername}')
            P.logger.debug(f'영화 ID: {folder_id}')
            
            # rclone으로 폴더 크기와 파일 개수 조회
            folder_info = self.get_folder_info_from_path(gds_path)
            
            # TMDB ID 추출 (있을 수도, 없을 수도)
            tmdb_match = re.search(r'\{tmdb-(\d+)\}', foldername)
            tmdb_id = tmdb_match.group(1) if tmdb_match else None
            
            # 제목 추출 (괄호 앞까지)
            title_match = re.match(r'(.+?)\s*\((\d{4})\)', foldername)
            title = title_match.group(1).strip() if title_match else foldername.split('{')[0].strip()
            # 대괄호로 시작하는 부분 제거
            if '[' in title:
                title = title.split('[')[0].strip()
            year = int(title_match.group(2)) if title_match else 0
            
            # TMDB API로 메타데이터 조회
            tmdb_meta = None
            if tmdb_id:
                # TMDB ID가 있으면 ID로 조회
                tmdb_meta = self.get_tmdb_metadata(tmdb_id)
            else:
                # TMDB ID가 없으면 제목으로 검색
                tmdb_meta = self.get_tmdb_movie_metadata_by_title(title, year)
            
            # 메타데이터 구성
            if tmdb_meta:
                # TMDB에서 가져온 정보 사용
                meta = {
                    'title': tmdb_meta.get('title', title),
                    'originaltitle': tmdb_meta.get('originaltitle', title),
                    'genre': tmdb_meta.get('genre', []),
                    'code': f'tmdb-{tmdb_id}',
                    'year': tmdb_meta.get('year', year),
                    'poster': tmdb_meta.get('poster', ''),
                    'country': tmdb_meta.get('country', [])
                }
            else:
                # TMDB 없거나 실패 시 기본 정보 사용
                meta = {
                    'title': title,
                    'originaltitle': title,
                    'genre': [],
                    'code': f'tmdb-{tmdb_id}' if tmdb_id else '',
                    'year': year,
                    'poster': '',
                    'country': []
                }
            
            # Discord bot_downloader 형식으로 변환
            data = {
                'msg': {
                    'data': {
                        'ca': 'movie',
                        'subject': foldername,
                        'folderid': folder_id,
                        'size': folder_info['size'],
                        'count': folder_info['count'],
                        'foldername': foldername,
                        'meta': meta
                    }
                }
            }
            
            # 영화 모듈에 전달
            movie_module = P.module_list[2]  # ModuleShareMovie는 3번째 (index 2)
            movie_module.process_log_data(data)
        
        except Exception as e:
            P.logger.error(f'단일 영화 폴더 처리 에러: {e}')
            P.logger.error(traceback.format_exc())


    @staticmethod
    def send_discord_notification(title, description, color=0x00ff00, fields=None, thumbnail_url=None):
        """디스코드 웹훅으로 알림 전송"""
        try:
            if not P.ModelSetting.get_bool('monitor_discord_use_notify'):
                return
            
            webhook_url = P.ModelSetting.get('monitor_discord_webhook_url')
            if not webhook_url:
                return
            
            # 한국 시간 (UTC+9)으로 timestamp 생성
            kst = timezone(timedelta(hours=9))
            
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "timestamp": datetime.now(kst).isoformat()
            }
            
            if fields:
                embed["fields"] = fields
            
            if thumbnail_url:
                embed["thumbnail"] = {"url": thumbnail_url}
            
            payload = {
                "embeds": [embed]
            }
            
            response = requests.post(webhook_url, json=payload, timeout=5)
            if response.status_code == 204:
                P.logger.debug(f'디스코드 알림 전송 성공: {title}')
            else:
                P.logger.warning(f'디스코드 알림 전송 실패: {response.status_code}')
        
        except Exception as e:
            P.logger.error(f'디스코드 알림 전송 에러: {e}')
            P.logger.error(traceback.format_exc())


    @staticmethod
    def search_gds_path_in_log(filename):
        """
        로그 파일에서 파일명으로 검색하여 gds_path와 program_folder 반환
        로그 로테이션된 파일도 자동 검색 (.log, .log.1, .log.2, ..., .log.5)
        
        Args:
            filename: 검색할 파일명
            
        Returns:
            dict: {'gds_path': str, 'program_folder': str} 또는 None
        """
        try:
            import json
            import re
            import os
            import ast
            
            # 올바른 설정 키 사용
            log_file_path = P.ModelSetting.get('monitor_log_file_path')
            if not log_file_path:
                P.logger.warning(f'로그 파일 경로가 설정되지 않음')
                P.logger.info(f'💡 해결 방법: 모니터 > 설정에서 "로그 파일 경로"를 설정하세요')
                return None
            
            # 로그 파일 목록 구성 (현재 파일 + 로테이션된 파일들)
            log_files = []
            
            # 메인 로그 파일 추가
            if os.path.exists(log_file_path):
                log_files.append(log_file_path)
            
            # 로테이션된 파일들 추가 (.log.1, .log.2, .log.3, .log.4, .log.5)
            for i in range(1, 6):
                rotated_file = f"{log_file_path}.{i}"
                if os.path.exists(rotated_file):
                    log_files.append(rotated_file)
            
            if not log_files:
                P.logger.warning(f'로그 파일이 존재하지 않음: {log_file_path}')
                return None
            
            P.logger.info(f'로그 파일 검색 시작: {filename}')
            P.logger.info(f'검색 대상: {len(log_files)}개 파일 (최대 6개: .log, .log.1~.log.5)')
            P.logger.debug(f'존재하는 파일: {", ".join([os.path.basename(f) for f in log_files])}')
            
            # 각 로그 파일을 순서대로 검색 (최신 파일부터)
            for log_file in log_files:
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        search_lines = min(20000, len(lines))
                        P.logger.debug(f'  [{os.path.basename(log_file)}] 총 {len(lines)}줄, 검색: 최근 {search_lines}줄')
                        
                        # 최근 로그부터 검색 (역순)
                        for line in reversed(lines[-search_lines:]):
                            if filename in line:
                                # JSON 파싱 시도
                                try:
                                    # 로그 형식: 시간|레벨|모듈|JSON (싱글 쿼트 사용)
                                    if '{' in line:
                                        json_str = line[line.index('{'):].strip()
                                        
                                        # GDS Tool 로그는 싱글 쿼트를 사용하므로 ast.literal_eval 사용
                                        log_data = ast.literal_eval(json_str)
                                        
                                        # gds_path 추출
                                        # 로그 형식: {'msg': {'data': {'gds_path': '...'}}} 또는 {'data': {'gds_path': '...'}}
                                        gds_path = None
                                        if 'msg' in log_data and 'data' in log_data['msg'] and 'gds_path' in log_data['msg']['data']:
                                            gds_path = log_data['msg']['data']['gds_path']
                                        elif 'data' in log_data and 'gds_path' in log_data['data']:
                                            gds_path = log_data['data']['gds_path']
                                        
                                        if gds_path:
                                            # 프로그램 폴더명 추출
                                            path_parts = gds_path.split('/')
                                            program_folder = ''
                                            
                                            # 외국 VOD인지 확인
                                            is_foreign = False
                                            for part in path_parts:
                                                if part == '외국' or part == '외국TV':
                                                    is_foreign = True
                                                    break
                                            
                                            if is_foreign:
                                                # 외국 VOD: (연도) 패턴이 있는 폴더 찾기
                                                for part in path_parts:
                                                    if re.search(r'\(\d{4}\)', part):
                                                        program_folder = part
                                                        break
                                                # 못 찾았으면 파일의 부모 폴더
                                                if not program_folder and len(path_parts) > 1:
                                                    program_folder = path_parts[-2]
                                            else:
                                                # 국내 VOD: 파일의 부모 폴더
                                                if len(path_parts) > 1:
                                                    program_folder = path_parts[-2]
                                            
                                            P.logger.info(f'✅ 로그에서 경로 발견!')
                                            P.logger.info(f'   파일: {os.path.basename(log_file)}')
                                            P.logger.info(f'   경로: {gds_path}')
                                            P.logger.info(f'   폴더: {program_folder}')
                                            
                                            return {
                                                'gds_path': gds_path,
                                                'program_folder': program_folder
                                            }
                                except (ValueError, SyntaxError) as e:
                                    # ast.literal_eval 실패
                                    P.logger.debug(f'로그 파싱 실패: {e}')
                                    continue
                                except Exception as e:
                                    P.logger.debug(f'로그 처리 실패: {e}')
                                    continue
                
                except Exception as e:
                    P.logger.debug(f'파일 읽기 실패 ({os.path.basename(log_file)}): {e}')
                    continue
            
            P.logger.warning(f'로그에서 파일을 찾을 수 없음: {filename}')
            P.logger.info(f'💡 검색 완료: {len(log_files)}개 파일 (각 최대 20,000줄)')
            return None
        
        except Exception as e:
            P.logger.error(f'로그 검색 중 에러: {e}')
            P.logger.error(traceback.format_exc())
            return None
