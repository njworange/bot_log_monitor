from .setup import *


class ModuleVod(PluginModuleBase):

    def __init__(self, P):
        super(ModuleVod, self).__init__(P, name='vod', first_menu='list')
        self.db_default = {
            f'{self.name}_db_version' : '2',  # v1.0.32: 중복 방지 기능 제거, 안정성 우선
            'vod_remote_path' : '',
            'vod_download_mode' : 'none', #Nothing, 모두받기, 블랙, 화이트
            'vod_blacklist_genre' : '',
            'vod_blacklist_program' : '',
            'vod_whitelist_genre' : '',
            'vod_whitelist_program' : '',
            'vod_watch_paths' : '/ROOT/GDRIVE/VIDEO/방송중/',  # 감시 경로 규칙 (줄바꿈으로 구분)
            'vod_item_last_list_option': '',
            f'{self.name}_db_delete_day': '30',
            f'{self.name}_db_auto_delete': 'False',
            'vod_use_notify': 'False',
        }
        self.web_list_model = ModelVodItem


    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret':'success'}
        if command == 'option':
            mode = arg1
            value = arg2
            value_list = P.ModelSetting.get_list(f'vod_{mode}', '|')
            if value in value_list:
                ret['ret'] = 'warning'
                ret['msg'] = '이미 설정되어 있습니다.'
            else:
                if len(value_list) == 0:
                    P.ModelSetting.set(f'vod_{mode}', value)
                else:
                    P.ModelSetting.set(f'vod_{mode}', P.ModelSetting.get(f'vod_{mode}') + ' | ' + value)
                ret['msg'] = '추가하였습니다'
        elif command == 'request_copy':
            item = ModelVodItem.get_by_id(arg1)
            ret = self.share_copy(item)
            if item is not None:
                item.save()
            return jsonify(ret)
        elif command == 'request_folder_copy':
            item = ModelVodItem.get_by_id(arg1)
            ret = self.share_folder_copy(item)
            if item is not None:
                item.save()
            return jsonify(ret)
        elif command == 'db_delete':
            if self.web_list_model.delete_by_id(arg1):
                ret['msg'] = '삭제하였습니다.'
            else:
                ret['ret'] = 'warning'
                ret['msg'] = '삭제 실패'

        return jsonify(ret)


    def process_log_data(self, data):
        """로그 데이터 처리 (Discord 데이터와 동일한 형식)"""
        filename = data.get('msg', {}).get('data', {}).get('f', '알 수 없음')
        item = ModelVodItem.process_discord_data(data)
        if item is None:
            P.logger.debug(f'VOD 스킵 (이미 존재): {filename}')
            return
        try:
            P.logger.debug(f'VOD 필터링 체크: {item.filename}')
            flag_download = self.condition_check_download_mode(item)
            P.logger.debug(f'VOD 필터링 결과: {flag_download} (로그: {item.log})')
            
            if flag_download:
                P.logger.info(f'VOD 복사 요청: {item.filename}')
                result = self.share_copy(item)
                P.logger.debug(f'VOD 복사 결과: {result}')
                
                # 디스코드 알림
                if result and result.get('ret') == 'success':
                    from .mod_monitor import ModuleMonitor
                    fields = [
                        {"name": "파일명", "value": item.filename, "inline": False},
                        {"name": "프로그램", "value": item.meta_title or '정보 없음', "inline": True},
                        {"name": "장르", "value": item.meta_genre or '미분류', "inline": True},
                        {"name": "크기", "value": f"{item.size / (1024**3):.2f} GB" if item.size else "정보 없음", "inline": True}
                    ]
                    ModuleMonitor.send_discord_notification(
                        title="🎬 국내 VOD 복사 요청",
                        description=f"**{item.filename}**",
                        color=0x00ff00,  # 초록색
                        fields=fields,
                        thumbnail_url=item.meta_poster
                    )
            else:
                P.logger.info(f'VOD 필터링 제외: {item.filename}')
                
            if P.ModelSetting.get_bool('vod_use_notify'):
                from tool import ToolNotify
                msg = f'봇 VOD 수신\n파일: {item.filename}\n로그: {item.log}'
                ToolNotify.send_message(msg, image_url=item.meta_poster, message_id=f"{P.package_name}_{self.name}")
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
            
            # 에러 시 디스코드 알림
            from .mod_monitor import ModuleMonitor
            ModuleMonitor.send_discord_notification(
                title="⚠️ 국내 VOD 처리 에러",
                description=f"**{filename}**\n\n에러: {str(e)}",
                color=0xff0000  # 빨간색
            )
        finally:
            item.save()


    def share_copy(self, item):
        try:
            vod_remote_path = P.ModelSetting.get('vod_remote_path')
            if vod_remote_path == '':
                return {'ret':'warning', 'msg':'리모트 경로 정보가 없습니다.'}
            try:
                import gds_tool
                PP = F.PluginManager.get_plugin_instance('gds_tool')
                if PP == None:
                    raise Exception()
            except:
                return {'ret':'warning', 'msg':'구글 드라이브 공유 플러그인이 설치되어 있지 않습니다.'}

            # copy_type='file'이므로 filename을 None으로 전달하여 폴더 생성 방지
            # 파일 ID만으로 직접 복사
            ret = PP.add_copy(item.fileid, None, 'bot_log_monitor_vod', item.meta_genre, item.size, 1, copy_type='file', remote_path=vod_remote_path)

            item.share_request_time = datetime.now()
            item.request_db_id = ret['request_db_id'] if 'request_db_id' in ret else None
            
            item.save()
            if ret['ret'] == 'success':
                # Discord 알람 전송
                from .mod_monitor import ModuleMonitor
                ModuleMonitor.send_discord_notification(
                    title='📺 국내 VOD 복사 요청',
                    description=f'**{item.meta_title}**\n파일: {item.filename}',
                    color=0x00FF00,
                    fields=[
                        {'name': '장르', 'value': item.meta_genre or '미분류', 'inline': True},
                        {'name': '크기', 'value': f'{item.size / (1024**3):.2f} GB' if item.size else '알 수 없음', 'inline': True}
                    ],
                    thumbnail_url=item.meta_poster if item.meta_poster else None
                )
                return {'ret':'success', 'msg': '요청하였습니다.'}
            elif ret['ret'] == 'remote_path_is_none':
                return {'ret':'warning', 'msg': '리모트 경로가 없습니다.'}
            elif ret['ret'] == 'already':
                return {'ret':'warning', 'msg': '이미 요청 DB에 있습니다.<br>상태: ' + ret['status']}
            elif ret['ret'] == 'cannot_access':
                return {'ret':'warning', 'msg': '권한이 없습니다.'}
            else:
                return {'ret':'warning', 'msg': '실패'}
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())


    def share_folder_copy(self, item):
        """프로그램 폴더 전체 복사"""
        try:
            P.logger.info(f'폴더 복사 요청 시작: {item.filename}')
            P.logger.debug(f'item.data 내용: {item.data}')
            
            vod_remote_path = P.ModelSetting.get('vod_remote_path')
            if vod_remote_path == '':
                return {'ret':'warning', 'msg':'리모트 경로 정보가 없습니다.'}
            try:
                import gds_tool
                PP = F.PluginManager.get_plugin_instance('gds_tool')
                if PP == None:
                    raise Exception()
            except:
                return {'ret':'warning', 'msg':'구글 드라이브 공유 플러그인이 설치되어 있지 않습니다.'}

            # item.data에서 경로 정보 추출
            gds_path = item.data.get('msg', {}).get('data', {}).get('gds_path', '')
            program_folder = item.data.get('msg', {}).get('data', {}).get('program_folder', '')
            
            P.logger.debug(f'추출된 정보 - gds_path: {gds_path}, program_folder: {program_folder}')
            
            # 경로 정보가 없으면 GDS Tool 로그에서 검색
            if not gds_path or not program_folder:
                P.logger.info(f'경로 정보 없음. GDS Tool 로그에서 검색 중: {item.filename}')
                
                # 모니터 모듈의 로그 검색 메서드 호출
                from .mod_monitor import ModuleMonitor
                result = ModuleMonitor.search_gds_path_in_log(item.filename)
                
                if result:
                    gds_path = result['gds_path']
                    program_folder = result['program_folder']
                else:
                    P.logger.warning(f'로그에서 파일을 찾을 수 없습니다: {item.filename}')
                    return {'ret':'warning', 'msg':'로그에서 파일 정보를 찾을 수 없습니다.<br>파일이 너무 오래되었거나 로그가 삭제되었을 수 있습니다.'}
            
            # 프로그램 폴더 경로 구성
            # program_folder를 gds_path에서 찾아서 그 폴더까지의 경로 구성
            # 예: gds_path = .../런닝맨 (2010) [SBS]/런닝맨.E789.mp4
            #     program_folder = 런닝맨 (2010) [SBS]
            #     folder_path = .../런닝맨 (2010) [SBS]
            if program_folder and program_folder in gds_path:
                folder_path = gds_path.split(program_folder)[0] + program_folder
            else:
                # fallback: 파일명만 제거
                P.logger.debug(f'program_folder 미검출, fallback 사용 (파일명 제거)')
                path_parts = gds_path.split('/')
                folder_path = '/'.join(path_parts[:-1])
            
            P.logger.info(f'폴더 복사 요청: {program_folder}')
            P.logger.debug(f'폴더 경로: {folder_path}')
            
            # rclone으로 폴더 ID 조회
            from .mod_monitor import ModuleMonitor
            monitor_module = P.module_list[3]  # ModuleMonitor
            folder_id = monitor_module.get_gdrive_id_from_path(folder_path, is_folder=True)
            
            if not folder_id:
                P.logger.warning(f'폴더 ID 조회 실패: {folder_path}')
                return {'ret':'warning', 'msg':'폴더 ID 조회 실패'}
            
            # rclone으로 폴더 크기와 파일 개수 조회
            folder_info = monitor_module.get_folder_info_from_path(folder_path)
            folder_size = folder_info.get('size', 0)
            folder_count = folder_info.get('count', 0)
            
            P.logger.debug(f'폴더 ID: {folder_id}, 크기: {folder_size}, 파일: {folder_count}개')
            
            # GDS Tool에 폴더 복사 요청
            ret = PP.add_copy(
                folder_id, 
                program_folder, 
                'bot_log_monitor_vod_folder', 
                item.meta_genre, 
                folder_size, 
                folder_count, 
                copy_type='folder', 
                remote_path=vod_remote_path
            )

            if ret['ret'] == 'success':
                # 디스코드 알림
                ModuleMonitor.send_discord_notification(
                    title="📁 국내 VOD 폴더 복사 요청",
                    description=f"**{program_folder}**",
                    color=0x00ff00,  # 초록색
                    fields=[
                        {"name": "폴더명", "value": program_folder, "inline": False},
                        {"name": "프로그램", "value": item.meta_title or '정보 없음', "inline": True},
                        {"name": "장르", "value": item.meta_genre or '미분류', "inline": True},
                        {"name": "크기", "value": f"{folder_size / (1024**3):.2f} GB ({folder_count}개 파일)" if folder_size else "정보 없음", "inline": False}
                    ],
                    thumbnail_url=item.meta_poster
                )
                return {'ret':'success', 'msg': f'폴더 복사 요청 완료 ({folder_count}개 파일, {folder_size / (1024**3):.2f} GB)'}
            elif ret['ret'] == 'remote_path_is_none':
                return {'ret':'warning', 'msg': '리모트 경로가 없습니다.'}
            elif ret['ret'] == 'already':
                return {'ret':'warning', 'msg': '이미 요청 DB에 있습니다.<br>상태: ' + ret['status']}
            elif ret['ret'] == 'cannot_access':
                return {'ret':'warning', 'msg': '권한이 없습니다.'}
            else:
                return {'ret':'warning', 'msg': '실패'}
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
            return {'ret':'error', 'msg': f'에러 발생: {str(e)}'}


    def condition_check_download_mode(self, item):
        try:
            vod_download_mode = P.ModelSetting.get('vod_download_mode')
            if vod_download_mode == 'none':
                return False
            if vod_download_mode == 'blacklist':
                flag_download = True
                if item.meta_title is None:
                    item.log += u'메타 정보 없음. 다운:On'
                    return flag_download
                vod_blacklist_genre = P.ModelSetting.get_list('vod_blacklist_genre', '|')
                vod_blacklist_program = P.ModelSetting.get_list('vod_blacklist_program', '|')
                if len(vod_blacklist_genre) > 0 and item.meta_genre in vod_blacklist_genre:
                    flag_download = False
                    item.log += '제외 장르. 다운:Off'
                if flag_download:
                    for program_name in vod_blacklist_program:
                        if item.meta_title.replace(' ', '').find(program_name.replace(' ', '')) != -1:
                            flag_download = False
                            item.log += '제외 프로그램. 다운:Off'
                            break
                if flag_download:
                    item.log += '블랙리스트 모드. 다운:On'
            else:
                flag_download = False
                if item.meta_title is None:
                    item.log += '메타 정보 없음. 다운:Off'
                    return flag_download
                vod_whitelist_genre = P.ModelSetting.get_list('vod_whitelist_genre', '|')
                vod_whitelist_program = P.ModelSetting.get_list('vod_whitelist_program', '|')

                if len(vod_whitelist_genre) > 0 and item.meta_genre in vod_whitelist_genre:
                    flag_download = True
                    item.log += '포함 장르. 다운:On'
                if flag_download == False:
                    for program_name in vod_whitelist_program:
                        if item.meta_title.replace(' ', '').find(program_name.replace(' ', '')) != -1:
                            flag_download = True
                            item.log += '포함 프로그램. 다운:On'
                            break
                if not flag_download:
                    item.log += '화이트리스트 모드. 다운:Off'
        except Exception as e: 
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
        return flag_download




class ModelVodItem(ModelBase):
    P = P
    __tablename__ = 'bot_log_monitor_vod_item'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    share_request_time = db.Column(db.DateTime)
    request_db_id = db.Column(db.Integer)
    share_completed_time = db.Column(db.DateTime)
    data = db.Column(db.JSON)
    fileid = db.Column(db.String)
    filename = db.Column(db.String)
    size = db.Column(db.Integer)
    filename_name = db.Column(db.String)
    filename_no = db.Column(db.Integer)
    filename_release = db.Column(db.String)
    filename_date = db.Column(db.String)
    filename_quality = db.Column(db.String)
    meta_genre = db.Column(db.String)
    meta_code = db.Column(db.String)
    meta_title = db.Column(db.String)
    meta_poster = db.Column(db.String)
    log = db.Column(db.String)

    def __init__(self):
        self.created_time = datetime.now()
        self.log = ''


    @classmethod
    def process_discord_data(cls, data):
        try:
            # 파일명으로 중복 체크만 수행 (v1.0.28 방식)
            entity = cls.get_by_filename(data['msg']['data']['f'])
            if entity is not None:
                return None
            
            # 새 항목 생성
            entity = ModelVodItem()
            entity.data = data
            entity.fileid = data['msg']['data']['id']
            entity.filename = data['msg']['data']['f']
            entity.size = data['msg']['data']['s']

            entity.filename_name = data['msg']['data']['vod']['name']
            
            # 에피소드 번호를 파일명에서 직접 추출 (E03 → 3, E90 → 90)
            import re
            episode_match = re.search(r'[._ ]E(\d{1,4})[._ ]', entity.filename, re.IGNORECASE)
            if episode_match:
                entity.filename_no = int(episode_match.group(1))
            else:
                entity.filename_no = data['msg']['data']['vod'].get('no', 0)
            
            entity.filename_release = data['msg']['data']['vod']['release']
            entity.filename_date = data['msg']['data']['vod']['date']
            entity.filename_quality = data['msg']['data']['vod']['quality']

            if data['msg']['data']['meta'] is not None:
                entity.meta_genre = data['msg']['data']['meta']['genre']
                entity.meta_code = data['msg']['data']['meta']['code']
                entity.meta_title = data['msg']['data']['meta']['title']
                entity.meta_poster = data['msg']['data']['meta']['poster']
            else:
                entity.meta_genre = '미분류'
            
            entity.save()
            return entity
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
            return None   


    @classmethod
    def get_by_filename(cls, filename):
        try:
            with F.app.app_context():
                return F.db.session.query(cls).filter_by(filename=filename).first()
        except Exception as e:
            cls.P.logger.error(f'Exception:{str(e)}')
            cls.P.logger.error(traceback.format_exc())
    
    
    @classmethod
    def get_list(cls):
        """모든 항목 조회 (마이그레이션용)"""
        try:
            with F.app.app_context():
                return F.db.session.query(cls).all()
        except Exception as e:
            cls.P.logger.error(f'Exception:{str(e)}')
            cls.P.logger.error(traceback.format_exc())
            return []


    
    @classmethod
    def make_query(cls, req, order='desc', search='', option1='all', option2='all'):
        with F.app.app_context():
            query = cls.make_query_search(F.db.session.query(cls), search, cls.filename)
            if option1 == 'request_true':
                query = query.filter(cls.share_request_time != None)
            elif option1 == 'request_false':
                query = query.filter(cls.share_request_time == None)
            
            if order == 'desc':
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)
            return query


    @classmethod
    def web_list(cls, req):
        ret = super().web_list(req)
        
        # DB 컬럼 문제로 ret가 None일 수 있음
        if ret is None:
            cls.P.logger.error('web_list 실패: DB 마이그레이션이 필요할 수 있습니다.')
            return None
        
        try:
            if F.PluginManager.get_plugin_instance('gds_tool'):
                ModelRequestItem = F.PluginManager.get_plugin_instance('gds_tool').ModelRequestItem
                for item in ret['list']:
                    if item['request_db_id'] != None:
                        req_item = ModelRequestItem.get_by_id(item['request_db_id'])
                        if req_item != None:
                            item['request_item'] = req_item.as_dict()
                        else:
                            item['request_item'] = None
        except Exception as e:
            cls.P.logger.error(f'Exception:{str(e)}')
            cls.P.logger.error(traceback.format_exc())
        return ret
