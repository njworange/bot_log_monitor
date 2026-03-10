setting = {
    'filepath' : __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': None,
    'menu': {
        'uri': __package__,
        'name': '봇 로그 모니터',
        'list': [
            {
                'uri': 'vod',
                'name': '국내 VOD',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'list', 'name': '목록'},
                ]
            },
            {
                'uri': 'foreign_vod',
                'name': '외국 VOD',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'list', 'name': '목록'},
                ]
            },
            {
                'uri': 'share_movie',
                'name': 'S-MOVIE',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'list', 'name': '목록'},
                ]
            },
            {
                'uri': 'monitor',
                'name': '모니터',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                ]
            },
            {
                'uri': 'manual',
                'name': '매뉴얼',
                'list': [
                    {'uri':'README.md', 'name':'README.md'},
                    {'uri':'CHANGELOG.md', 'name':'CHANGELOG.md'}
                ]
            },
            {
                'uri': 'log',
                'name': '로그',
            },
        ]
    },
    'setting_menu': None,
    'default_route': 'normal',
}


from plugin import *

P = create_plugin_instance(setting)

try:
    from .mod_share_movie import ModuleShareMovie
    from .mod_vod import ModuleVod
    from .mod_foreign_vod import ModuleForeignVod
    from .mod_monitor import ModuleMonitor
    P.set_module_list([ModuleVod, ModuleForeignVod, ModuleShareMovie, ModuleMonitor])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

logger = P.logger
