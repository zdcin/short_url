# -*- coding:utf-8 -*-
'''
Created on 2015年11月18日

@author: leo
'''
pool = None
__conf = {'test': {'is_debug': True,
                   'listen_port': 8888,
                   'db': {'mincached': 1, 'maxcached': 62, 'host': '',
                          'port': , 'user': '', 'passwd': '',
                          'db': 'UDAS', 'use_unicode': False, 'charset': "utf8"
                          },
                   'redis': {},
                   'md5_salt': '123456',
                   'max_log_size': 100 * 1024 * 1024,
                   'log_num': 5,
                   'log_name': 'log/short_url.log',
                   'ip_white_set': set(['localhost'
                                        ]),
                   },
          'product': {'is_debug': True,
                      'listen_port': 8888,
                      'db': {'mincached': 62, 'maxcached': 80, 'host': '',
                             'port': 3306, 'user': '', 'passwd': '',
                             'db': 'shorturl', 'use_unicode': False, 'charset': "utf8"
                             },
                      'redis': {},
                      'md5_salt': '123456',
                      'max_log_size': 100 * 1024 * 1024,
                      'log_num': 5,
                      'log_name': 'log/short_url.log',
                      'ip_white_set': set(['localhost']),
                      }}

CONFIG = __conf['product']
