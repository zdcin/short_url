# -*- coding:utf-8 -*-

# 1. 输入url,检查入库， 返回key
# 2. 输入key，返回url
# 3. 功能1批量版本
# 4. 功能2批量版本
# 5. 输入url，不入库，不检查冲突，返回key
# 6. 302跳转功能

import sys
import web
import MySQLdb
from DBUtils.PooledDB import PooledDB
from json import JSONEncoder, JSONDecoder
from log import initlog, logger
import config
from myfuntions import saveUrl, getUrl, toKey, batchSave


def is_ip_deny(thisweb):
    '''只有重定向接口不用检查'''
    # print "REMOTE_ADDR=%s" % thisweb.ctx.env.get('REMOTE_ADDR')
    # logger.info("REMOTE_ADDR=%s" % thisweb.ctx.env.get('REMOTE_ADDR'))
    if thisweb.ctx.env.get('REMOTE_ADDR') not in config.CONFIG['ip_white_set']:
        logger.error("%s is deny." % thisweb.ctx.env.get('REMOTE_ADDR'))
        return True
    else:
        return False
    # return thisweb.ctx.env.get('REMOTE_ADDR') not in config.CONFIG['ip_white_set']


class url2key:
    def GET(self):
        return self.POST()

    def POST(self):
        logger.info("woker %s" % 'url2key')
        if is_ip_deny(web):
            raise web.forbidden()
        ip = web.ctx.env.get('REMOTE_ADDR')
        url = web.input().get('url')
        logger.info('url2key IN: ip=%s url=%s' % (ip, url))
        # print "url=%s" % url
        if url is None or len(url.strip()) == 0:
            logger.error('url is %s' % 'None')
            return web.NotFound('url is null')
        key = saveUrl(url.strip())
        logger.info("url2key OUT: ip=%s key=%s" % (ip, key))
        # print "key=%s" % key
        #  TODO 构造json，返回
        return key


class redirect_to:
    def GET(self, key):
        logger.info("woker %s" % 'redirect_to')
        logger.info("key=%s" % key)
        # print "key=%s" % key
        if key is None or len(key.strip()) == 0:
            logger.error('key is %s' % 'None')
            return web.NotFound('key is null')
        url = getUrl(key.strip())
        if url is None:
            logger.error('geturl url is %s' % 'None')
            return web.NotFound(message='url not found')
        # return web.seeother(url, absolute=True)#  TODO 检查语义是否合理
        if not url.lower().startswith("http"):
            url = "http://%s" % url
        logger.info("url is %s, 302" % url)
        return web.redirect(url, '302')


class url2key_without_save:
    def GET(self):
        logger.info("woker %s" % 'url2key_without_save')
        if is_ip_deny(web):
            raise web.forbidden()
        url = web.input().get('url')
        logger.info("url=%s" % url)
        # print "url=%s" % url
        if url is None or len(url.strip()) == 0:
            logger.error("url is %s" % "None")
            return web.NotFound('url is null')
        key = toKey(url.strip())
        logger.info("key=%s" % key)
        # print "key=%s" % key
        #  TODO 构造json，返回
        return key


class key2url:
    def POST(self):
        return self.GET()

    def GET(self):
        logger.info("woker %s" % 'key2url')
        if is_ip_deny(web):
            raise web.forbidden()
        ip = web.ctx.env.get('REMOTE_ADDR')
        key = web.input().get('key')
        logger.info("key2url IN: ip=%s key=%s" % (ip, key))
        # print "key=%s" % key
        if key is None or len(key.strip()) == 0:
            logger.error('%s is None' % 'key')
            return web.NotFound('key is null')
        url = getUrl(key.strip())
        logger.info("key2url OUT: ip=%s url=%s" % (ip, url))
        # print "url=%s" % url
        #  TODO 构造json，返回
        return url


class batch_url2key:
    def POST(self):
        logger.info("woker %s" % 'batch_url2key')
        if is_ip_deny(web):
            raise web.forbidden()
        ip = web.ctx.env.get('REMOTE_ADDR')
        urls = web.input().get('urls')
        logger.info("batch_url2key IN: ip=%s urls=%s" % (ip, urls))
        # print "urls=%s" % urls
        if urls is None or len(urls.strip()) == 0:
            logger.error('%s is Null' % 'urls')
            return web.NotFound('urls is null')
        url_list = JSONDecoder().decode(urls)
        # print 'will save'
        keys = batchSave(url_list)
        logger.info("batch_url2key OUT: ip=%s keys=%s" % (ip, keys))
        # print 'save ok'
        return JSONEncoder().encode({'keys': keys})


class batch_key2url:
    def POST(self):
        return self.GET()

    def GET(self):
        logger.info("woker %s" % 'batch_key2url')
        if is_ip_deny(web):
            raise web.forbidden()
        ip = web.ctx.env.get('REMOTE_ADDR')
        keys = web.input().get('keys')
        logger.info("batch_key2url IN: ip=%s keys=%s" % (ip, keys))
        # print "keys=%s" % keys
        if keys is None or len(keys.strip()) == 0:
            logger.error('%s is Null' % 'keys')
            return web.NotFound('keys is null')
        key_list = JSONDecoder().decode(keys)

        def _mapfn(key):
            if key is None or len(key.strip()) == 0:
                logger.debug('%s is None' % 'key')
                return ''
            else:
                url = getUrl(key.strip())
                return url
        urls = map(_mapfn, key_list)
        logger.info("batch_key2url OUT: ip=%s url_list=%s" % (ip, urls))
        return JSONEncoder().encode({'urls': urls})


if __name__ == "__main__":

    reload(sys)
    sys.setdefaultencoding("utf-8")

    initlog(config.CONFIG['log_name'])
    dbc = config.CONFIG['db']
#      def __init__(self, creator,
#             mincached=0, maxcached=0,
#             maxshared=0, maxconnections=0, blocking=False,
#             maxusage=None, setsession=None, reset=True,
#             failures=None, ping=1,
#             *args, **kwargs):
    config.pool = PooledDB(creator=MySQLdb, mincached=dbc['mincached'], maxcached=dbc['maxcached'],
                           # maxshared=10, maxconnections=80, maxusage=100,
                           maxshared=10, maxconnections=80,
                           host=dbc['host'], port=dbc['port'], user=dbc['user'], passwd=dbc['passwd'],
                           db=dbc['db'], use_unicode=dbc['use_unicode'], charset=dbc['charset'])
    urls = (
        '/url2key', 'url2key',
        '/url2key_without_save', 'url2key_without_save',
        '/key2url', 'key2url',
        '/batch_url2key', 'batch_url2key',
        '/batch_key2url', 'batch_key2url',
        '/(.+)', 'redirect_to')
    web.config.debug = config.CONFIG['is_debug']
    app = web.application(urls, globals())
    app.run()
