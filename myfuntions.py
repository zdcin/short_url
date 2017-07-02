# -*- coding:utf-8 -*-
'''
Created on 2015年11月18日

@author: leo
'''
import hashlib
import config
from log import logger
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Semaphore

# Table中内容是 [0-9, A-Z, a-z]，共62个字符
CHAR_TABLE = list('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')

# 控制连接池使用的信号量, 跟最大连接数一样，每当需要使用db 连接的时候都要使用信号量控制，可以批量控制分配连接
DB_POOL_SEMAPHORE = Semaphore(config.CONFIG['db']['maxcached'])


class cursor_warpper:
    '''对数据库资源的with封装'''
    def __init__(self, dbpool):
        self._conn = dbpool.connection()
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None or exc_val is not None or exc_tb is not None:
            print exc_type, exc_val, exc_tb
        if self._conn is not None:
            self._conn.commit()
        if self._cursor is not None:
            self._cursor.close()
        if self._conn is not None:
            self._conn.close()


# 表名生成器
def table_name_creator():
    global CHAR_TABLE
    dic = {}
    for i in range(len(CHAR_TABLE)):
        dic[CHAR_TABLE[i]] = i

    def get_table_name(key):
        return "short_urls_%d" % dic[key[0]]
    return get_table_name

get_table_name_fn = table_name_creator()

table_index_list = list(CHAR_TABLE)


def __saveSome(para):
    index, list2 = para[0], para[1]

    # 1. 定义写入方法，内部用
    def __try_save(list3):
        with cursor_warpper(config.pool) as cursor:
            # sql 必须能够防止注入攻击
            affect_row_num = cursor.executemany("insert ignore into " + get_table_name_fn(index) + " (s,l) values (%s, %s)",
                                                map(lambda url: (toKey(url), url), list3))
            return affect_row_num == len(list3)
    # 2. 尝试写入，并检查是否成功, 成功的话返回key
    if __try_save(list2):
        logger.info("insert %d items ok" % len(list2))
        # print "insert %d items ok" % len(list2)
        return map(lambda url: (url, toKey(url)), list2)
    else:
        # print list2
        logger.info('list2: %s, try save failed in batchSave!' % list2)
        # print 'try save failed in batchSave!!!!!'
        return map(lambda url: (url, saveUrl(url)), list2)


def batchSave(url_list):
    global CHAR_TABLE, DB_POOL_SEMAPHORE
    # 处理url 为空的情况
    no_empty_url_list = filter(lambda url: url is not None and len(url.strip()) > 0,
                               list(set(url_list)))
    url_list_dict = map(lambda index: (index,
                                       filter(lambda url: toKey(url).startswith(index),
                                              no_empty_url_list)),
                        CHAR_TABLE)
    url_list_dict = filter(lambda (x, y): len(y) > 0, url_list_dict)
    results = {}
    if len(url_list_dict) > 0:
        try:
            DB_POOL_SEMAPHORE.acquire(len(url_list_dict))

            pool = ThreadPool(len(url_list_dict))
            # Open the urls in their own threads
            # and return the results
            r = pool.map(__saveSome, url_list_dict)
            # close the pool and wait for the work to finish
            pool.close()
            pool.join()
            for list_of_tuple in r:
                for tuplev in list_of_tuple:
                    results[tuplev[0]] = tuplev[1]
        finally:
            for _ in range(len(url_list_dict)):
                DB_POOL_SEMAPHORE.release()

    # 处理url 为空的情况
    def _mapfn(url):
        if url is None or len(url.strip()) == 0 or url not in results:
            return ''
        else:
            return results[url]
    returv = map(_mapfn, url_list)
    return returv


# 写入mysql，返回key，如果冲突过多，可能返回None
def saveUrl(url):
    global get_table_name_fn, DB_POOL_SEMAPHORE

    # 1. 定义写入方法，内部用
    def try_save(key, url):
        with cursor_warpper(config.pool) as cursor:
            # sql 必须能够防止注入攻击
            affect_row_num = cursor.execute("insert ignore into " + get_table_name_fn(key) + " (s,l) values (%s, %s)",
                                            (key, url))
            return affect_row_num == 1

    # 2. 获取key
    key = toKey(url)
    # 3. 尝试写入，并检查是否成功, 成功的话返回key
    is_save_ok = False
    try:
        DB_POOL_SEMAPHORE.acquire()
        is_save_ok = try_save(key, url)
    finally:
        DB_POOL_SEMAPHORE.release()
    if is_save_ok:
        return key

    # 4. 写入失败，可能是已经存在该url了
    try:
        DB_POOL_SEMAPHORE.acquire()
        with cursor_warpper(config.pool) as cursor:
            affect_row_num = cursor.execute("select s from " + get_table_name_fn(key) + " where s like %s and l=%s",
                                            (key + "%", url))
            if affect_row_num >= 1:
                r = cursor.fetchone()
                key = r[0]
                return key
    finally:
        DB_POOL_SEMAPHORE.release()
    # n = cursor.insert('short_urls', s=key, l=url)
    # 4. 冲突的话，让key 逻辑自增1，再次尝试写入，做多尝试5次
    try_num = 5
    for _ in range(try_num):
        key = getNextKey(key)
        is_save_ok = False
        try:
            DB_POOL_SEMAPHORE.acquire()
            is_save_ok = try_save(key, url)
        finally:
            DB_POOL_SEMAPHORE.release()
        if is_save_ok:
            return key
    # 5. 最终返回
    return None


# 从数据库中查询 TODO，添加redis支持
def getUrl(key):
    global get_table_name_fn, DB_POOL_SEMAPHORE
    # 1. 从redis中查询，看是否存在，如存在，返回
    # 2. 从数据库中查询，看是否存在，如果存在，写入reids，并返回
    try:
        DB_POOL_SEMAPHORE.acquire()
        with cursor_warpper(config.pool) as cursor:
            # sql 必须能够防止注入攻击, 参数必须后边加上逗号，才认为是tuple
            affect_row_num = cursor.execute("select l from " + get_table_name_fn(key) + " where s= %s",
                                            (key,))
            if affect_row_num > 0:
                r = cursor.fetchone()
                url = r[0]
                return url
    finally:
        DB_POOL_SEMAPHORE.release()

    # 3. 如果都不存在，返回 None
    return None


# 用算法讲url缩短
# 算法，加盐，对url取32位md5，取前30位，均分为6段， 每段为5个16进制数，转成int类型，
# 对62取余，对应到字母表，依次将取出的字符拼接，得到结果
def toKey(url):
    global CHAR_TABLE
    m2 = hashlib.md5()
    m2.update('%s%s' % (url, config.CONFIG['md5_salt']))
    md5_32 = m2.hexdigest()
#     md5_30 = md5_32[0:30]
    key = list()
    index = 0
#     for _ in range(6):
#         subs = md5_30[index:index + 5]
#         index = index + 5
#         intv = int(subs, 16)
#         key.append(CHAR_TABLE[intv % len(CHAR_TABLE)])
    for _ in range(8):
        subs = md5_32[index:index + 4]
        index = index + 4
        intv = int(subs, 16)
        key.append(CHAR_TABLE[intv % len(CHAR_TABLE)])

    return "".join(key)


# 冲突是使用， 让当前key 逻辑加一
def getNextKey(key):
    return key + '_'
