# -*- coding: utf-8 -*-

import cx_Oracle
from DBUtils.PooledDB import PooledDB
import time
class oracle(object):
    """数据连接对象，产生数据库连接池.
    此类中的连接采用连接池实现获取连接对象：conn = oracle.getConn()
    释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None
    def __init__(self, user="abc", pwd="123456", ip="192.168.1.16", db="orcl"):
        self.user = user
        self.pwd = pwd
        self.ip = ip
        self.db = db
        self._conn = oracle.get_onn(self)
        self._cursor = self._conn.cursor()
    def get_onn(self):
        """ 静态方法，从连接池中取出连接
        return oracle.connection
        """
        if oracle.__pool is None:
            dsn = self.ip + "/" + self.db
            __pool = PooledDB(creator=cx_Oracle, mincached=1, maxcached=20, user=self.user, password=self.pwd,dsn=dsn)
        return __pool.connection()
    def execute(self, sql):
        """执行指定sql语句"""
        self._cursor.execute(sql)  # 执行语句
        self._conn.commit()  # 然后提交
    def close(self):
        """释放连接池资源"""
        self._cursor.close()
        self._conn.close()
    def fetchone(self):
        return self._cursor.fetchone()
    def fetchall(self):
        return self._cursor.fetchall()
