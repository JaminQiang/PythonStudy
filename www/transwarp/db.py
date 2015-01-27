#!usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Jamin Qiang'

'''
Database operation module.
'''

import time,uuid,functools,threading,logging

# Dict object:

class Dict(dict):
	"""
	Simple dict but support access as x.y style.

	>>> d1 = Dict()
	>>> d1['x'] = 100
	>>> d1.x
	100
	>>> d1.y = 200
	>>> d1['y']
	200
	>>> d2 = Dict(a=1,b=2,c='3')
	>>> d2.c
	'3'
	>>> d2['empty']
	Traceback (most recent call last):
		...
	KeyError: 'empty'
	>>> d2.empty
	Traceback (most recent call last):
		...
	AttributeError: 'Dict' object has no attribute 'empty'
	>>> d3 = Dict(('a','b','c'),(1,2,3))
	>>> d3.a
	1
	>>> d3.b
	2
	>>> d3.c
	3
	"""
	def __init__(self,names=(),values=(),**kw):
	# super is used to find the father of Dict, which is dict, and execute __init__(dict) 
	# super函数用于将父类的__init__函数直接作用于子类
	# **kw关键字参数，可以传入任意个含参数名的参数，这些参数在函数内部被组装为一个dict
		super(Dict, self).__init__(**kw)
		for k,v in zip(names,values):
			self[k] = v
	
	def __getattr__(self,key):
	# 用于取出Dict实例的属性值，内建函数，通过此函数实现了self.key直接访问属性值
	# 前后加双下划线__xx__表示特殊函数
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%'" % key)

	def __setattr__(self,key,value):
	# 用于设置Dict实例的属性，内建函数
		self[key] = value

def next_id(t=None):
	"""
	Return next id as 50-char string.

	Args:
		t: unix timestamp, default to None and using time.time().
	"""
	# next_id用于mySQL，具体作用？
	# uuid库，用于生成唯一的id，hex十六进制
	# time.time()用于生成时间戳
	if t is None:
		t = time.time()
	return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

def _profiling(start,sql=''):
	# _profiling函数用于mySQL
	# 函数前加下划线表示私有函数
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' % (t,sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' % (t,sql))

class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass

class _LasyConnection(object):
	# 延迟连接，用于减少长连接数，当有调用发起时，再创建长连接
	# 打开游标时，才触发连接，并调用cursor/commit/rollback等函数

	def __init__(self):
		self.connection = None

	def cursor(self):
	# 游标函数，如果打开游标时连接为空，建立连接为engine.connect()，返回游标值
		if self.connection is None:
			connection = engine.connect()
			logging.info('open connection <%s>...' % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()

	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info('close connection <%s>...' % hex(id(connection)))
			connection.close()

class _DbCtx(threading.local):
	"""
	Thread local object that holds connection info.
	"""
	# 进程相关内容？
	# DbCtx：database context 数据库上下文？
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None

	def init(self):
		logging.info('open lazy connection...')
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection = None

	def cursor(self):
		"""
		Return cursor
		"""
		return self.connection.cursor()

# thread-local db context:
_db_ctx = _DbCtx()
# threadlocal对象，持有的数据库连接对每个线程看到的是不一样的，任何一个线程都无法访问其他线程的数据

# global engine object:
engine = None
# 全局变量engine，每个需要调用的函数都会声明global engine
# 如果engine为空，通过create_engine函数，创建_Engine类的实例engine，并将参数params赋值至engine
#

class _Engine(object):
	"""

	"""
	def __init__(self, connect):
		self._connect = connect

	def connect(self):
		return self._connect()

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
	# 初始化数据库连接信息，初始化之后即可执行数据库操作。
	import mysql.connector
	# 导入SQL驱动
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized.')
	params = dict(user=user, password=password, database=database, host=host, port=port)
	# params 取值为create_engine函数的输入参数
	defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
	# defaults 包括四个参数，都赋予了默认值
	for k, v in defaults.iteritems():
		params[k] = kw.pop(k, v)
	# 将defaults中的四个关键值取出，如果在kw中有赋值，按kw中的值赋予params，若kw中无赋值，按defaults的值赋予params
	params.update(kw)
	# 将kw中所有未插入params的关键值全部插入params
	params['buffered'] = True
	# params 中再插入一个关键值
	engine = _Engine(lambda: mysql.connector.connect(**params))
	# 以params中的关键值为参数建立连接
	# test connection...
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _ConnectionCtx(object):
	# 自动获取和释放连接
	"""
	_ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most outer connection has effect.
	
	with connection():
		pass
		with connection():
			pass
	"""
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	"""
	Return _ConnectionCtx object that can be used by 'with' statement:

	with connection():
		pass
	"""
	return _ConnectionCtx()

def with_connection(func):
	# 装饰器，使__enter__()和__exit__()的对象可以用于with
	"""
	Decorator for reuse connection.

	@with_connection
	def foo(*args, **kw):
		f1()
		f2()
		f3()
	"""
	@functools.wraps(func)
	def _wrapper(*args, **kw):
		with _ConnectionCtx():
			return func(*args, **kw)
	return _wrapper

class _TranscationCtx(object):
	"""
	_TranscationCtx object that can handle transactions.

	with _TranscationCtx():
		pass
	"""
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			# needs open a connection first:
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions +1
		logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions -1
		try:
			if _db_ctx.transactions==0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()
		
	def commit(self):
		global _db_ctx
		logging.info('commit transaction...')
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed. try rollback...')
			_db_ctx.connection.rollback()
			logging.warning('rollback ok.')
			raise

	def rollback(self):
		global _db_ctx
		logging.warning('rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollback ok.')

def transaction():
	"""
	Create a transaction object so can use with statement:

	with transaction():
		pass

	>>> def update_profile(id, name, rollback):
	... 	u = dict(id=id, name=name, email='%s@test.org'% name, passwd=name, last_modified=time.time())
	... 	insert('user', **u)
	... 	r = update('update user set passwd=? where id=?', name.upper(), id)
	... 	if rollback:
	... 		raise StandardError('will cause rollback...')
	>>> with transaction():
	... 	update_profile(900301, 'Python', False)
	>>> select_one('select * from user where id=?',900301).name
	u'Python'
	>>> with transaction():
	... 	update_profile(900302, 'Ruby', True)
	Traceback (most recent call last):
	 ...
	StandardError: will cause rollback...
	>>> select('select * from user where id=?', 900302)
	[]
	"""
	return _TranscationCtx()

def with_transaction(func):
	"""
	A decorator that makes function around transaction.

	>>> @with_transaction
	... def update_profile(id, name, rollback):
	... 	u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
	... 	insert('user', **u)
	... 	r = update('update user set passwd=? where id=?', name.upper(), id)
	... 	if rollback:
	... 		raise StandardError('will cause rollback...')
	>>> update_profile(8080, 'Julia', False)
	>>> select_one('select * from user where id=?', 8080).passwd
	u'JULIA'
	>>> update_profile(9090, 'Robert', True)
	Traceback (most recent call last):
		...
	StandardError: will cause rollback...
	>>> select('select * from user where id=?', 9090)
	[]
	"""
	@functools.wraps(func)
	def _wrapper(*args, **kw):
		_start = time.time()
		with _TranscationCtx():
			return func(*args, **kw)
		_profiling(_start)
	return _wrapper

def _select(sql, first, *args):
	' execute select SQL and return unique result or list results.'
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL: %s, ARGS: %s' % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		# 打开游标
		cursor.execute(sql, args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not values:
				return None
			return Dict(names, values)
		return [Dict(names, x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql, *args):
	"""
	Execute select SQL and expected one result.
	If no result found, return None.
	If multiple results found, the first one returned.
	"""
	return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
	"""
	Execute select SQL and expected one int and only one int result.
	"""
	d = _select(sql, True, *args)
	if len(d)!=1:
		raise MultiColumnsError('Except only one column.')
	return d.values()[0]

@with_connection
def select(sql, *args):
	"""
	Execute select SQL and return list or empty list if no result.
	"""
	return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
	# 函数名前加单个下划线_表示此函数为私有函数，但不是真正的私有访问权限
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL: %s, ARGS:%s' % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args)
		r = cursor.rowcount
		if _db_ctx.transactions==0:
			# no transaction enviroment:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def insert(table, **kw):
	"""
	Execute insert SQL.
	"""
	cols, args = zip(*kw.iteritems())
	sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
	return _update(sql, *args)

def update(sql, *args):
	# 直接调用_update函数
	"""
	Execute update SQL.

	>>> u1 = dict(id=1000, name='Jamin', email='qxm96731@gamil.com',passwd='123456', last_modified=time.time())
	>>> insert('user',**u1)
	1
	>>> u2 = select_one('select * from user where id=?',1000)
	>>> u2.email
	u'qxm96731@gmail.com'
	>>> u2.passwd
	u'123456'
	>>> update('update user set email=?, passwd=? where id=?','qxm96731@gmail.com','123456',1000)
	1
	>>> u3 = select_one('select * from where id=?',1000)
	>>> u3.email
	u'qxm96731@gmail.com'
	>>> u3.passwd
	u'123456'
	>>> update('update user set passwd=? where id=?','***','123\' or id=\'456')
	0
	"""
	return _update(sql, *args)

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('www-data', 'www-data', 'test')
	update('drop table if exists user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
	import doctest
	doctest.testmod()

		
		
		


