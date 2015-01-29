#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Jamin Qiang'


"""
A WSGI application entry.
"""

import logging; logging.basicConfig(level=logging.INFO)

import os, time
from datetime import datetime

from transwarp import db
from transwarp.web import WSGIApplication, Jinja2TemplateEngine

from config import configs

# init db:
# 初始化数据库：
db.create_engine(**configs.db)
# 参数为config.py中的变量configs的db属性
# config.py合并了config_default和config_override，导入config_default，尝试导入config_override，如果成功，进行merge。
# configs变量最后进行了toDict的操作，为什么要进行这个操作？configs中的session的作用是？

# init wsgi app:
# 创建一个WSGIApplication:
wsgi = WSGIApplication(os.path.dirname(os.path.abspath(__file__)))
# 服务器网关接口Python Web Service Gateway Interface
# WSGI分为两部分，一个是服务器/网关，一个是应用程序/应用框架。在处理一个WSGI请求时，服务器会为应用程序提供环境资讯及一个会呼函数（callback function）。当应用程序完成处理请求后，通过回呼函数，将结果回传给服务器。
# 此处的wsgi调用了web.py的类WSGIApplication，__file__是当前文件的位置，直接在IDS输入会报错__file__ not defined。
# 此处的os.path.dirname(os.path.abspath(__file__))输出的是当前文件所处的文件夹
# 执行该代码后，生成了一个类WSGIApplication的实例wsgi

# 定义datetime_filter, 输入是t, 输出是unicode字符串：
def datetime_filter(t):
	delta = int(time.time() - t)
	if delta < 60:
		return u'1分钟前'
	if delta < 3600:
		return u'%s分钟前' % (delta // 60)
	if delta < 86400:
		return u'%s小时前' % (delta // 3600)
	if delta < 604800:
		return u'%s天前' % (delta // 86400)
	dt = datetime.fromtimestamp(t)
	return u'%s年%s月%s日' % (dt.year, dt.month, dt.day)

# 初始化jinja2模版引擎：
template_engine = Jinja2TemplateEngine(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))
# 生成一个类Jinja2TemplateEngine的实例template_engine，参数是模版文件的文件夹

# 把filter添加到jinjia2, filter名称为datetime, filter本身是一个函数对象：
template_engine.add_filter('datetime', datetime_filter)

wsgi.template_engine = template_engine

# 加载带有@get／@post的URL处理函数：
import urls

wsgi.add_interceptor(urls.user_interceptor)
# URL拦截器，用于根据URL进行权限检查
# 拦截器接受一个next()函数，这样拦截器可以根据是否返回next()来判断继续处理还是直接返回
"""
	def add_interceptor(self, func):
		self._check_not_running()
		self.__interceptors.append(func)
		logging.info('Add interceptor: %s' % str(func))
"""
# __interceptors是一个空列表，在空列表中加入urls.user_interceptor
# 在空列表里插入了，然后呢？不懂。
"""
@interceptor('/')
def user_interceptor(next):
	""
	def interceptor(pattern='/'):
		def _decorator(func):
			func.__interceptor__ = _build_pattern_fn(pattern)
			return func
		return _decorator
	""
	logging.info('try to bind user from session cookie...')
	user = None
	cookie = ctx.request.cookies.get(_COOKIE_NAME)
	if cookie:
		logging.info('parse session cookie...')
		user = parse_signed_cookie(cookie)
		if user:
			logging.info('bind user <%s> to session...' % user.email)
	ctx.request.user = user
	return next()
"""
wsgi.add_interceptor(urls.manage_interceptor)
"""
	""
	def interceptor(pattern='/'):
		def _decorator(func):
			func.__interceptor__ = _build_pattern_fn(pattern)
			return func
		return _decorator
	""
	""
	def _build_pattern_fn(pattern):
		m = _RE_INTERCEPTROR_STARTS_WITH.match(pattern)
		if m:
			return lambda p: p.startswith(m.group(1))
		m = _RE_INTERCEPTROR_ENDS_WITH.match(pattern)
		if m:
			return lambda p: p.endswith(m.group(1))
		raise ValueError('Invalid pattern definition in interceptor.')
	""
@interceptor('/manage/')
def manage_interceptor(next):
	user = ctx.request.user
	if user and user.admin:
		return next()
	raise seeother('/signin')
"""
# 这里的装饰器里的__interceptor__和_build_pattern_fn不是很懂。
# _build_pattern_fn中的返回了lambda，是返回了一个函数？
wsgi.add_module(urls)
"""
	def add_module(self, mod):
		self._check_not_running()
		m = mod if type(mod)==types.ModuleType else _load_module(mod)
		logging.info('Add module: %s' % m.__name__)
		for name in dir(m):
			fn = getattr(m, name)
			if callable(fn) and hasattr(fn, '__web_route__') and hasattr(fn, '__web_method__'):
				self.add_url(fn)
"""
# 检查输入参数是否是模块，对于模块内的所有函数，检查其是否包含__web_route__属性与__web_method__属性，如果包含，执行add_url函数
# 

# 在9000端口上启动本地测试服务器：
if __name__ == '__main__':
	wsgi.run(9000, host='0.0.0.0')