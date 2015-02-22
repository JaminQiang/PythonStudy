#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Jamin Qiang'

# ORM: Object Relational Mapping. 对象映射关系。
# 用于实现面向对象编程语言里不同类型系统的数据之间的转换。创建了一个可在编程语言中使用的“虚拟对象数据库”
# 面向对象时从软件工程基本原则（如耦合、聚合、封装）的基础上发展起来，关系数据库时从数学理论发展而来，两套理论存在显著的区别。对象关系映射技术就是为了解决这个不匹配现象。

"""
Database operation module. This module is independent with web module.
"""

import time, logging

import db

class Field(object):

	_count = 0

	def __init__(self, **kw):
		self.name = kw.get('name', None)
		self._default = kw.get('default', None)
		self.primary_key = kw.get('primary_key', False)
		self.nullable = kw.get('nullable', True)
		self.updatable = kw.get('updatable', True)
		self.insertable = kw.get('insertable', True)
		self.ddl = kw.get('ddl', '')
		self._order = Field._count
		Field._count = Field._count + 1

	@property
	def default(self):
		d = self._default
		return d() if callable(d) else d

	def __str__(self):
		s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
		self.nullable and s.append('N')
		self.updatable and s.append('U')
		self.insertable and s.append('I')
		s.append('>')
		return ''.join(s)

class StringField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'varchar(255)'
		super(StringField, self).__init__(**kw)

class IntegerField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = 0
		if not 'ddl' in kw:
			kw['ddl'] = 'bigint'
		super(IntegerField, self).__init__(**kw)

class FloatField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = 0.0
		if not 'ddl' in kw:
			kw['ddl'] = 'real'
		super(FloatField, self).__init__(**kw)

class BooleanField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = False
		if not 'ddl' in kw:
			kw['ddl'] = 'bool'
		super(BooleanField, self).__init__(**kw)

class TextField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'text'
		super(TextField, self).__init__(**kw)

class BlobField(Field):
	"""
	"""

	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'bolb'
		super(BlobField, self).__init__(**kw)

class VersionField(Field):
	"""
	"""

	def __init__(self, name=None):
		super(VersionField, self).__init__(name=name, default=0, ddl='bigint')

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def _gen_sql(table_name, mappings):
	# 生成数据库表结构？
	pk = None
	sql = ['-- generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
	for f in sorted(mappings.values(), lambda x,y: cmp(x._order, y._order)):
		# sorted中的是一个匿名函数lambda？？意义何在？？这是sorted函数的参数cmp，用于调整排序逻辑。
		# 在对mappings.values()进行排序时，按照每个值的._order的顺序排序。
		if not hasattr(f, 'ddl'):
			raise StandardError('no ddl in field "%s".' % n)
		ddl = f.ddl
		nullable = f.nullable
		if f.primary_key:
			pk = f.name
		sql.append(nullable and ' `%s` %s,' % (f.name, ddl) or ' `%s` %s not null,' % (f.name, ddl))
	sql.append(' primary key(`%s`)' % pk)
	sql.append(');')
	return '\n',join(sql)

class ModelMetaclass(type):
	"""
	Metaclass for model objects.
	"""
	def __new__(cls, name, bases, attrs):
		# skip base Model class:
		if name=='Model':
			return type.__new__(cls, name, bases, attrs)

		# store all subclasses info:
		if not hasattr(cls, 'subclasses'):
			cls.subclasses = {}
		if not name in cls.subclasses:
			cls.subclasses[name] = name
		else:
			logging.warning('Redefine class: %s' % name)

		logging.info('Scan ORMapping %s...' % name)
		mappings = dict()
		primary_key = None
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				if not v.name:
					v.name = k
				logging.info('Found mapping: %s => %s' % (k, v))
				# check duplicate primary key:
				if v.primary_key:
					if primary_key:
						raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
					if v.updatable:
						logging.warning('NOTE: change primary key to non-updatbale.')
						v.updatable = False
					if v.nullable:
						logging.warning('NOTE: change primary key to non-nullable.')
						v.nullable = False
					primary_key = v
				mappings[k] = v
		# check exist of primary key:
		if not primary_key:
			raise TypeError('Primary key not defines in class: %s' % name)
		for k in mappings.iterkeys():
			attrs.pop(k)
		if not '__table__' in attrs:
			attrs['__table__'] = name.lower()
		attrs['__mappings__'] = mappings
		attrs['__primary_key__'] = primary_key
		attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
		for trigger in _triggers:
			if not trigger in attrs:
				attrs[trigger] = None
		return type.__new__(cls, name, bases, attrs)

class Model(dict):
	"""
	Base class for ORM.


	"""
	__metaclass__ = ModelMetaclass
	# 定义这个类的属性的目的是？用于读取具体的映射关系？

	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	@classmethod
	# classmethod装饰器可以使该方法不用实例化，直接调用。
	# 可以直接通过类方法实现主键查找，不需要实例化
	def get(cls, pk):
		"""
		Get by primary key.
		"""
		d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
		return cls(**d) if d else None

	@classmethod
	def find_first(cls, where, *args):
		"""
		Find by where clause and return one result.
		"""
		d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
		return cls(**d) if d else None

	@classmethod
	def find_all(cls, *args):
		"""
		Find all and return list.
		"""
		L = db.select('select * from `%s`' % cls.__table__)
		return [cls(**d) for d in L]

	@classmethod
	def find_by(cls, where, *args):
		"""
		Find by where clause and return list.
		"""
		L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
		return [cls(**d) for d in L]

	@classmethod
	def count_all(cls):
		"""
		Find by 'select count(pk) from table' and return integer.
		"""
		return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

	@classmethod
	def count_by(cls, where, *args):
		"""
		Find by 'select count(pk) from table where ...' and return int.
		"""
		return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

	def update(self):
		self.pre_update and self.pre_update()
		L = []
		args = []
		for k, v in self.__mappings__.iteritems():
			if v.updatable:
				if hasattr(self, k):
					arg = getattr(self, k)
				else:
					arg = v.default
					setattr(self, k, arg)
				L.append('`%s`=?' % k)
				args.append(arg)
		pk = self.__primary_key__.name
		args.append(getattr(self, pk))
		db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
		return self

	def delete(self):
		self.pre_delete and self.pre_delete()
		pk = self.__primary_key__.name
		args = (getattr(self, pk), )
		db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)

	def insert(self):
		# 实例方法，实例化后调用方法进行处理。
		self.pre_insert and self.pre_insert()
		params = {}
		for k, v in self.__mappings__.iteritems():
			if v.insertable:
				if not hasattr(self, k):
					setattr(self, k, v.default)
				params[v.name] = getattr(self, k)
		db.insert('%s' % self.__table__, **params)
		return self

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)
	db.create_engine('www-data', 'www-data', 'test')
	db.update('drop table if exists user')
	db.update('creata table user (id int primary key, name text, email text, passwd test, last_modified real')
	import doctest
	doctest.testmod()








	