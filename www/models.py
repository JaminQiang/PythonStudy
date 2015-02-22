#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Jamin Qiang'

"""
Models for user, blog, commet.
"""

import time, uuid

from transwarp.db import next_id
from transwarp.orm import Model, StringField, BooleanField, FloatField, TextField

def next_id():
	return '%015d%s000' % (int(time.time() * 1000), uuid.uuid4().hex)

class User(Model):
	__table__ = 'users'

	id = StringField(primary_key=True, default=next_id, ddl='varchar(50)')
	email = StringField(updatable=False, ddl='carchar(50)')
	password = StringField(ddl='varchar(50)')
	admin = BooleanField()
	name = StringField(ddl='varchar(50)')
	image = StringField(ddl='varchar(500)')
	create_at = FloatField(updatable=False, default=time.time)
	# 这里的__table__，id等属性都是类的属性，不是实例的属性。从类级别上定义的属性用来描述对象和表的映射关系，而实例属性必须用__init__进行初始化。

class Blog(Model):
	__table__ = 'blogs'

	id = StringField(primary_key=True, default=next_id, ddl='varchar(50)')
	user_id = StringField(updatable=False, ddl='varchar(50)')
	user_name = StringField(ddl='varchar(50)')
	user_image = StringField(ddl='varchar(500)')
	name = StringField(ddl='varchar(50)')
	summary = StringField(ddl='varchar(200)')
	content = TextField()
	create_at = FloatField(updatable=False, default=time.time)

class Comment(Model):
	__table__ = 'comments'

	id = StringField(primary_key=True, default=next_id, ddl='varchar(50)')
	blog_id = StringField(updatable=False, ddl='varchar(50)')
	user_id = StringField(updatable=False, ddl='varchar(50)')
	user_name = StringField(ddl='varchar(50)')
	content = TextField()
	create_at = FloatField(updatable=False, default=time.time)