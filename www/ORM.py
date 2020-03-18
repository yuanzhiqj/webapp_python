#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#Object Relational Mapping
#有了这个映射关系，我们再操作程序中对象就相当于操作数据库

import asyncio, logging

import aiomysql

#将我们所需要了解的信息打印到log中，便于调试
def log(sql, args=()):                
    logging.info('SQL: %s' % sql)

@asyncio.coroutine
async def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    #连接池 缺省情况下将编码设置为utf8,自动提交事务
    global __pool   
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset','utf8'),
        autocommit = kw.get('charset','utf8'),
        maxsize = kw.get('maxsize',10),
        minsize = kw.get('minsize',1),
        loop = loop
    )

#传入SQL语句和SQL参数
#SQL语句的占位符使？,而MySQL的占位符是%s
#每个元素都是一个tuple，对于一行记录
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    #with (yield from __pool) as conn:
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
        #用参数替换而非字符串拼接可以防止sql注入
        #cur = yield from conn.cursor(aiomysql.DictCursor)
            await cur.execute(sql.replace('?','%s'),args or ())
            #如果传入size参数，就通过fetchmany()获取最多指定数量的记录，否则就通过fetchall()获取所有记录
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            #yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

#要执行INSERT、UPDATE、DELETE语句,可以定义一个通用的execute()函数
#因为这3种的执行都需要相同的参数，以及返回一个整数表示影响的行数
#execute()函数和select()函数所不同的是，cursor对象不返回结果集，而是通过rowcount返回结果数。
async def execute(sql,args,autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        finally:
            conn.close()
        return affected

class Field(object):
    def __init__(self,name,column_type,primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
    def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        #排除Model类本身：
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        #获取table名称
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        #获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k,v))
                mappings[k] = v
                if v.primary_key:
                    #找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings #保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey #主键属性名
        attrs['__field__'] = fields #除主键外的属性名
        #构造默认的SELECT,INSERT,UPDATE,DELETE语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

 class Model(dict, metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribure '%s'" % key)

    def __setattr__(self,key,value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)
    
    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,str(value)))
                setattr(self,key,value)
        return value

    @classmethod
    async def find(cls,pk):
        ' find object by primary key'
        rs = await select('%s where `%s`=?' % (cls.__select__,cls.primary_key__),[pk],1)
        if(len(rs) == 0):
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)
user['id']
