# --------------------------------database connection pool-------------------------------
# 我们需要创建一个全局的连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。
# 使用连接池的好处是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用。
# 连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务：
import logging; logging.basicConfig(level=logging.INFO)
import logging
import aiomysql
# create a pool to make it possible for every http request directly to get data from database

async def create_pool(loop, **kw):
    # kw get a dict contain information about access request to the database
    logging.info('create database connection pool')
    # the pool is a global variation
    global __pool
    __pool = await aiomysql.create_pool(
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        # mysql 里设置的对database：webdata 有访问权限的账户的信息
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),  # charset 字符集
        autocommit=kw.get('autocommit', True),
        loop=loop
    )


# 访问数据库需要创建数据库连接、游标对象，然后执行SQL语句，最后处理异常，清理资源。
# 这些访问数据库的代码如果分散到各个函数中，势必无法维护，也不利于代码复用。
# 所以，我们要首先把常用的SELECT、INSERT、UPDATE和DELETE操作用函数封装起来。
def log(sql, args=()):
    logging.info('SQL: %s' % sql)
    if args != ():
        logging.info('Args: %s', str(args))


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# SELECT function
async def select(sql, args, size=None):
    # sql: sql instruction
    # args: things you wanna select

    log(sql)
    global __pool

    # use pool to make a connection
    # while meeting io operation during Asynchronous coding, use await
    with (await __pool) as connection:
        # use DictCursor method to build a asynchronous cursor which return result as a dictionary
        # use await while meeting asynchronous IO operation
        cursor = await connection.cursor(aiomysql.DictCursor)
        await cursor.execute(sql.replace('?', '%s'), args or ())

        # 如果传入size参数，就通过fetchmany()获取最多指定数量的记录
        # 否则，通过fetchall() 获取所有记录。
        if size:
            result = await cursor.fetchmany(size)
        else:
            result = await cursor.fetchall()

        await cursor.close()
        logging.info('rows returned: %s' % len(result))
        return result


# Insert, Update, Delete
# 要执行INSERT、UPDATE、DELETE语句，可以定义一个通用的execute()函数
# 因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数

async def execute(sql, args, autocommit=True):
    log(sql)
    with (await __pool) as connection:
        if not autocommit:
            await connection.begin()
        try:
            cursor = await connection.cursor()
            await cursor.execute(sql.replace('?', '%s'), args)
            affected = cursor.rowcount
            await cursor.close()
            if not autocommit:
                await connection.commit()
        except BaseException:
            # if something goes wrong rollback to the last version
            if not autocommit:
                await connection.rollback()
            raise
        return affected


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, column_type='varchar(100)'):
        # ??????????????? why super can't add arguments???
        super().__init__(name, column_type, primary_key, default)


class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # if you didn't set __table__ when you new a class it will assign the name of your class to __tabe;__
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table:%s)' % (name, tableName))

        # acquire all fields and primary_key
        mappings = dict()
        fields = []
        primaryKey = None

        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeRttot('Duplicate primary key for field: %s' % k)
                    # pk 记录主键
                    primaryKey = k
                else:
                    # field 储存所有不是主键的field的key值
                    fields.append(k)

        if not primaryKey:
            raise RuntimeError('Primary key not found.')

        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda field: '`%s`' % field, fields))

        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields

        # method to create sql statements
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = \
            'insert into `%s` (%s, `%s`) values (%s)' \
            % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = \
            'update `%s` set %s where `%s`=?' \
            % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    # make self[key] equal to self.key
    def __getattr__(self, key):
        try:
            return self[key]
        except:
            raise AttributeError(r"'Model' object doesn't have attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 该装饰器用于添加可以让所有子类调用的method
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        # find objects by where clause.
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self, **kwargs):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)