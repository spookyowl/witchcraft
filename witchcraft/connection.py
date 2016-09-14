import sqlalchemy


def get_value(data, keys, default=None):
    result = None

    for k in keys:
        result = data.get(k)
        
        if result is not None:
            return result

    if result is None:
        return default


class Connection(object):
    
    def __init__(self, auth):
        url = self._build_url(auth)
        self.database_type = get_value(auth, ['type', 'db_type'], 'pgsql')
        self.engine = sqlalchemy.create_engine(url, convert_unicode=True)
        self.connection = None
        self.transaction = None

    def connect(self):
        self.connection = self.engine.connect()

    def disconnect(self):
        self.connection.close()
        self.connection = None

    def execute(self, sql_query):
        if not self.is_connected():
            self.connect()

        return self.connection.execute(sql_query)

    def begin_transaction(self):
        if not self.is_connected():
            self.connect()
        self.transaction = self.connection.begin()

    def end_transaction(self):
        self.transaction.commit()
        self.transaction = None

    def is_connected(self):
        return self.connection is not None

    def _build_url(self, auth):


        db_type = get_value(auth, ['type', 'db_type'], 'pgsql')

        if db_type == 'mysql':
            default_port = 3306

        elif db_type == 'pgsql':
            default_port = 5432

        ctx = (get_value(auth, ['user']),
               get_value(auth, ['passwd', 'password', 'pass']),
               get_value(auth, ['host', 'server'], 'localhost'),
               get_value(auth, ['port'], default_port),
               get_value(auth, ['database', 'db_name', 'database_name', 'db']))

        if db_type == 'pgsql':
            url_tpl = 'postgresql+psycopg2://%s:%s@%s:%s/%s' % ctx

        elif db_type == 'mysql':
            url_tpl = 'mysql+mysqldb://%s:%s@%s:%s/%s' % ctx

        else:
            RaiseValue('db_type must be eighter "mysql" or "pgsql"')

        return url_tpl % auth


class Session(object):

    def __init__(self, args):
        self.args = args

    def __enter__(self):
        self.connection = Connection(self.args)
        self.connection.connect()
        return self.connection

    def __exit__(self, type, value, traceback):
        self.connection.disconnect()



