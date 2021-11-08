import psycopg2 as db


class Configuracao():
    def __init__(self):
        self.config = {
            "default": {
                'user': 'root',
                'password': 'seal@2020#',
                'host': 'localhost',
                'port': '5432',
                'database': 'seal2', 
            }
        }

class Connection(Configuracao):
    def __init__(self):
        Configuracao.__init__(self)
        
        try:
            self.conn = db.connect(**self.config['default'])
            self.cur = self.conn.cursor()
        except Exception as e:
            print('Erro na conexão de modulos.')
            exit(1)
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self.conn
    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.connection.commit()

    def fecthall(self):
        return self.cursor.fetchall()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fecthall()

class Querys(Connection):
    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

    def insert(self, table, fields, values):
        try:
            
            sql = f"INSERT INTO {table} ({fields}) VALUES {values};"
            print(sql)
            self.execute(sql)
            print(self.commit())

            return {'Success': 'The values was insert into table abastecimento'}
        except Exception as e:
            self.execute('rollback;')
            self.commit()
            print(e)
            return {'erro ao inserir!': e}

    def select(self, table, fields, operador=None, *args):
        try:
            condicionais = []
            for arg in args:
                condicionais.append(arg)
    
            if len(condicionais) == 0:
                sql = f"select {fields} from {table};"
            else:
                condition = f' {operador} '.join(condicionais)
                sql = f"select {fields} from {table} where {condition};"
            
            self.execute(sql)
            self.commit()
            
        except Exception as e:
    
            self.execute('rollback;')
            self.commit()

            print(e)
    
    def rollback(self):
        self.execute('rollback;')
        self.commit()