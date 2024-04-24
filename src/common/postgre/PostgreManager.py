import re
import psycopg2
# from tabulate import tabulate
from src.common.tools.logging_class import LoggerObj
from src.common.tools.library import get_exception, print_exception


class PostgreManager:
    @staticmethod
    def __build_insert_into(table, attributes, values):
        query = 'INSERT INTO ' + table + ' ('
        query += ','.join(attributes)
        query += ') VALUES ('
        query += ','.join(['\'' + str(value) + '\'' for value in values])
        query += ');'
        return query

    def __init__(self, db_url, caller="", ext_logger=None, logger_level="DEBUG", logging_queue=None):
        self.name = '{}PostgreManager'.format(caller)
        self.caller = caller
        self.db_url = db_url

        # __ init logging __
        self.logger = self.__init_logger(logger_level, logging_queue) if not ext_logger else ext_logger

    # __ Logger __
    def __init_logger(self, logger_level, logging_queue):
        logger = LoggerObj(self.name, logger_level)
        logger.add_stream_handler("DEBUG")
        if logging_queue:
            logger.add_queue_handler(logging_queue, 'WARNING')

        return logger

    def set_logger_stream(self, level):
        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.stream_class()]
        for handler in handlers:
            handler.setLevel(level)

    def set_logger_queue(self, level):
        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.queue_class()]
        for handler in handlers:
            handler.setLevel(level)

    def __log_exception(self):
        self.logger.error(get_exception()) if self.logger else print_exception()
        return None

    # __ Connection __
    def __connect(self, sslmode='require'):
        try:
            self.connection = psycopg2.connect(self.db_url, sslmode=sslmode)
            self.cursor = self.connection.cursor()

            # __ get PostgreSQL version __
            self.cursor.execute("SELECT version();")
            self.version = self.cursor.fetchone()
            self.logger.info("PostgreDB connection established. Version: {}".format(self.version))
            return True

        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Could not connect to postgreDB")
            return self.__log_exception()

    def connect(self, sslmode='require'):
        return self.__connect(sslmode=sslmode)

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.logger.info("PostgreSQL connection closed")

    def connection_properties(self):
        print(self.connection.get_dsn_parameters(), "\n")
        print(self.connection.encoding)
        print("You are connected to - ", self.version, "\n")

    def get_tables(self):
        self.cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        return self.cursor.fetchall()

    # TODO: Add get_total_number_of_rows

    def commit(self):
        self.connection.commit()
        self.logger.debug('COMMIT')

    def rollback(self):
        self.connection.rollback()
        self.logger.debug('ROLLBACK')

    # __ Queries __
    def __execute_query(self, query, values=None):
        try:
            self.logger.debug(re.sub(' +', ' ', query.replace('\n', ' ').strip(' ')))
            self.cursor.execute(query) if not values else self.cursor.execute(query, values)
            return True

        except Exception as error:
            self.logger.debug(error)
            return False

    def __execute_and_commit_query(self, query, values=None):
        if self.__execute_query(query, values=values):
            self.commit()
            return True
        self.rollback()
        return False

    def select_query(self, query, table_show=False, dict_output=True):
        if self.__execute_query(query):
            records = self.cursor.fetchall()

            if table_show:
                columns = [x.name for x in self.cursor.description]
                # self.logger.debug('\n' + tabulate([[str(row) for row in record] for record in records], headers=columns))

            if dict_output:
                columns = [x.name for x in self.cursor.description]
                records = [{columns[i]:record[i] for i in range(len(columns))} for record in records]

            return records

        return None

    def insert_query(self, query=None, table=None, attributes=None, values=None, build=False, commit=False):
        query = self.__build_insert_into(table=table, attributes=attributes, values=values) if build else query
        return self.__execute_and_commit_query(query, values=values) if commit else self.__execute_query(query, values=values)

    def update_query(self, query, values=None, commit=False):
        return self.__execute_and_commit_query(query, values=values) if commit else self.__execute_query(query)

    def delete_query(self, query, commit=False):
        return self.__execute_and_commit_query(query) if commit else self.__execute_query(query)

    def create_query(self, query, commit=False):
        return self.__execute_and_commit_query(query) if commit else self.__execute_query(query)
        

if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import run_main, get_environ
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    sslmode = 'require'
    postgre_key_var = 'POSTGRE_URL_HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)

    # __ init postgre manager __
    postgre_manager = PostgreManager(db_url=config_manager.get_postgre_url(database_key=postgre_key_var), caller='Example', logging_queue=Queue())
    postgre_manager.connect(sslmode=sslmode)

    print(postgre_manager.get_tables())

    query_ = "select * from notes limit 10"
    print(postgre_manager.select_query(query_))

    postgre_manager.close_connection()










