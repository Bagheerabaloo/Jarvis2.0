import re
import psycopg2
# from tabulate import tabulate
from typing import List, Optional
from src.common.tools.logging_class import LoggerObj
from src.common.tools.library import get_exception, print_exception, int_timestamp_now
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramPendingUser import TelegramPendingUser
from src.common.telegram.TelegramChat import TelegramChat


class PostgreManager:
    # TODO: add protection against insert/update/delete
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
    def __execute_query(self, query, values=None) -> bool:
        try:
            self.logger.debug(re.sub(' +', ' ', query.replace('\n', ' ').strip(' ')))
            self.cursor.execute(query) if not values else self.cursor.execute(query, values)
            return True

        except Exception as error:
            self.logger.debug(error)
            return False

    def __execute_and_commit_query(self, query, values=None) -> bool:
        if self.__execute_query(query, values=values):
            self.commit()
            return True
        self.rollback()
        return False

    def select_query(self, query, table_show=False, dict_output=True) -> Optional[List[dict]]:
        if self.__execute_query(query):
            records = self.cursor.fetchall()

            if table_show:
                columns = [x.name for x in self.cursor.description]
                # self.logger.debug('\n' + tabulate([[str(row) for row in record] for record in records], headers=columns))

            if dict_output:
                columns = [x.name for x in self.cursor.description]
                records = [{columns[i]: record[i] for i in range(len(columns))} for record in records]
            return records
        return None

    def insert_query(self, query=None, table=None, attributes=None, values=None, build=False, commit=False) -> bool:
        query = self.__build_insert_into(table=table, attributes=attributes, values=values) if build else query
        return self.__execute_and_commit_query(query, values=values) if commit else self.__execute_query(query, values=values)

    def update_query(self, query, values=None, commit=False) -> bool:
        return self.__execute_and_commit_query(query, values=values) if commit else self.__execute_query(query)

    def delete_query(self, query, commit=False) -> bool:
        return self.__execute_and_commit_query(query) if commit else self.__execute_query(query)

    def create_query(self, query, commit=False):
        return self.__execute_and_commit_query(query) if commit else self.__execute_query(query)

    """ ##### Telegram Users ##### """
    def get_telegram_users(self, admin_user: TelegramUser = None):
        telegram_users = self.select_query("SELECT * FROM telegram_users")
        if (not telegram_users or len(telegram_users) == 0) and admin_user:
            self.add_telegram_user_to_db(user=admin_user)
            return [admin_user]
        return [TelegramUser(telegram_id=x["telegram_id"], name=x["name"], username=x["username"], is_admin=x["is_admin"]) for x in telegram_users]

    def get_telegram_admin_user(self):
        admin_telegram_users = self.select_query("SELECT * FROM telegram_users WHERE is_admin = TRUE")
        admin_telegram_users = [TelegramUser(telegram_id=x["telegram_id"], name=x["name"], username=x["username"], is_admin=x["is_admin"]) for x in admin_telegram_users]
        if len(admin_telegram_users) > 1:
            print("WARNING: more than one telegram admin")
        return admin_telegram_users[0] if len(admin_telegram_users) > 0 else None

    def get_telegram_chats_from_db(self, admin_chat: TelegramChat = None):
        telegram_chats = self.select_query("SELECT * FROM telegram_chats")
        if (not telegram_chats or len(telegram_chats) == 0) and admin_chat:
            self.add_telegram_chat_to_db(chat=admin_chat)
            return [admin_chat]
        return [TelegramChat(chat_id=x['chat_id'], type=x["type"], username=x["username"], first_name=x["first_name"], last_name=x["last_name"]) for x in telegram_chats]

    def add_telegram_user_to_db(self, user: TelegramUser, commit: bool = True) -> bool:
        query = f"""
                INSERT INTO telegram_users
                (telegram_id, name, username, is_admin, created, last_modified)
                VALUES
                ({user.telegram_id}, '{user.name}', '{user.username}', {user.is_admin}, {int_timestamp_now()}, {int_timestamp_now()})
                """
        return self.insert_query(query=query, commit=commit)
        # self.logger.info('Admin user created')

    def add_telegram_chat_to_db(self, chat: TelegramChat, commit: bool = True) -> bool:
        query = f"""
                INSERT INTO telegram_chats
                (chat_id, type, username, first_name, last_name, created, last_modified)
                VALUES
                ({chat.chat_id}, $${chat.type}$$, $${chat.username}$$, $${chat.first_name}$$, $${chat.last_name}$$, {int_timestamp_now()}, {int_timestamp_now()})
                """
        return self.insert_query(query=query, commit=commit)
        # self.logger.info('Admin user created')

    """ ##### Pending Users ##### """
    def get_telegram_pending_users(self) -> List[TelegramPendingUser]:
        pending_users = self.select_query("SELECT * FROM telegram_pending_users")
        if not pending_users or len(pending_users) == 0:
            return []
        return [TelegramPendingUser(telegram_id=x["telegram_id"],
                                    name=x["name"],
                                    username=x["username"],
                                    approved=x["approved"],
                                    banned=x["banned"],
                                    app=x["app"])
                for x in pending_users]

    def get_telegram_pending_users_by_app(self, app: str) -> List[TelegramPendingUser]:
        pending_users = self.select_query(f"SELECT * FROM telegram_pending_users WHERE app = $${app}$$")
        if not pending_users or len(pending_users) == 0:
            return []
        return [TelegramPendingUser(telegram_id=x["telegram_id"],
                                    name=x["name"],
                                    username=x["username"],
                                    approved=x["approved"],
                                    banned=x["banned"],
                                    app=x["app"])
                for x in pending_users]

    def add_pending_telegram_user_to_db(self, user: TelegramPendingUser) -> bool:
        query = f"""
                INSERT INTO telegram_pending_users
                (telegram_id, name, username, approved, banned, app, created, last_modified)
                VALUES
                ({user.telegram_id}, $${user.name}$$, $${user.username}$$, {user.approved}, {user.banned}, $${user.app}$$, {int_timestamp_now()}, {int_timestamp_now()})
                """
        return self.insert_query(query=query, commit=True)

    def approve_pending_telegram_user(self, user: TelegramUser, app: str, commit: bool = True) -> bool:
        query = f"""
                 UPDATE telegram_pending_users
                 SET approved = TRUE, last_modified = {int_timestamp_now()}
                 WHERE telegram_id = {user.telegram_id}
                 AND app = $${app}$$
                 """
        return self.update_query(query=query, commit=commit)

    def ban_pending_telegram_user(self, user: TelegramUser, app: str, commit: bool = True) -> bool:
        query = f"""
                 UPDATE telegram_pending_users
                 SET banned = TRUE, last_modified = {int_timestamp_now()}
                 WHERE telegram_id = {user.telegram_id}
                 AND app = $${app}$$
                 """
        return self.update_query(query=query, commit=commit)


if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import run_main, get_environ
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    sslmode_ = 'require'
    postgre_key_var = 'POSTGRE_URL_HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)

    # __ init postgre manager __
    postgre_manager = PostgreManager(db_url=config_manager.get_postgre_url(database_key=postgre_key_var), caller='Example', logging_queue=Queue())
    postgre_manager.connect(sslmode=sslmode_)

    print(postgre_manager.get_tables())

    query_ = "select * from notes limit 10"
    print(postgre_manager.select_query(query_))

    postgre_manager.close_connection()










