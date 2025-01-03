from typing import List

from src.common.tools.library import class_from_args
from src.common.postgre.PostgreManager import PostgreManager
from stock.src.app.StockUser import StockUser


class StockPostgreManager(PostgreManager):
    """ ____ DB: Users Collection _____"""
    def get_stock_users(self) -> List[StockUser]:
        query = """
                SELECT * 
                FROM stock_users S 
                JOIN telegram_users T ON S.telegram_id = T.telegram_id
                """
        stock_users = self.select_query(query=query)
        if not stock_users or len(stock_users) == 0:
            telegram_admin_user = self.get_telegram_admin_user()
            if not telegram_admin_user:
                return []
            new_stock_users = StockUser(telegram_id=telegram_admin_user.telegram_id, name=telegram_admin_user.name, username=telegram_admin_user.username, is_admin=telegram_admin_user.is_admin)
            self.add_stock_user_to_db(new_stock_users)
            return [new_stock_users]
        return [class_from_args(StockUser, x) for x in stock_users]

    def add_stock_user_to_db(self, user: StockUser, commit: bool = True) -> bool:
        query = f"""
                INSERT INTO stock_users
                (telegram_id)
                VALUES
                ({user.telegram_id})
                """
        return self.insert_query(query=query, commit=commit)


if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import get_environ
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    sslmode_ = 'require'
    postgre_key_var = 'POSTGRE_URL_HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)

    # __ init postgre manager __
    postgre_manager = StockPostgreManager(db_url=config_manager.get_postgre_url(database_key=postgre_key_var), caller='Example', logging_queue=Queue())
    postgre_manager.connect(sslmode=sslmode_)

    print(postgre_manager.get_tables())

    query_ = "select * from notes limit 10"
    print(postgre_manager.select_query(query_))

    postgre_manager.close_connection()










