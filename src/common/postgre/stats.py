import psycopg2
from src.common.postgre.PostgreManager import PostgreManager
from src.common.file_manager.FileManager import FileManager


def main():
    # __ init file manager __
    config_manager = FileManager()

    # __ get telegram token __
    postgre_url = config_manager.get_postgre_url("POSTGRE_URL_LOCAL_STOCK")

    postgre_manager = PostgreManager(db_url=postgre_url, delete_permission=True)
    if not postgre_manager.connect(sslmode='disable'):
        # logger.warning("PostgreDB connection not established: cannot connect")
        return

    query = """
            SELECT tablename 
            FROM pg_tables
            WHERE schemaname = 'public';
            """

    tables = postgre_manager.select_query(query)

    # Initialize an empty string to build the dynamic query.
    query = ""

    # Loop through each table name and build a part of the final query.
    for table in tables:
        query += f"SELECT 'public' AS schema_name, '{table['tablename']}' AS table_name, COUNT(*) AS row_count FROM public.{table['tablename']} UNION ALL "

    # Remove the trailing 'UNION ALL' and add a semicolon to complete the query.
    query = query.rstrip(' UNION ALL ') + ';'

    # Fetch all results from the executed dynamic query.
    rows = postgre_manager.select_query(query)

    # Print each row from the results.
    for row in rows:
        print(row)

    postgre_manager.close_connection()


if __name__ == "__main__":
    main()
