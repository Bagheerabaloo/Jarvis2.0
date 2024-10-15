import os
import glob

# Define the folder path
folder_path = r'C:\Users\Vale\PycharmProjects\Jarvis2.0\src\stock\DB\single_tables'

# Get all SQL files in the folder
sql_files = glob.glob(os.path.join(folder_path, '*.sql'))

# Define the output file path
output_file = r'C:\Users\Vale\PycharmProjects\Jarvis2.0\src\stock\DB\db_ddl.sql'

# Open the output file in write mode
with open(output_file, 'w') as outfile:
    # Handle ticker_ddl.sql separately
    ticker_file = os.path.join(folder_path, 'ticker_ddl.sql')
    if ticker_file in sql_files:
        with open(ticker_file, 'r') as infile:
            content = infile.read()
            outfile.write(content)
            outfile.write('\n')
        sql_files.remove(ticker_file)

    # Handle the rest of the files
    for sql_file in sql_files:
        with open(sql_file, 'r') as infile:
            content = infile.read()
            outfile.write(content)
            outfile.write('\n')