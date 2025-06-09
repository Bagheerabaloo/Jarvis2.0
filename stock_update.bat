@echo off
call C:\Users\Vale\anaconda3\Scripts\activate.bat
call conda activate Jarvis2.0
set PYTHONPATH=C:\Users\Vale\PycharmProjects\Jarvis2.0
python C:\Users\Vale\PycharmProjects\Jarvis2.0\src\stock\stock_main.py --process_name scheduled