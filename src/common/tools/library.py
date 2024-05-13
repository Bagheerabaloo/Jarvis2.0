import math
import sys
import traceback
import csv
import numpy as np
import calendar
import logging
import threading
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import matplotlib.patches as patches
from matplotlib import dates, ticker
from zipfile import ZipFile
from sigfig import round as sround
from datetime import datetime, timedelta
from time import time, sleep
from src.common.tools.GracefulKiller import GracefulKiller
from pathlib import Path
# import matplotlib as mpl
# import plotly.graph_objs as go
# from mpl_finance import candlestick_ohlc
from dataclasses import dataclass, fields
import pytz
from queue import Queue

def class_from_args(class_name, arg_dict):
    field_set = {f.name for f in fields(class_name) if f.init}
    filtered_arg_dict = {k: v for k, v in arg_dict.items() if k in field_set}
    return class_name(**filtered_arg_dict)


# __ Running main applications __
def run_main(app, log_queue: Queue = None):
    killer = GracefulKiller()
    
    while app.run:
        # if log_queue and not log_queue.empty():
        #     info = log_queue.get_nowait()
        #     if type(info) == logging.LogRecord:
        #         txt = '[%s]  [%s]  %s' % (info.name, info.levelname, info.message)
        #         app.telegram.send_message(chat_id=app.admin["chat"], text=txt, silent=True)

        # Check for SIGINT signal
        if killer.kill_now:
            # app.logger.info("Received SIGINT signal ...")
            print("Received SIGINT signal ...")
            app.close()

        sleep(1)

    # app.logger.info('Exiting main loop')
    print('Exiting main loop')
    start = time()
    while len([x for x in threading.enumerate() if not x.daemon]) > 1 and not time_out(start, 10):
        sleep(0.25)

    if len([x for x in threading.enumerate() if not x.daemon]) > 1:
        # app.logger.warning('Forcing thread closure')
        print('Forcing thread closure')
        for thread in threading.enumerate():
            # app.logger.info(thread)
            print(thread)
    else:
        # app.logger.info('All threads ended correctly')
        print('All threads ended correctly')


def get_environ():
    try:
        return os.environ.get('OS_ENVIRON')
    except:
        return None


def safe_execute(default, function, **args):
    try:
        return function(**args)
    except:
        return default


# _____ Operations on numbers ______ #
def to_int(x):
    return int(x)


def to_float(x):

    return float(x)


def is_number(s):
    """ Returns True is string is a number. """
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_percentage(x):

    return True if is_number(x) and 0 <= float(x) <= 100 else False


def float_to_string(x, max_len=12, add_plus_sign=False):

    try:

        s = "{:.12f}".format(float(x)).rstrip("0")

        if s[-1] == '.':
            s = s[:-1]

        if s[0] == '-':
            sign = '-'
            s = s[1:]
        else:
            sign = '' if not add_plus_sign else '+'

        if '.' in s and len(s) > max_len - 1:

            x = s.split('.')
            if len(s) > 1 and s[:2] == '0.':
                a = s
                a = a[2:].lstrip('0')
                ll = len(a)
            else:
                ll = len(s) - 1

            n = ll - max_len
            s = sign + x[0] + '.' + x[1][:(len(x[1]) - n)] if n < len(x[1]) else x[0]

        else:
            s = sign + s

        return s

    except:
        return None


def ordinal(n):

    return "%d%s" % (n, "tsnrhtdd"[(math.floor(n / 10) % 10 != 1) * (n % 10 < 4) * n % 10::4])


def truncate(number, digits) -> float:

    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper


def order_of_magnitude(number):
    return math.floor(float(float_to_string(math.log(number, 10))))


def significant_figures(number):

    return len(str(number).replace('.', '').strip('0'))


def psychological_round(number, side='buy', recursive=2):

    sig_figs = significant_figures(number)
    rounded = sround(float(number) * 10 ** (2 - order_of_magnitude(float(number))), sigfigs=sig_figs)

    main_digit = float(str(rounded)[0])
    secondary_digits = float(str(rounded)[1:])

    increment = psychological_buy(main_digit, secondary_digits) if side == 'buy' else psychological_sell(main_digit, secondary_digits)

    final_number = sround((rounded + increment) * 10 ** (order_of_magnitude(float(number)) - 2), sigfigs=5)

    if str(final_number)[0] != str(number)[0]:
        final_number = psychological_round(final_number, side=side, recursive=0)

    if recursive > 0:

        reference = 1
        point = False
        while reference < len(str(final_number)) and str(final_number)[reference] in ['0', '.']:
            if str(final_number)[reference] == '.':
                point = True
            reference += 1
        remaining = str(final_number)[reference:]
        if len(remaining) > 0:
            final_number = str(final_number)[:reference]
            result = psychological_round(remaining, side=side, recursive=recursive-1)
            final_number += str(result).split('.')[0] if point and '.' in str(result) else str(result)
            final_number = sround(float(final_number), sigfigs=5)

    if abs(final_number / float(number) - 1) >= 0.1:
        return float(number)

    return max(final_number, float(number)) if side == 'buy' else min(final_number, float(number))


def psychological_buy(main_digit, secondary_digits):

    increment = 0

    if 49 - main_digit < secondary_digits < 53 - 2.6 * (1 - main_digit/9):
        increment = 50.05 + 0.375*((main_digit/3)**1.5) - secondary_digits
    elif 99 - 2*((main_digit/3)**1.5) < secondary_digits < 100:
        increment = 100.05 + 0.75*((main_digit/3)**1.5) - secondary_digits
    elif secondary_digits < 0.3 + 7/9 * ((float(main_digit)/3)**1.5):
        increment = 0.3 + 7/9 * ((main_digit/3)**1.5) - secondary_digits
    elif float(str(secondary_digits)[-1]) == 0:
        increment = max(0.311, main_digit/10 + 0.11)

    return increment + 0.001


def psychological_sell(main_digit, secondary_digits):

    increment = 0

    if 47 + 2.6 * (1 - main_digit / 9) < secondary_digits < 51 + main_digit:
        increment = 47 + 0.375 * ((main_digit / 3) ** 1.5) - secondary_digits
    elif 99 - 2 * ((main_digit / 3) ** 1.5) < secondary_digits < 100:
        increment = - 7/9 * ((main_digit / 3) ** 1.5)
    elif secondary_digits < 1 + 2 * ((main_digit / 3) ** 1.5):
        increment = - secondary_digits - 0.75 * ((main_digit / 3) ** 1.5)
    elif float(str(secondary_digits)[-1]) == 0:
        increment = - max(0.311, main_digit / 10 + 0.11)

    return increment - 0.001


# _____ Operations on timestamps/dates ______ #
def int_timestamp_now():
    return int(time())


def validate_date(date_text, format):
    try:
        datetime.strptime(date_text, format)
        return True
    except ValueError:
        return False


def timestamp2date(timestamp, frmt='%Y-%m-%d %H:%M:%S'):
    return datetime.utcfromtimestamp(timestamp).strftime(frmt)


def date2timestamp_v2(date):
    date = date.replace('T', ' ').replace('Z', '')

    for frmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M']:
        if validate_date(date, frmt):
            dt = datetime.strptime(date, frmt)
            return calendar.timegm(dt.utctimetuple())
    return None


def date2timestamp(date, frmt='%Y-%m-%d %H:%M:%S'):
    dt = datetime.strptime(date, frmt)
    return calendar.timegm(dt.utctimetuple())


def add_hours_to_date_string(date, hours=0, minutes=0, frmt='%Y-%m-%d %H.%M.%S'):

    timestamp = date2timestamp(date, frmt)

    dt_object = datetime.utcfromtimestamp(timestamp)

    dt_object += timedelta(hours=hours, minutes=minutes)

    frmt = '{:' + frmt + '}'

    return frmt.format(dt_object)


def add_months_to_date_string(date, months):

    timestamp, frmt = date2timestamp_v2(date)
    sourcedate = datetime.utcfromtimestamp(timestamp)

    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])

    dt = datetime(year=year, month=month, day=day)

    frmt = '{:' + frmt + '}'
    return frmt.format(dt)


def get_compact_month_year_from_timestamp(timestamp):
    result = datetime.utcfromtimestamp(timestamp).strftime('%m/%Y')
    split_result = result.split('/')
    month = calendar.month_name[int(split_result[0])]
    year = split_result[1][-2:]

    return month[:3] + ' ' + year


def get_human_date_from_timestamp(timestamp):
    result = datetime.utcfromtimestamp(timestamp).strftime('%d/%m/%Y %H:%M')
    split_result = result.split('/')
    month = calendar.month_name[int(split_result[1])]
    return f"{split_result[0]} {month[:3]} {split_result[2]}"


def get_compact_month_year_from_timestamp_v2(result):
    split_result = result.split('/')
    month = calendar.month_name[int(split_result[0])]
    year = split_result[1][-2:]

    return month[:3] + ' ' + year


def get_month_year_from_timestamp_v2(result):
    split_result = result.split('/')
    month = calendar.month_name[int(split_result[0])]
    year = split_result[1][-2:]

    return month + ' ' + year


def time_out(start, eta):
    return True if time() >= (start + eta) else False


def seconds_to_time(seconds):
    minutes = to_int(to_int(seconds)/60)
    seconds = to_int(to_int(seconds) % 60)
    hours = to_int(minutes / 60)
    minutes = minutes % 60
    days = to_int(hours / 24)
    hours = hours % 24

    return {'days': days,
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds}


def seconds_to_time_str(seconds):
    conversion = seconds_to_time(seconds)

    text = ''
    if conversion['days']:
        text += '{}D '.format(conversion['days'])
    if conversion['hours']:
        text += '{:02d}h '.format(conversion['hours'])
    if conversion['minutes']:
        text += '{:02d}m'.format(conversion['minutes'])
    if conversion['seconds']:
        text += '{:02d}s'.format(conversion['seconds'])

    return text


def now(frmt='%Y-%m-%d %H.%M.%S'):
    dt = datetime.now(tz=None)
    frmt = '{:' + frmt + '}'
    return frmt.format(dt)


def build_eta(target_hour, target_minute=0):
    dt = datetime.now(pytz.timezone('Europe/Rome'))
    eta = ((target_hour - dt.hour - 1) * 60 * 60) + ((60 + target_minute - dt.minute - 1) * 60) + (61 - dt.second)
    if eta < 0:
        eta += 24 * 60 * 60
    return eta

# _____ File read/write _____ #

def create_folder(path):
    """ create a folder given the full path if not already existing """
    Path(path).mkdir(parents=True, exist_ok=True)


def file_read(path):

    with open(path, newline='') as myfile:
        content = myfile.read()

    return content


def file_write(path, data):

    with open(path, 'w+', newline='') as myfile:
        line = myfile.write(data)


def csv_read(path, delimiter=',', quotechar='|'):

    with open(path, newline='') as csv_file:
        spam_reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
        data = []
        for row in spam_reader:
            data.append(row)

    return data


def csv_write(path, data, delimiter=',', quotechar='|', method='w'):

    with open(path, method, newline='') as csv_file:
        spam_writer = csv.writer(csv_file, delimiter=delimiter, quotechar=quotechar)
        for row in data:
            spam_writer.writerow(row)


# _____ Exception Handling _____ #

def print_exception():

    _, exc_obj, tb = sys.exc_info()
    # f = tb.tb_frame
    # line_no = tb.tb_lineno
    # filename = f.f_code.co_filename
    # linecache.checkcache(filename)
    # line = linecache.getline(filename, line_no, f.f_globals)
    # print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, line_no, line.strip(), exc_obj))
    # print('EXCEPTION IN ({}): {}'.format(traceback.format_list(traceback.extract_tb(tb)[-1:])[-1], exc_obj))
    lines = traceback.format_list(traceback.extract_tb(tb)[-1:])[-1].splitlines()
    text = lines[0] + ' --> ' + lines[1].lstrip(' ')
    print('EXCEPTION IN {}:   {}'.format(text, exc_obj))


def get_exception():

    _, exc_obj, tb = sys.exc_info()
    # f = tb.tb_frame
    # line_no = tb.tb_lineno
    # filename = f.f_code.co_filename
    # linecache.checkcache(filename)
    # line = linecache.getline(filename, line_no, f.f_globals)
    # print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, line_no, line.strip(), exc_obj))
    # print('EXCEPTION IN ({}): {}'.format(traceback.format_list(traceback.extract_tb(tb)[-1:])[-1], exc_obj))
    lines = traceback.format_list(traceback.extract_tb(tb)[-1:])[-1].splitlines()
    text = lines[0] + ' --> ' + lines[1].lstrip(' ') if len(lines) > 1 else lines[0]
    return 'EXCEPTION IN {}:   {}'.format(text, exc_obj)


def print_x(name_thread, text, flag=False):

    print('THREAD: {} --> {}'.format(name_thread, text)) if not flag else print('THREAD: {} --> # {} #'.format(name_thread, text))


# _____ Telegram functions _____ #


def simulate_telegram_message(from_id, name='', text='', username='', message_id=0, chat=None):

    return {'chat': chat if chat else from_id,
            'from_id': from_id,
            'from_name': name,
            'text': text,
            'from_username': username,
            'message_id': message_id}


def admin_message(admin, text, username='', message_id=0, chat=None):

    return simulate_telegram_message(from_id=admin['chat'], name=admin['name'], text='end', username=username, message_id=message_id, chat=chat)


def square_keyboard(inputs):

    l = len(inputs)
    rows = int(math.floor(math.sqrt(l)))
    (quot, rem) = divmod(l, rows)
    qty = [quot] * rows
    pos = 0
    while rem > 0:
        qty[pos] += 1
        pos += 1
        rem -= 1
    count = 0
    keyboard = []
    for i in range(rows):
        vector = []
        for j in range(qty[i]):
            vector.append(str(inputs[count]))
            count += 1
        keyboard.append(vector)
    return keyboard


# _____ Miscellaneous _____ #

def load_password(name_archive, pwd):

    try:
        with ZipFile(name_archive) as myzip:
            name_file = myzip.filelist[0].filename
            with myzip.open(name_file, pwd=pwd.encode('cp850', 'replace')) as myfile:
                line = myfile.read()
                content = line.decode('utf-8')
    except:
        print_exception()
        content = None

    return content


def init_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] %(name)s - %(message)s')
    if logger.hasHandlers():
        ch = logger.handlers[0]
    else:
        # create console handler
        ch = logging.StreamHandler()
        logger.addHandler(ch)

    ch.setFormatter(formatter)
    return logger

# def real_time_plot_candles(candles):
#
#     canvas = np.zeros((480, 640))
#     screen = pf.screen(canvas, 'Sinusoid')
#
#     ohlc_data = []
#
#     for line in candles:
#         ohlc_data.append((dates.datestr2num(timestamp2date(line[0])), line[3], line[2], line[1], line[4], line[5]))
#
#     fig, ax1 = plt.subplots()
#     candlestick_ohlc(ax1, ohlc_data, width=0.5 / (24 * 60), colorup='g', colordown='r', alpha=0.8)
#
#     ax1.xaxis.set_major_formatter(dates.DateFormatter('%d/%m/%Y %H:%M'))
#     ax1.xaxis.set_major_locator(ticker.MaxNLocator(10))
#
#     plt.xticks(rotation=30)
#     plt.grid()
#     plt.xlabel('Date')
#     plt.ylabel('Price')
#     plt.title('ETHUSD')
#     plt.tight_layout()
#
#     fig.canvas.draw()
#
#     image = np.fromstring(fig.canvas.tostring_rgb(), dtype=np.uint8, sep='')
#     image = image.reshape(fig.canvas.get_width_height()[::-1] + (3,))
#
#     screen.update(image)


