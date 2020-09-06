import pymysql
import configparser
import datetime
from telegram.ext import Updater

cf = configparser.ConfigParser()
cf.read('config.cfg')
DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
TOKEN = cf.get('telegram', 'TOKEN')
VALID_USER = cf.get('telegram', 'VALID_USER')

LIMIT_PER = 0.70
connection = pymysql.connect(host=DB_IP,
                             user=DB_USER,
                             password=DB_PWD,
                             db=DB_SCH,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def select_distinct_stocks():
    cursor = connection.cursor()
    cursor.execute("select name, code from daily_stock group by code")
    return cursor.fetchall()


def get_potential_datas(limit_rate, code):
    cursor = connection.cursor()
    query = """
    SELECT ds.name, f.type, f.code, f.analyze_at, f.potential, f.volume , f.percent, f.evaluate, d.close
    FROM data.forecast f, data.daily_stock ds 
    WHERE f.type = 3 AND ds.code = f.code AND potential > %s AND f.code = %s 
    group by f.id 
    ORDER BY f.analyze_at, f.code ASC
    """
    cursor.execute(query, (str(limit_rate), code))
    return cursor.fetchall()


def is_compare_chain_minus(code, analyze_at, day_cnt):
    cursor = connection.cursor()
    cursor.execute("select date from data.daily_stock ds "
                   "where ds.code = %s and ds.date < %s order by ds.id desc limit %s", (code, analyze_at, day_cnt))
    dates = cursor.fetchall()

    result = True
    for date in dates:
        cursor.execute(
            "select (ds.close-ds.open) as compare from data.daily_stock ds where ds.code = %s and ds.date = %s",
            (code, date.get('date')))
        compare = cursor.fetchone().get('compare')
        if compare > 0 and result is True:
            result = False
    return result


def forecast_result(code, name):
    foreacast_rate = 100
    datas = get_potential_datas(LIMIT_PER, code)
    for data in datas:
        if is_compare_chain_minus(code=code, analyze_at=data.get('analyze_at'), day_cnt=1):
            result_msg = '[' + code[1:] + '][' + name + ']'         
            percent = data.get('percent')
            if percent is not None :
                foreacast_rate = foreacast_rate + (foreacast_rate * percent)
                ratio = round(foreacast_rate,  1)
                result_msg += ('[' + str(ratio) + ']')
    return result_msg

def get_code(param):
    param = param.strip()
    cursor = connection.cursor()
    cursor.execute("SELECT distinct(code), name FROM data.daily_stock WHERE name = %s", (param))
    result = cursor.fetchone()
    if result is not None:
        return result.get('code'), result.get('name')
    cursor.execute("SELECT distinct(code), name FROM data.daily_stock WHERE code = %s", (param))
    result = cursor.fetchone()
    if result is not None:
        return result.get('code'), result.get('name')
    return None


def simulator(code):
    code, name = get_code(code)
    if code is None:
        return None
    return forecast_result(code, name)


def get_max_target_at():
    cursor = connection.cursor()
    cursor.execute("select max(evaluate) as evaluate_max from data.forecast")
    evaluate_max = cursor.fetchone().get('evaluate_max')
    cursor.execute("select analyze_at from data.forecast group by analyze_at order by analyze_at desc limit %s",
                   (evaluate_max))
    results = cursor.fetchall()
    if len(results) >= evaluate_max:
        return results[evaluate_max - 1].get('analyze_at')
    return datetime.date.today()


def get_potential_data_results(target_at, limit_rate):
    cursor = connection.cursor()
    query = """
            SELECT ds.name, f.type, f.code, f.analyze_at, f.potential, f.volume , f.percent, f.evaluate 
            FROM data.forecast f, data.daily_stock ds
            WHERE f.type = 3 AND ds.code = f.code AND analyze_at > %s and potential > %s 
            group by f.id ORDER BY f.analyze_at, f.code ASC
            """
    cursor.execute(query, (target_at, str(limit_rate)))
    return cursor.fetchall()


def get_potential(target_at, chan_minus, limit_rate):
    datas = get_potential_data_results(target_at, limit_rate)
    potens = list()
    for data in datas:
        compare = is_compare_chain_minus(data.get('code'), data.get('analyze_at'), chan_minus)
        if compare:
            potens.append(data)
    return potens

def append_msg(append_list) :
    result = ''
    for append in append_list:
        if append is not None:
            result += '[' + str(append) + ']'
    return result + '\n'

def print_potentials(datas):
    msg = ''
    dates = []
    for data in datas:
        date = data.get('analyze_at').strftime("%Y-%m-%d")
        if date not in dates:
            dates.append(date)            
            msg += append_msg([date])
        msg += append_msg([data.get('percent'), 
                        simulator(data.get('code')),
                        data.get('type'), 
                        data.get('potential'), 
                        data.get('volume')])
    print(msg)
    return msg




def send_telegram(msg):
    updater = Updater(TOKEN)
    updater.bot.sendMessage(chat_id=VALID_USER, text=msg)


datas = get_potential(target_at=get_max_target_at() - datetime.timedelta(days=1), chan_minus=1, limit_rate=LIMIT_PER)
msg = print_potentials(datas)
send_telegram(msg)
