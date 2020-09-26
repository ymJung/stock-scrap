import pymysql
import configparser
from datetime import timedelta

cf = configparser.ConfigParser()
cf.read('config.cfg')

DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
VALID_USER = cf.get('telegram', 'VALID_USER')
TOKEN = cf.get('telegram', 'TOKEN')


class DBManager:
    def __init__(self):
        self.conn = pymysql.connect(host=DB_IP, user=DB_USER, password=DB_PWD, db=DB_SCH, charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    def __del__(self):
        self.conn.close()

    def select_forecast(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, type, code, evaluate, analyze_at, potential FROM data.forecast where calculated = 0")
        return cursor.fetchall()

    def select_stock_data(self, stock_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, code, date, open, close, st_purchase_inst FROM data.daily_stock WHERE id = %s",
                       (stock_id))
        return cursor.fetchone()

    def select_stock_datas(self, code, date, evaluate):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, code, date, open, close, st_purchase_inst FROM data.daily_stock WHERE code = %s AND date >= %s ORDER BY date ASC LIMIT 0, %s",
            (code, date, evaluate))
        return cursor.fetchall()

    def select_stock_ids(self, code, date, evaluate):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id as stock_id FROM data.daily_stock WHERE code = %s AND date >= %s ORDER BY date ASC LIMIT 0, %s",
            (code, date, evaluate))
        stock_ids = []
        for data in cursor.fetchall():
            stock_ids.append(data.get('stock_id'))
        return stock_ids

    def update_forecast_percent(self, forecast_id, percent):
        cursor = self.conn.cursor()
        cursor.execute('UPDATE `data`.`forecast` SET `percent`=%s, `calculated`=1 WHERE `id`= %s',
                       (percent, forecast_id))
        self.conn.commit()

    def getPotentialDatas(self, limitRate):
        cursor = self.conn.cursor()
        cursor.execute("select max(evaluate) as evaluateMax, max(analyze_at) as analyze_at_max from data.forecast")
        result = cursor.fetchone()
        target_at = result.get('analyze_at_max') - timedelta(days=result.get('evaluateMax'))
        query = "SELECT ds.name, f.type, f.code, f.analyze_at, f.potential, f.volume , f.percent, f.evaluate FROM data.forecast f, data.daily_stock ds WHERE ds.code = f.code AND analyze_at > %s and potential > %s group by f.id ORDER BY f.analyze_at, f.code ASC"
        cursor.execute(query, (target_at, str(limitRate)))
        return cursor.fetchall()

    def select_last_calculated_id(self):
        cursor = self.conn.cursor()
        cursor.execute("select id from forecast where calculated = 0 order by id asc limit 1")
        return cursor.fetchone().get('id')

class Score:
    def run_score(self):
        dbm = DBManager()
        datas = dbm.select_forecast()
        for data in datas:
            select = TYPE_MAP[data.get('type')]
            analyze_at = data.get('analyze_at')
            code = data.get('code')
            evaluate = data.get('evaluate')
            forecast_id = data.get('id')

            stock_ids = dbm.select_stock_ids(code, analyze_at, evaluate)
            if len(stock_ids) < evaluate:
                print('not yet', code, analyze_at, evaluate)
                continue

            start_data = dbm.select_stock_data(stock_ids[0])
            end_data = dbm.select_stock_data(stock_ids[evaluate - 1])
            changed = end_data.get(select) - start_data.get(select)
            percent = round((changed / start_data.get(select)) * 100, 2)
            print(TYPE_MAP[data.get('type')], start_data.get(select), changed, percent)
            dbm.update_forecast_percent(forecast_id, percent)

        print('done')
TYPE_MAP = {3: 'close', 6:'st_purchase_inst'}

Score().run_score()



