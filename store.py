from datetime import datetime, timedelta, date
import pymysql
import win32com.client as com
import configparser


class Store:
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read('config.cfg')
        self.connection = pymysql.connect(host=cf.get('db', 'DB_IP'),
                                          user=cf.get('db', 'DB_USER'),
                                          password=cf.get('db', 'DB_PWD'),
                                          db=cf.get('db', 'DB_SCH'),
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DEFAULT_FIRST_DATE = 20040101
        self.KOSPI_200 = 180
        self.stock_chart = com.Dispatch("CpSysDib.StockChart")
        self.stock_chart.SetInputValue(1, ord('1'))  # 기간으로 요청
        self.stock_chart.SetInputValue(5,
                                       (0, 2, 3, 4, 5, 8, 16, 21))  # 요청필드(날짜, 시가, 고가, 저가, 종가, 거래량, 외국인 보유수량, 기관 누적 순매수
        self.stock_chart.SetInputValue(6, ord('D'))  # 일간데이터
        self.stock_chart.SetInputValue(9, ord('1'))  # 수정주가 요청
        self.code_mgr = com.Dispatch("CpUtil.CpCodeMgr")
        self.ins = com.Dispatch("CpUtil.CpStockCode")

    def __del__(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def save_stocks(self, code, ds_stock_chart):
        cursor = self.connection.cursor()
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        for i in range(ds_stock_chart.GetHeaderValue(3)):
            date = ds_stock_chart.GetDataValue(0, i)

            s_date = datetime(date // 10000, date // 100 % 100, date % 100)
            open = ds_stock_chart.GetDataValue(1, i)
            high = ds_stock_chart.GetDataValue(2, i)
            low = ds_stock_chart.GetDataValue(3, i)
            close = ds_stock_chart.GetDataValue(4, i)
            volume = ds_stock_chart.GetDataValue(5, i)
            hold_foreign = float(ds_stock_chart.GetDataValue(6, i))
            st_purchase_inst = float(ds_stock_chart.GetDataValue(7, i))

            cursor.execute("select count(date) as cnt from data.daily_stock where date = %s and code = %s",
                           (date, code))
            exist = cursor.fetchone()
            if exist.get('cnt') > 0:
                cursor.execute("select id from data.daily_stock where date = %s and code = %s", (date, code))
                upd_id = cursor.fetchone().get('id')
                cursor.execute("UPDATE data.daily_stock SET "
                               "code = %s, date = %s, open = %s, high = %s, low= %s, close= %s, volume= %s, hold_foreign= %s, st_purchase_inst= %s"
                               " WHERE `id`=%s",
                               (code, s_date, open, high, low, close, volume, hold_foreign, st_purchase_inst, upd_id))
                print('updated stocks code ', code, ' date', date)
            else:
                if s_date == today:
                    if datetime.now().hour < 15:
                        continue
                cursor.execute(
                    "INSERT INTO "
                    "data.daily_stock(code, date, open, high, low, close, volume, hold_foreign, st_purchase_inst) "
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (code, s_date, open, high, low, close, volume, hold_foreign, st_purchase_inst)
                )
                print('saved stocks code ', code, ' date', date)
        self.commit()

    def get_possible_store_date(self, code):
        cursor = self.connection.cursor()
        cursor.execute("SELECT date FROM data.daily_stock WHERE code = %s ORDER BY date DESC LIMIT 1", code)
        last_date = cursor.fetchone()
        if last_date is None:
            return self.DEFAULT_FIRST_DATE
        after_day = last_date.get('date') + timedelta(days=1)
        if (after_day.date() == date.today()) == (datetime.today().hour >= 15 and datetime.today().minute > 30):
            after_day = after_day - timedelta(days=1)
        return after_day.year * 10000 + after_day.month * 100 + after_day.day

    def is_invalid_status(self):
        if self.stock_chart.BlockRequest() != 0 or self.stock_chart.GetDibStatus() != 0:
            print('invalid status.')
            return True
        return False

    def update_stock_name(self):
        total_count = self.ins.GetCount()
        for i in range(0, total_count):
            ds_code = self.ins.GetData(0, i)
            ds_name = self.ins.getData(1, i)
            self.update_daily_stocks_code(ds_code, ds_name)

    def update_daily_stocks_code(self, code, name):
        cursor = self.connection.cursor()
        cursor.execute("select id, name from data.daily_stock where code = %s", (code))
        results = cursor.fetchall()
        for result in results:
            if result.get('name') != name:
                print('update name', name, code)
                cursor.execute("UPDATE `data`.`daily_stock` SET `name`=%s WHERE `id`=%s", (name, result.get('id')))
        self.commit()

    def insert_kospi_stocks(self):
        for code in self.code_mgr.GetGroupCodeList(self.KOSPI_200):
            possible_store_date = self.get_possible_store_date(code)
            self.stock_chart.SetInputValue(0, code)
            self.stock_chart.SetInputValue(3, possible_store_date)  # 종료일
            if self.is_invalid_status():
                continue
            if self.stock_chart.GetHeaderValue(5) < possible_store_date:  # 최종 영업일이 요청일 보다 이전인 경우 Skip
                continue
            self.save_stocks(code, self.stock_chart)

            while self.stock_chart.Continue:
                if self.is_invalid_status():
                    continue
                self.save_stocks(code, self.stock_chart)

    def run(self):
        self.insert_kospi_stocks()
        self.update_stock_name()


Store().run()
