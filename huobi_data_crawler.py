from huobi_swap_client import Huobi_Swap_Client
from huobi_spot_client import Huobi_Spot_Client
import pandas as pd
import mysql.connector
from MySQL_client import MySQL_client
import time,datetime

# alaric0001_macd
Huobi_Access_Key = ''
Huobi_Secret_Key = ''

is_proxies = False

contract_type = 'USDT_Margined_Swap'
contract_code = 'BTC-USDT'
contract_period = '1min'

db_name = contract_type
table_name = 'BTC_USDT_1min'

from_time = '2021-03-29 00:00:00'
to_time =   '2021-04-11 00:00:00'
frequency = 60 * 60 * 24 * 1

class Huobi_data_crawler():

    def __init__(self):
        # Swap client instance
        self.huobi_swap_client = Huobi_Swap_Client(Access_Key=Huobi_Access_Key,
                                                   Secret_Key=Huobi_Secret_Key,
                                                   is_proxies=is_proxies)
        self.mydb = mysql.connector.connect(
            host = 'localhost',                              # 数据库主机地址
            user = 'root',                                   # 数据库用户名
            passwd = '',                         # 数据库密码
            auth_plugin = 'mysql_native_password'            # 密码插件改变
        )
        self.mycursor = self.mydb.cursor()
        self.mysql_client = MySQL_client()

    # 分钟级别数据需要1天内1天爬
    # 小时数据可1次爬1个月
    def get_k_lines(self, from_time, to_time, contract_code = contract_code,period = contract_period):
        self.create_table()
        from_timestamp = self.transfer_datetime_to_timestamp(datetime = from_time)
        to_timestamp = self.transfer_datetime_to_timestamp(datetime = to_time)

        raw_data = self.huobi_swap_client.get_k_lines(contract_code = contract_code,
                                                      from_time = from_timestamp,
                                                      to_time = to_timestamp,
                                                      period = period)
        # print(raw_data)
        column_names = ('Datetime', 'high', 'open', 'low','close','amount','vol','trade_turnover','count')
        for i in raw_data['data']:
            columnn_values = (
                self.transfer_timestamp_to_datetime(i['id']),
                float(i['high']),
                float(i['open']),
                float(i['low']),
                float(i['close']),
                float(i['amount']),
                int(i['vol']),
                float(i['trade_turnover']),
                int(i['count'])
            )
            # print(column_names)
            # print(columnn_values)
            self.mysql_client.insert_data_line(db_name=db_name,
                                               table_name=table_name,
                                               column_names=column_names,
                                               columnn_values=columnn_values)
            # print(raw_data)
        return raw_data

    def create_db(self):
        self.mycursor.execute('CREATE DATABASE IF NOT EXISTS ' + db_name )

    def create_table(self):
        self.create_db()
        self.mycursor.execute('USE ' + db_name)
        execute_info = f'''CREATE TABLE IF NOT EXISTS `{table_name}` (
                            `Datetime` DATETIME,
                            `high` FLOAT,
                            `open` FLOAT,
                            `low` FLOAT,
                            `close` FLOAT,
                            `amount` FLOAT,
                            `vol` INT,
                            `trade_turnover` FLOAT,
                            `count` INT,
                            PRIMARY KEY ( `Datetime` )
                            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
                        '''
        self.mycursor.execute(execute_info)

    def transfer_timestamp_to_datetime(self,timestamp):
        datetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(timestamp))
        return datetime

    def transfer_datetime_to_timestamp(self,datetime):
        now=time.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        timestamp = int(time.mktime(now))
        return timestamp

    def split_time_ranges(self, from_time, to_time, frequency):
        from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
        time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
        if to_time not in time_range:
            time_range.append(to_time)
        time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
        time_ranges = []
        for item in time_range:
            f_time = item
            t_time = (datetime.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=frequency))
            if t_time >= to_time:
                t_time = to_time.strftime("%Y-%m-%d %H:%M:%S")
                time_ranges.append([f_time, t_time])
                break
            time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
        return time_ranges


if __name__ == '__main__':
    test = Huobi_data_crawler()

    time_list = test.split_time_ranges(from_time= from_time,
                                       to_time= to_time,
                                       frequency= frequency)
    print(time_list)
    for i in time_list:
        test.get_k_lines(from_time=i[0], to_time =i[1])



