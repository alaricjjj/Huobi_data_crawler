import mysql.connector

class MySQL_client():

    def __init__(self):

        self.mydb = mysql.connector.connect(
            host = 'localhost',                              # 数据库主机地址
            user = 'root',                                   # 数据库用户名
            passwd = '',                         # 数据库密码
            auth_plugin = 'mysql_native_password'            # 密码插件改变
        )
        self.mycursor = self.mydb.cursor()

    def create_database(self, dn_name):
        self.mycursor.execute('CREATE DATABASE ' + dn_name)

    def check_all_db(self):
        self.mycursor.execute('SHOW DATABASES')
        for x in self.mycursor:
            print(x[0])

    def create_table(self,db_name, table_name, column_names):

        self.mycursor.execute('USE ' + db_name)

        column_names_input = '(id INT AUTO_INCREMENT PRIMARY KEY'
        for i in column_names:
            column_names_input += f''', {i} VARCHAR(255)'''
        column_names_input += ')'

        self.mycursor.execute('CREATE TABLE ' + table_name + ' ' + column_names_input)

    def insert_data_line(self, db_name, table_name, column_names, columnn_values ):

        self.mycursor.execute('USE ' + db_name)

        column_names_str = ', '.join(column_names)
        column_names_str = '('+column_names_str+')'

        excute_info = 'INSERT ignore INTO ' + table_name + ' ' + column_names_str + ' ' + 'VALUES' + ' ' + str(columnn_values)+';'
        print(excute_info)
        self.mycursor.execute(excute_info)
        self.mydb.commit()    # 数据表内容有更新，必须使用到该语句

    def insert_data_multi(self, db_name, table_name, column_names, columnn_values_list):

        self.mycursor.execute('USE ' + db_name)

        column_names_str = ', '.join(column_names)
        column_names_str = '('+column_names_str+')'

        s_multi = ['%s'] * len(column_names)
        s_str = ', '.join(s_multi)
        s_str = '(' + s_str + ')'

        sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
        sql = f'''INSERT INTO {table_name} {column_names_str} VALUES {s_str}'''

        val = columnn_values_list

        self.mycursor.executemany(sql, val)

        self.mydb.commit()    # 数据表内容有更新，必须使用到该语句

    def read_table(self, db_name, table_name):
        self.mycursor.execute('USE ' + db_name)

        self.mycursor.execute(f'''SELECT * FROM {table_name}''')
        myresult = self.mycursor.fetchall()

        for x in myresult:
            print(x)
            print(type(x[1]),type(x[2]))

    def read_table_by_column(self,db_name, table_name, search_column):
        self.mycursor.execute('USE ' + db_name)

        search_column_str = ', '.join(search_column)

        self.mycursor.execute(f'''SELECT {search_column_str} FROM {table_name}''')

        myresult = self.mycursor.fetchall()

        for x in myresult:
            print(x)

    def read_table_by_where(self, db_name, table_name, column_names, value):
        self.mycursor.execute('USE ' + db_name)

        sql = f'''SELECT * FROM {table_name} WHERE {column_names} ={value} '''

        self.mycursor.execute(sql)

        myresult = self.mycursor.fetchall()

        for x in myresult:
            print(x)

    def delete_row(self, db_name, table_name, column_names, value):

        self.mycursor.execute('USE ' + db_name)

        sql = f'''DELETE FROM {table_name} WHERE {column_names} = %s'''
        na = (value,)

        self.mycursor.execute(sql, na)

        self.mydb.commit()

    def update_data(self,db_name, table_name, column_names, pre_value, final_value):

        self.mycursor.execute('USE ' + db_name)
        sql = f'''UPDATE {table_name} SET {column_names} = %s WHERE {column_names} = %s'''
        val = (final_value, pre_value )

        print(sql)
        self.mycursor.execute(sql, val)

        self.mydb.commit()



if __name__ == "__main__":
    test = MySQL_client()
    # test.create_table(db_name= 'MySQL_client_test',
    #                   table_name= 'btc_test',
    #                   column_names=['datetime', 'h', 'o', 'l', 'c'])
    #
    # column_names = ('datetime', 'h', 'o', 'l', 'c')
    # columnn_values = ('2020-01-01',5,6,7,8)
    #
    # test.insert_data_line(db_name='MySQL_client_test',
    #                       table_name='btc_test',
    #                       column_names=column_names,
    #                       columnn_values = columnn_values)

    # test.read_table(db_name='MySQL_client_test',
    #                 table_name='btc_data')


    # test.create_database(dn_name='BTC_klines')
    # test.check_all_db()
    # test.create_table(db_name= 'MySQL_client_test', table_name= 'create_table_test', column_names=['alaric', 'meitong', 'adam', 'albert'])

    # column_names = ('Datetime', 'high', 'open', 'low', 'close', 'amount', 'vol', 'trade_turnover', 'count')
    # columnn_values = ('2021-03-01 00:00:00', 43958, 43467.9, 43276.3, 43571.9, 20377.566, 20377566, 888748638.3932, 72204)
    # test.insert_data_line(db_name='USDT_Margined_Swap',table_name= 'BTC-USDT-60min',column_names=column_names,columnn_values = columnn_values)

    column_names = ('alaric', 'meitong', 'adam', 'albert')
    columnn_values = ('15','16','17','18')
    test.insert_data_line(db_name='MySQL_client_test',table_name= 'create_table_test',column_names=column_names,columnn_values = columnn_values)


    # column_names = ('alaric', 'meitong', 'adam', 'albert')
    # columnn_values_list = [('15','16','17','18'),
    #                        ('25','26','27','28'),
    #                        ('35','36','37','38')
    #                        ]
    # test.insert_data_multi(db_name='MySQL_client_test',table_name= 'create_table_test',column_names=column_names,columnn_values_list = columnn_values_list)

    # test.read_table(db_name='MySQL_client_test',table_name= 'create_table_test')

    # column_names = ('alaric', 'meitong', 'albert')
    # test.read_table_by_column(db_name='MySQL_client_test',table_name= 'create_table_test',search_column= column_names)
    # test.read_table_by_where(db_name='MySQL_client_test',table_name= 'create_table_test', column_names = 'alaric', value = 15)

    # test.delete_row(db_name='MySQL_client_test',table_name= 'create_table_test', column_names = 'alaric', value = 5)
    # test.update_data(db_name='MySQL_client_test',table_name= 'create_table_test', column_names = 'alaric', pre_value='35', final_value=38)

