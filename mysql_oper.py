# -*- coding: utf-8 -*-
import logging
import datetime
import time
import json
import random
from dateutil.relativedelta import relativedelta

import index

logger = logging.getLogger('pymysql_operation')
logger.setLevel(logging.DEBUG)


def getConnection():
    return index.init_db_con

def query_realtime(order_body):
    """
    input request.json:
    {
        "symbols": "sz000001,sz404040,..."
    } 
    input parameters "symbols" is required, others are optional
    """

    try:
        conn = getConnection()
        with conn.cursor() as cursor:
            print("INFO: Started to query historical stock Data.")

            trade_date = get_latest_trade_date(cursor)

            user_sql = "select * from stockshistoricaldatas where ts_code in %s and trade_date=%s"
            param_list = get_query_params_list(order_body, trade_date)
            cursor.execute(user_sql, param_list)
            data_fetchall = cursor.fetchall()
            

            stock_sql = "select * from stocks where symbol in %s"
            param_list = get_stock_params_list(order_body)
            cursor.execute(stock_sql, param_list)
            stock_fetchall = cursor.fetchall()


            result = transfer_to_realtime_format(data_fetchall, stock_fetchall)
            print(result)
            return result
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def get_latest_trade_date(cursor):
    try:
        calanda_sql = "select * from trade_calendar ORDER BY cal_date desc LIMIT 1"
        cursor.execute(calanda_sql)
        cal_date_fetchone = cursor.fetchone()
        trade_date = cal_date_fetchone[1]
        print(trade_date)

        return trade_date
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def get_random_trade_date(cursor):
    try:
        calanda_sql = "select cal_date from trade_calendar"
        cursor.execute(calanda_sql)
        cal_date_fetchall = cursor.fetchall()
        random_one = random.choice(cal_date_fetchall)
        trade_date = random_one[0]
        print(trade_date)

        return trade_date
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def transfer_to_realtime_format(data_fetchall, stock_fetchall):
    stock_map = {}
    for stock in stock_fetchall:
        stock_map[stock[1]] = stock[2]

    result_list = []
    for hist_e in data_fetchall:
        str_list = []
        symbol = transfer_ts_code(hist_e[0])

        str_list.append(stock_map.get(symbol)) #  stock name
        str_list.append(hist_e[2]) # open
        str_list.append(hist_e[6]) # pre_close

        # 随机计算high和low的数据，是按照open价格的(-0.1, 0.1)来计算
        open_f = float(hist_e[2])
        h = random.uniform(0, 0.1)
        l = random.uniform(-0.1, 0)
        h_f = round(open_f * (1 + h), 3)
        l_f = round(open_f * (1 + l), 3)

        now_price = round(random.uniform(l_f, h_f), 3)
        print(f"now_price : {now_price}")
        str_list.append(str(now_price))
        str_list.append(str(h_f)) # high
        str_list.append(str(l_f)) # low
        list_t = [str(i) for i in range(24)] #  中间的暂时求数目代替
        str_list = str_list + list_t

        time_list = get_now_datetime().split(" ")
        str_list = str_list + time_list
        str_list.append("00")

        data_str = ",".join(str_list)

        full_str = f'var hq_str_{symbol}="{data_str}";\n'

        result_list.append(full_str)
    
    return "".join(result_list)
    


def get_stock_params_list(order_body):
    symbol_list = order_body.get("symbols").split(",")
    return (symbol_list,)

def transfer_userStockData_to_map(data_fetchall):
    print(data_fetchall)
    result_list = []
    keys_list = ["interested_date", "interested_price", "strategy_id", "parameters", "watch_price", "name", "stock_id"]

    for stock in data_fetchall:
        value_tmp_list = list(stock)

        value_tmp_list[0] = value_tmp_list[0].strftime('%Y-%m-%d %H:%M:%S')
        value_tmp_list[3] = json.loads(value_tmp_list[3])
        value_tmp_list[4] = json.loads(value_tmp_list[4])

        dict_tmp = dict(zip(keys_list, value_tmp_list))
        result_list.append(dict_tmp)

    return result_list

def query_stock(data_fetchall, cursor):
    sql = "SELECT * FROM userstockstrategy where id in %s"
    id_list = [e[0] for e in data_fetchall]
    cursor.execute(sql, (id_list, ))
    userStockStrategy_fetchall = cursor.fetchall()

    return userStockStrategy_fetchall

def query_userStockStrategy(data_fetchall, cursor):
    sql = "SELECT * FROM userstockstrategy where id in %s"
    id_list = [e[0] for e in data_fetchall]
    cursor.execute(sql, (id_list, ))
    userStockStrategy_fetchall = cursor.fetchall()

    return userStockStrategy_fetchall

def get_stocks_info(result_list, cursor):
    try:
        user_sql = "SELECT * from stocks WHERE symbol in %s"
        stocks_list = get_stocks_list_from_userStocks(result_list)
        cursor.execute(user_sql, (stocks_list, ))
        stock_data_fetchall = cursor.fetchall()

        if not stock_data_fetchall or list(stock_data_fetchall) == 0:
            print("INFO: there's not mapped stocks DB info data.")
            return result_list

        merge_stocks_info(result_list, stock_data_fetchall)

        return result_list

    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def merge_stocks_info(result_list, stock_data_fetchall):
    print(result_list)
    print(stock_data_fetchall)
    stock_map = {}

    for e in stock_data_fetchall:
        stock_map[e[1]] = e
    
    for m_e in result_list:
        stock_data = stock_map.get(m_e.get("stock_id"))
        if not stock_data:
            continue
        m_e["name"] = stock_data[2]
        m_e["area"] = stock_data[3]
        m_e["industry"] = stock_data[4]
        m_e["market"] = stock_data[5]

def get_stocks_list_from_userStocks(result_list):
    stocks_list = []
    for m_e in result_list:
        stocks_list.append(m_e.get("stock_id"))
    return stocks_list

def transfer_to_map(data_fetchall):
    print(data_fetchall)
    result_list = []
    keys_list = ["user_id", "stock_id", "interested_date", "interested_price", "interested_state", "public_trigger_state"]

    for stock in data_fetchall:
        value_tmp_list = list(stock)

        # remove the index
        value_tmp_list = value_tmp_list[1:]

        value_tmp_list[2] = value_tmp_list[2].strftime('%Y-%m-%d %H:%M:%S')
        dict_tmp = dict(zip(keys_list, value_tmp_list))
        result_list.append(dict_tmp)

    return result_list

def get_query_params_list(order_body, trade_date):

    symbol_list = order_body.get("symbols").split(",")

    result_list =[]
    for symbol_t in symbol_list:
        if not symbol_t:
            continue
        # result_list.append([transfer_symbol(symbol_t), trade_date])
        result_list.append(transfer_symbol(symbol_t))
    
    return (result_list, trade_date)

def update_userStock(order_body):
    """
    update/insert the user selected stock DB data
    input request.json:
    {
        "user_id": "xdg1204",
        "stock_id": "sz000001",
        "interested_price": "12.46",
        "watch_price": "55.3",
        "parameters": {
            "days": 7,
            "method": "average"
        }
        "interested_state": 1/0,
        "public_trigger_state": 1/0,
    }
    input parameters "user_id" and "stock_id" are required, others are optional.
    "interested_price", "watch_price" and "parameters" will be stored in userStockStrategy table, as the default strategy id=1.
    """

    try:
        conn = getConnection()
        with conn.cursor() as cursor:
            user_id = order_body.get("user_id")
            stock_id = order_body.get("stock_id")
            now_time = get_now_datetime()

            user_sql = "SELECT * from userstocks WHERE user_id=%s and stock_id=%s"
            cursor.execute(user_sql, (user_id, stock_id))
            existed_userStock = cursor.fetchone()

            # insert data when there's not existed one, while update it once there's one
            if not existed_userStock:
                result_id = insert_userStock(order_body, now_time, cursor)
                #insert_userStockStrategy(result_id, order_body, cursor)              
            else:
                update_existed_userStock(order_body, existed_userStock, cursor)

            insert_userStock_operation_log(order_body, now_time, cursor)
        
            conn.commit()
        
            return order_body
    except Exception as e:
        conn.rollback()
        logger.error(e)
        print(e)
        raise e

def insert_userStockStrategy(result_id, order_body, cursor):
    try:
        print("INFO: Started to insert userStockStrategy Data.")
    
        insert_sql = "insert into userstockstrategy(userstock_id, strategy_id, parameters, watch_price, map_state) values(%s, %s, %s, %s, %s)"
        parameters_list = get_strategy_parameter_list(userstock_id, order_body)
        cursor.execute(insert_sql, parameters_list)
        last_id = cursor.lastrowid
        print("INFO: Finished to insert userStockStrategy Date.")
        
        return last_id
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def get_strategy_parameter_list(userstock_id, order_body):
    strategy_id = 1
    if None != order_body.get("strategy_id"):
        strategy_id = order_body.get("strategy_id")

    parameters = json.dumps({})
    if None != order_body.get("parameters"):
        parameters = json.dumps(order_body.get("parameters"))

    watch_price = json.dumps({})
    if None != order_body.get("watch_price"):
        watch_price = json.dumps(order_body.get("watch_price"))

    map_state = 1
    if None != order_body.get("map_state"):
        map_state = order_body.get("map_state")

    return [userstock_id, strategy_id, parameters, watch_price, map_state]


def insert_userStock_operation_log(order_body, now_time, cursor):
    try:
        print("INFO: Started to insert userStocksOperationLog Date.")
    
        operation_detail = generate_insert_operation_detail(order_body)
        insert_sql = "insert into userstockoperationlogs(user_id, stock_id, operation_date, operation_detail, operation_price) values(%s, %s, %s, %s, %s)"
        parameters_list = get_update_logs_param_list(order_body, now_time, operation_detail)
        cursor.execute(insert_sql, parameters_list)
        print("INFO: Finished to insert userStocksOperationLog Date.")
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def get_update_logs_param_list(order_body, now_time, operation_detail):
    interested_price = "0"
    if None != order_body.get("interested_price"):
        interested_price = order_body.get("interested_price")
    
    return [order_body.get("user_id"), order_body.get("stock_id"), now_time, operation_detail, interested_price]

def generate_insert_operation_detail(order_body):
    if 1 == order_body.get("interested_state"):
        detail = "user '{user_id}' map stock '{stock_id}' at price '{interested_price}'".format_map(order_body)
    else:
        detail = "user '{user_id}' un-map stock '{stock_id}' at price '{interested_price}'".format_map(order_body)
    return detail

def get_parameter_list(order_body, now_time):
    
    interested_price = "0"
    if None != order_body.get("interested_price"):
        interested_price = order_body.get("interested_price")

    interested_state = False
    if None != order_body.get("interested_state"):
        interested_state = order_body.get("interested_state")

    public_trigger_state = False
    if None != order_body.get("public_trigger_state"):
        public_trigger_state = order_body.get("public_trigger_state")

    return [order_body.get("user_id"), order_body.get("stock_id"), now_time, interested_price, interested_state, public_trigger_state]

def get_update_parameter_list(order_body, existed_userStock):
    print(order_body)
    interested_price = existed_userStock[4]
    if None != order_body.get("interested_price"):
        interested_price = order_body.get("interested_price")

    interested_state = existed_userStock[5]
    if None != order_body.get("interested_state"):
        interested_state = order_body.get("interested_state")

    public_trigger_state = existed_userStock[6]
    if None != order_body.get("public_trigger_state"):
        public_trigger_state = order_body.get("public_trigger_state")

    return [get_now_datetime(), interested_price, interested_state, public_trigger_state, order_body.get("user_id"), order_body.get("stock_id")]


def insert_userStock(order_body, now_time, cursor):
    try:
        print("INFO: Started to insert userStocks Date.")
    
        insert_sql = "insert into userstocks(user_id, stock_id, interested_date, interested_price, interested_state, public_trigger_state) values(%s, %s, %s, %s, %s, %s)"
        parameters_list = get_parameter_list(order_body, now_time)
        cursor.execute(insert_sql, parameters_list)
        last_id = cursor.lastrowid
        print("INFO: Finished to insert userStocks Date.")
        
        return last_id
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def update_existed_userStock(order_body, existed_userStock, cursor):
    try:
        print("INFO: Started to update userStocks Date.")

        update_sql = "update userstocks set interested_date=%s, interested_price=%s, interested_state=%s, public_trigger_state=%s where user_id=%s and stock_id=%s"
        parameters_list = get_update_parameter_list(order_body, existed_userStock)
        print(parameters_list)
        cursor.execute(update_sql, parameters_list)
        print("INFO: Finished to update userStocks Date.")
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def remove_userStock(order_body):
    """
    delete the mapping datas of user selected stocks
    
    input request.json:
    {
        "user_id": "4sdf452xxx",
        "stocks_id": ["sz000404", "sh300902", ....]
    }
    
    parameter "user_id" and "stocks_id" are required.
    """

    try:
        conn = getConnection()
        with conn.cursor() as cursor:
            print("INFO: Started to query userStocks Date.")
            params_list = get_remove_params_list(order_body)

            query_user_sql = "SELECT * from userstocks WHERE user_id=%s"
            cursor.execute(query_user_sql, order_body.get("user_id"))
            data_fetchall = cursor.fetchall()
            print(data_fetchall)

            filter_data = filter_userStocks_data(data_fetchall, order_body.get("stocks_id"))

            if not filter_data or list(filter_data) == 0:
                print("INFO: there's no mapped data in userStocks. Ignore the remove operation.")
                return filter_data

            print(filter_data)

            list_map = transfer_to_map(filter_data)
            print("INFO: Finished to query userStocks Date.")

            print("INFO: Started to remove userStocks Date.")
            delete_sql = "DELETE from userstocks WHERE user_id=%s and stock_id=%s"
            cursor.executemany(delete_sql, params_list)

            insert_remove_userStock_operation_log(list_map, cursor)

            conn.commit()

            print("INFO: Finished to remove userStocks Date.")
            return order_body
    except Exception as e:
        conn.rollback()
        logger.error(e)
        print(e)
        raise e


def filter_userStocks_data(data_fetchall, stocks_list):
    data_list = []

    for data_t in data_fetchall:
        if data_t[2] in stocks_list:
            data_list.append(list(data_t))
    
    return data_list

def insert_remove_userStock_operation_log(list_map, cursor):
    try:
        print("INFO: Started to insert userStocksOperationLog Date.")
        insert_sql = "insert into userstockoperationlogs(user_id, stock_id, operation_date, operation_detail, operation_price) values(%s, %s, %s, %s, %s)"
        parameters_list = get_logs_param_list(list_map)
        cursor.executemany(insert_sql, parameters_list)
        print("INFO: Finished to insert userStocksOperationLog Date.")
    except Exception as e:
        logger.error(e)
        print(e)
        raise e

def get_logs_param_list(list_map):
    now_time =  get_now_datetime()
    param_list = []
    for m in list_map:
        if 1 == m.get("interested_state"):
            detail = "userStock data removed: user '{user_id}' map stock '{stock_id}' at price '{interested_price}'".format_map(m)
        else:
            detail = "userStock data removed: user '{user_id}' un-map stock '{stock_id}' at price '{interested_price}'".format_map(m)
        l_t = [m.get("user_id"), m.get("stock_id"), now_time, detail, m.get("interested_price")]
        param_list.append(l_t)
    return param_list

def get_remove_params_list(order_body):
    user_id = order_body.get("user_id")
    stocks_id = order_body.get("stocks_id")
    print(type(stocks_id))

    param_list = []
    for stock_id in stocks_id:
        t = [user_id, stock_id]
        param_list.append(t)
    
    return param_list

def transfer_symbol(symbol):
    ts = symbol[:2]
    number = symbol[2:]
    result = number + "." + ts.upper()
    return result

def transfer_ts_code(ts_code):
    l = ts_code.split(".")
    return l[1].lower() + l[0]


def get_stocks_dict_from_DB_fetchall(user_stock_fetchall):

    params_list = {}
    
    for userStock in user_stock_fetchall:
        datetime_tmp = userStock[3].strftime('%Y-%m-%d %H:%M:%S')
        timeArray = time.strptime(datetime_tmp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.mktime(timeArray)
        params_list[userStock[2]] = int(timestamp)

    return params_list

def get_stocks_symbol_list(order_body):
    stocks_symbol_list = order_body.get("stocks")

    if not stocks_symbol_list or len(stocks_symbol_list) < 1:
        raise Exception("ERROR: the stocks list in input request is None.")

    return stocks_symbol_list

def get_values_params(user_data_fetchone, stock_data_fetchall):
    user_id = user_data_fetchone[0]

    now_time = get_now_datetime()

    values_params = [[user_id, stock[0], now_time] for stock in stock_data_fetchall]

    return values_params

def get_delete_values_params(user_data_fetchone, stock_data_fetchall):
    user_id = user_data_fetchone[0]

    now_time = get_now_datetime()

    values_params = [[user_id, stock[0]] for stock in stock_data_fetchall]

    return values_params

def get_yesterday():
    now_time = datetime.datetime.now()
    hkl_now_time = now_time + relativedelta(hours=8)
    hkl_yesterday = hkl_now_time - relativedelta(days=1)
    return hkl_yesterday.strftime('%Y%m%d')


def get_now_date():
    """
    因为系统拿到的是英国时间，所以需要加上八小时
    """
    now_time = datetime.datetime.now()
    hkl_now_time = now_time + relativedelta(hours=8)
    return hkl_now_time.strftime('%Y%m%d')

def get_now_datetime():
    """
    因为系统拿到的是英国时间，所以需要加上八小时
    """
    now_time = datetime.datetime.now()
    hkl_now_time = now_time + relativedelta(hours=8)
    return hkl_now_time.strftime('%Y-%m-%d %H:%M:%S')

def transfer_by_DB_data(user_fetchone, stock_fetchall, params_stocks_dict):
    result_list = []

    keys_list = ["userId", "userName", "openId", "stockId", "stockName", "symbol", "stockStatus", "marketId", "createTime"]

    for stock in stock_fetchall:
        value_tmp_list = [user_fetchone[0], user_fetchone[1], user_fetchone[2], stock[0], stock[1], stock[2], stock[3], stock[5], params_stocks_dict.get(stock[0])]
        dict_tmp = dict(zip(keys_list, value_tmp_list))
        result_list.append(dict_tmp)
    
    return result_list

