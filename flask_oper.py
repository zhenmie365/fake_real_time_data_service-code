# coding=utf-8
import json

from flask import Flask, request
from mysql_oper import update_userStock, query_realtime, remove_userStock
from responseCode import return_error_result_6004, return_success_result, return_error_result_6003, return_error_result_6002

app = Flask(__name__)

base_path = ''

@app.route('/realtime/query', methods=['GET'])
def query_realtime_data():
    """
    input request.json:
    {
        "symbols": "sz000001,sz404040,..."
    } 
    input parameters "symbols" is required, others are optional
    """

    print("INFO: start to query the userStock data")
    request_body = request.args

    if not request_body or not 'symbols' in request_body:
        return return_error_result_6002([]), 400

    try:
        result = query_realtime(request_body)
    except Exception as e:
        return return_error_result_6003(str(e)), 400 

    if not result:
        return return_error_result_6002([]), 200

    return return_success_result(result), 201

@app.route('/userStock/update', methods=['POST'])
def udpate_user_stock():
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

    print("INFO: start to update the userStock data")
    request_input = request.get_data()
    order_body = json.loads(request_input.decode("utf-8"))

    if not order_body or not 'user_id' in order_body or not "stock_id" in order_body:
        return return_error_result_6002([]), 400

    try:
        result = update_userStock(order_body)
    except Exception as e:
        return return_error_result_6003(str(e)), 400 

    if not result:
        return return_error_result_6002([]), 200

    return return_success_result(result), 201

@app.route('/userStock/remove', methods=['POST'])
def delete_users_selected_stocks():
    """
    delete the mapping datas of user selected stocks
    
    input request.json:
    {
        "user_id": "4sdf452xxx",
        "stocks_id": ["sz000404", "sh300902", ....]
    }
    
    parameter "user_id" and "stocks_id" are required.
    """

    request_input = request.get_data()
    order_body = json.loads(request_input.decode("utf-8"))
    
    if not order_body or not 'user_id' in order_body or not 'stocks_id' in order_body:
        return return_error_result_6002([]), 400

    try:
        result = remove_userStock(order_body)
    except Exception as e:
        return return_error_result_6003(str(e)), 400 

    if not result:
        return return_error_result_6002([]), 200

    return return_success_result(result), 201