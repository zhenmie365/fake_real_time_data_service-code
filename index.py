# -*- coding: utf-8 -*-
import logging

import pymysql
from flask_oper import app

logger = logging.getLogger('index')
logger.setLevel(logging.DEBUG)

init_db_con = False


def initializer(context):
    global init_db_con
    try:
        init_db_con = pymysql.connect(
            host='rm-wz9uxu2x6pv2z85by.mysql.rds.aliyuncs.com',
            port=3306,
            user='admin_berg',
            passwd='Black1000',
            db='stock_data',
            connect_timeout=5)
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")


def handler(environ, start_response):
    return app(environ, start_response)
