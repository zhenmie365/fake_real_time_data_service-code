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
            host='rm-bp1y26j8fi5a84hl0.mysql.rds.aliyuncs.com',
            port=3306,
            user='lion_watch_admin',
            passwd='LionWatch2021',
            db='lion_watch_db',
            connect_timeout=5)
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")


def handler(environ, start_response):
    return app(environ, start_response)
