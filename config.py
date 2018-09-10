# @File    : config
# @Software: PyCharm
# @Author  : SandyHeng
import pymysql

# 数据源数据库配置
DBCONFIG = {
    'host': ' ',
    'port': 3306,
    'user': ' ',
    'password': ' ',
    'db': " ",
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}

# Logs日志目录
LOG_DIR = './logs/'

# 城市代码（可在 http://www.ip138.com/post/ 查询）
CITY_CODE = '028'

# 写入目标表
AIM_TABLES = {
    'PROJECT_BASE': 't_base',
    'PROJECT_BUILDING': 't_building',
    'PROJECT_FLOOR': 't_floor',
    'PROJECT_PRESALE': 't_presale_license',
    'PROJECT_ROOM': 't_room',
    'PROJECT_UNIT': 't_unit',
    'PRODUCT': 'product',
    'BUILDING': 'building'
}

# 数据来源表
SOURCE_TABLE = {
    "SALENUMBER": 'salenumber_copy',
    "LOUPAN": 'loupan_copy',
    "LOUDONG": 'loudong_copy',
    "HOUSE": 'house_copy'
}

# 目标数据库配置
AIM_DBCONFIG = {'host': ' ',  # 默认127.0.0.1
             'user': ' ',
             'password': ' ',
             'port': 3306,  # 默认即为3306
             'database': ' ',
             'charset': 'utf8',  # 默认即为utf8
             'cursorclass': pymysql.cursors.DictCursor
             }

EXTRACT_DAYS=30