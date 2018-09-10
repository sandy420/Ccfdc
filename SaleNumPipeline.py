# @Software: PyCharm
# @Author  : SandyHeng
import pymysql

from config import SOURCE_TABLE, AIM_TABLES, AIM_DBCONFIG, DBCONFIG
from DataPipeline import DataBase


class SaleNumPipeline(object):

    def __init__(self, log):
        self.log = log
        self.__aim_conn = pymysql.connect(**AIM_DBCONFIG)
        self.__source_conn = pymysql.connect(**DBCONFIG)

    def get_no_salenum_build(self):
        sql = '''select `tb`.`id`,`tb`.`name`,`tp`.`original_name` from `{}` tb inner join 
                `{}` tp on `tb`.`project_base_id`=`tp`.`id` where `tb`.`presale_license_id` is null;'''.format(
            AIM_TABLES['PROJECT_BUILDING'], AIM_TABLES['PROJECT_BASE'])
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_build_salenum(self, community, build_name):
        sql = """select `ld`.`pre_sale_number` from `{}` lp inner join `{}` ld on 
          `lp`.`loupan_id` = `ld`.`loupan_id` where `lp`.`name`= '{}' and `ld`.`name` = '{}'""".format(
            SOURCE_TABLE['LOUPAN'], SOURCE_TABLE['LOUDONG'], community, build_name)
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('pre_sale_number')
        return ''

    def update_salenum(self, sale, salenum_id, _time1, build_id):
        sql = 'update `{}` set `presale_license_id`={},`presale_license_number`="{}",`update_time`="{}" where `id`={}'.format(
            AIM_TABLES['PROJECT_BUILDING'],
            salenum_id, sale, _time1, build_id)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        self.__aim_conn.commit()
        return last_id

    def close(self):
        self.__source_conn.close()
        self.__aim_conn.close()
