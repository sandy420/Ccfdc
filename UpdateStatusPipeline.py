# @Software: PyCharm
# @Author  : SandyHeng
import pymysql

from config import AIM_TABLES, AIM_DBCONFIG, SOURCE_TABLE, DBCONFIG
from DataPipeline import DataBase
from utils import clean_flag


class UpdateStatusPipeline(object):

    def __init__(self, log):
        self.__aim_conn = pymysql.connect(**AIM_DBCONFIG)
        self.log = log
        self.__source_conn = pymysql.connect(**DBCONFIG)

    def get_update_house(self):
        sql = """select `cp`.`name` as `loupan_name`,`cd`.`name` as `loudong_name`,`ch`.`flag`,
`ch`.`id`,`ch`.`floor`,`ch`.`unit_number`,`ch`.`room_number` from `{}` ch inner join 
`{}` cd on `ch`.`loudong_id` = `cd`.`loudong_id` inner join `{}` cp on 
`cp`.`loupan_id` = `cd`.`loupan_id` where `ch`.`update_flag`=1;""".format(SOURCE_TABLE['HOUSE'],
                                                                          SOURCE_TABLE['LOUDONG'],
                                                                          SOURCE_TABLE['LOUPAN'])
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def update_house_info(self, _time1, **kwargs):
        house_status = clean_flag(kwargs.get('flag'))
        sql = """
                SELECT 
                    tpr.id
                FROM
                    t_project_room tpr 
                LEFT JOIN t_project_floor tpf 
                    ON tpr.`project_floor_id`=tpf.`id`
                LEFT JOIN t_project_unit tpu 
                    ON tpf.`project_unit_id`=tpu.`id`
                LEFT JOIN t_project_building tpbl 
                    ON tpu.`project_building_id`=tpbl.`id`
                LEFT JOIN t_project_base tpbase 
                    ON tpbl.`project_base_id`=tpbase.id
                WHERE tpbase.name = '{}' 
                AND tpbl.`name` = '{}' 
                AND tpu.`name` = '{}' 
                AND tpf.`name` = '{}' 
                AND tpr.`name`='{}';
                """.format(kwargs.get('loupan_name'), kwargs.get('loudong_name'), kwargs.get('unit_number'),
                           kwargs.get('floor'), kwargs.get('room_number'))
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        if row_count == 0:
            return None
        house_id = all_result[0].get('id')
        sql = "update `t_project_room` set `house_status`='{}',`update_time`='{}' where `id`={}".format(house_status,
                                                                                                        _time1,
                                                                                                        house_id)
        DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return house_id

    def update_house_update_flag(self, _id):
        sql = 'update `{}` set `update_flag` = 0 where `id`={}'.format(SOURCE_TABLE["HOUSE"], _id)
        DataBase.execute_sql(self.__source_conn, sql, self.log)
