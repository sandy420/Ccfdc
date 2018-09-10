# @Software: PyCharm
# @Author  : SandyHeng
import datetime
import pymysql
from config import DBCONFIG, SOURCE_TABLE, AIM_TABLES, AIM_DBCONFIG, CITY_CODE, EXTRACT_DAYS


class AimPipeline():

    def __init__(self, log=None):
        self.__aim_conn = pymysql.connect(**AIM_DBCONFIG)
        self.log = log

    def write_loudong(self, loudong):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_BUILDING'], loudong)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def write_project(self, project):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_BASE'], project)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        self.commit()
        return last_id

    def write_salenumber(self, salenumber):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_PRESALE'], salenumber)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        self.commit()
        return last_id

    def repeat_salenumber(self, name):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_PRESALE'], filed='`id`',
                                         where='`presale_license_number` = "{}"'.format(name.strip()))
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def write_unit(self, unit):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_UNIT'], unit)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def write_floor(self, floor):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_FLOOR'], floor)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def write_house(self, house):
        sql = DataBase.create_insert_sql(AIM_TABLES['PROJECT_ROOM'], house)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def update_project(self, project_id, project_data):
        return self.__update(AIM_TABLES['PROJECT_BASE'], project_id, project_data)

    def update_salenumber(self, sale_id, sale_data):
        return self.__update(AIM_TABLES['PROJECT_PRESALE'], sale_id, sale_data)

    def update_build(self, build_id, build_data):
        d = {'presale_license_id': build_data['presale_license_id'],
             'presale_license_number': build_data['presale_license_number'], 'name': build_data['name']}
        print('楼栋:{}-更新'.format(build_data['name']))
        return self.__update(AIM_TABLES['PROJECT_BUILDING'], build_id, d)

    def update_unit(self, unit_id, unit_data):
        return self.__update(AIM_TABLES['PROJECT_UNIT'], unit_id, unit_data)

    def update_floor(self, floor_id, floor_data):
        return self.__update(AIM_TABLES['PROJECT_FLOOR'], floor_id, floor_data)

    def update_room(self, room_id, room_data):
        return self.__update(AIM_TABLES['PROJECT_ROOM'], room_id, room_data)

    def __update(self, table, _id, data):
        sql = DataBase.create_update_sql(table, data, where='`id`={}'.format(_id))
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def update_room_tag(self, room_id, tag):
        sql = 'update `{}` set `house_status`="{}" where `id`={}'.format(AIM_TABLES['PROJECT_ROOM'], tag, room_id)
        last_id, row_count, all_result = DataBase.execute_sql(self.__aim_conn, sql, self.log)
        return last_id

    def rollback(self):
        self.__aim_conn.rollback()

    def commit(self):
        self.__aim_conn.commit()

    def close(self):
        self.__aim_conn.close()


class SourcePipeline():
    def __init__(self, log=None):
        self.__source_conn = pymysql.connect(**DBCONFIG)
        self.log = log

    def get_loupan(self, pid):
        sql = DataBase.create_select_sql(SOURCE_TABLE['LOUPAN'], where='`loupan_id` = "{}"'.format(pid))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result[0]
        return {}

    def get_sale_number(self):
        sql = DataBase.create_select_sql(SOURCE_TABLE['SALENUMBER'], filed='DISTINCT `loupan_id`',
                                         where='`sale_number_send_date`>="{}"'.format(
                                             (datetime.datetime.now() - datetime.timedelta(days=EXTRACT_DAYS)).strftime(
                                                 '%Y-%m-%d')))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_loudong(self, lid):
        sql = DataBase.create_select_sql(SOURCE_TABLE['LOUDONG'],
                                         where="`loupan_id`='{}'".format(lid))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_units(self, lid):
        sql = DataBase.create_select_sql(SOURCE_TABLE['HOUSE'], filed="DISTINCT `unit_number`",
                                         where='`loudong_id`="{}"'.format(lid))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_floor(self, lid, unit):
        sql = DataBase.create_select_sql(SOURCE_TABLE['HOUSE'], filed="DISTINCT `floor`",
                                         where='`loudong_id`="{}" and `unit_number`="{}"'.format(lid, unit))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_house(self, lid, unit, floor):
        sql = DataBase.create_select_sql(SOURCE_TABLE['HOUSE'],
                                         where='`loudong_id`="{}" and `unit_number`="{}" and `floor`="{}"'.format(lid,
                                                                                                                  unit,
                                                                                                                  floor))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def get_salenumbers(self, lid):
        sql = DataBase.create_select_sql(SOURCE_TABLE['SALENUMBER'], where='`loupan_id`="{}"'.format(lid))
        last_id, row_count, all_result = DataBase.execute_sql(self.__source_conn, sql, self.log)
        if row_count > 0:
            return all_result
        return []

    def close(self):
        self.__source_conn.close()


class RepeatPipeline():
    def __init__(self, log=None):
        self.__repeat_conn = pymysql.connect(**AIM_DBCONFIG)
        self.log = log

    def get_build_id(self, name):
        where = '`{}`.`id`=`{}`.`id` and `name`="{}" and `city`="{}"'.format(AIM_TABLES["PRODUCT"],
                                                                             AIM_TABLES["BUILDING"], name.strip(),
                                                                             CITY_CODE)
        sql = DataBase.create_select_sql('{}`,`{}'.format(AIM_TABLES["PRODUCT"], AIM_TABLES["BUILDING"]),
                                         filed='`{}`.`id`'.format(AIM_TABLES["BUILDING"]), where=where)
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return 'null'

    def repeat_project(self, spider_base_id):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_BASE'], filed='`id`',
                                         where='`spider_base_id` = "{}"'.format(spider_base_id.strip()))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def repeat_salenumber(self, name):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_PRESALE'], filed='`id`',
                                         where='`presale_license_number` = "{}"'.format(name.strip()))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def repeat_loudong(self, spider_building_id):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_BUILDING'], filed='`id`',
                                         where='`spider_building_id`="{}"'.format(spider_building_id))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def repeat_unit(self, build_id, unit_name):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_UNIT'], filed='`id`',
                                         where='`name`="{}" and `project_building_id`={}'.format(unit_name.strip(),
                                                                                                 build_id))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def repeat_floor(self, unit_id, floor_name):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_FLOOR'], filed='`id`',
                                         where='`name`={} and `project_unit_id`={}'.format(floor_name,
                                                                                           unit_id))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def repeat_house(self, spider_room_id):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_ROOM'], filed='`id`',
                                         where='`spider_room_id`="{}"'.format(spider_room_id))
        last_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('id')
        return None

    def get_batch(self):
        sql = DataBase.create_select_sql(AIM_TABLES['PROJECT_BASE'], filed='max(`batch`) as `batch`')
        ast_id, row_count, all_result = DataBase.execute_sql(self.__repeat_conn, sql, self.log)
        if row_count > 0:
            return all_result[0].get('batch')
        return 0

    def close(self):
        self.__repeat_conn.close()


class DataBase():
    @classmethod
    def create_insert_sql(cls, table, write_dict):
        """
        生成insert sql语句
        :param table: 表名
        :param write_dict: 需要写入数据库的字典
        :return: 一个insert语句
        """
        base_sql = "insert into `%s`%s VALUES %s;"
        pre = "(%s)"
        end = "(%s)"
        pre_str = ''
        end_str = ''
        for item in write_dict.items():
            pre_str += '`%s`,' % item[0]
            if isinstance(item[1], int):
                end_str += '%d,' % item[1]
            elif isinstance(item[1], float):
                end_str += '%f,' % item[1]
            elif isinstance(item[1], str):
                _tmp = item[1].strip()
                if len(_tmp) == 0:
                    _tmp = 'null'
                end_str += '"%s",' % _tmp
            else:
                end_str += '"%s",' % item[1]
        pre_str = pre_str[:-1]
        end_str = end_str[:-1]
        end_str = end_str.replace('"null"', 'null')
        sql = base_sql % (table, pre % pre_str, end % end_str)
        return sql

    @classmethod
    def create_select_sql(cls, table, filed="*", where="1=1"):
        """
        创建简单查询语句
        :param table:表名
        :param filed: 字段列表 "*" or "`id`[,`url`,...]"
        :param where: 查询条件 "1=1" or "`id`=10 [and ....]"
        :return: 一个查询语句
        """
        sql = "select %s from `%s` where %s;" % (filed, table, where)
        return sql

    @classmethod
    def execute_sql(cls, conn, sql, log):
        conn.ping(reconnect=True)
        if log:
            log.write_log(sql)
        cur = conn.cursor()
        cur.execute(sql)
        last_id = cur.lastrowid
        row_count = cur.rowcount
        all_result = cur.fetchall()
        return last_id, row_count, all_result

    @classmethod
    def create_update_sql(cls, table_name, item, where):
        if item.get('create_time', '123') != '123':
            del item['create_time']
        base_sql = 'update `%s` set %s where %s;'
        e = ''
        for it in item.items():
            if isinstance(it[1], int):
                e += "`%s`=%d," % (it[0], it[1])
            elif isinstance(it[1], float):
                e += "`%s`=%f," % (it[0], it[1])
            elif isinstance(it[1], str):
                _tmp = it[1].strip()
                if len(_tmp) == 0:
                    _tmp = 'null'
                e += '`%s`="%s",' % (it[0], _tmp)
            else:
                e += '`%s`="%s",' % (it[0], it[1])
        e = e[:-1]
        e = e.replace('"null"', 'null')
        return base_sql % (table_name, e, where)
