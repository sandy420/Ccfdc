# @Software: PyCharm
# @Author  : SandyHeng
import datetime
from DataPipeline import AimPipeline, SourcePipeline, RepeatPipeline
from DataUmwandlung import DataUmwandlung

import threading

lock = threading.Lock()
sp1 = SourcePipeline()
loupan_ids = sp1.get_sale_number()
sp1.close()
rp1 = RepeatPipeline()
batch = rp1.get_batch() + 1
rp1.close()


def get_current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def extract_info(loupan_id):
    ap = AimPipeline()
    sp = SourcePipeline()
    rp = RepeatPipeline()
    du = DataUmwandlung(batch)
    project = sp.get_loupan(loupan_id)
    if not project:
        return
    print("楼盘：{}".format(project.get('name')))
    du.set_project_data(project)
    build_id = rp.get_build_id(project.get('name'))
    project_info = du.get_project_data(build_id)

    project_id = rp.repeat_project(project_info.get('spider_base_id'))
    if not project_id:
        project_id = ap.write_project(project_info)

    sales = sp.get_salenumbers(project.get('loupan_id'))
    for salenumber in sales:
        du.set_salenumber_data(salenumber)
        salenumber_info = du.get_salenumber_data(project_id, build_id)
        salenumber_id = rp.repeat_salenumber(salenumber_info.get('presale_license_number'))
        if not salenumber_id:
            salenumber_id = ap.write_salenumber(salenumber_info)

    loudongs = sp.get_loudong(project.get('loupan_id'))
    du.set_loudong_data(loudongs)
    loudong_s = du.get_loudong_data(project_id, build_id)
    try:
        for loudong, _loudong in zip(loudongs, loudong_s):
            print("楼栋:{}".format(_loudong.get('name')))
            if loudong.get('pre_sale_number').strip() == "":
                salenumber_id = 'null'
            else:
                salenumber_id = ap.repeat_salenumber(_loudong.get('presale_license_number'))
                if not salenumber_id:
                    salenumber_id = rp.repeat_salenumber(_loudong.get('presale_license_number'))

            # 如果没有售楼号 则丢弃
            if salenumber_id == 'null' or salenumber_id is None:
                continue

            # 查询是否存在，存在则更新，不存在则插入
            _loudong['presale_license_id'] = salenumber_id
            loudong_id = rp.repeat_loudong(_loudong.get('spider_building_id'))
            if loudong_id:
                ap.update_build(loudong_id, _loudong)
            else:
                loudong_id = ap.write_loudong(_loudong)

            units = sp.get_units(loudong.get('loudong_id'))
            for unit in units:
                unit_dict = {'project_building_id': loudong_id,
                             'name': unit.get('unit_number') if unit.get('unit_number') else 'x',
                             'create_time': get_current_time(), 'update_time': get_current_time(), 'batch': batch}
                unit_id = rp.repeat_unit(loudong_id, unit_dict.get('name'))
                if not unit_id:
                    unit_id = ap.write_unit(unit_dict)
                floors = sp.get_floor(loudong.get('loudong_id'), unit.get('unit_number'))

                for floor in floors:
                    floor_dict = {'project_unit_id': unit_id, 'name': floor.get('floor'),
                                  'create_time': get_current_time(), 'update_time': get_current_time(),
                                  'batch': batch}
                    if floor_dict.get('name', 'x') == 'x':
                        continue

                    floor_id = rp.repeat_floor(unit_id, floor_dict.get('name'))
                    if not floor_id:
                        floor_id = ap.write_floor(floor_dict)

                    houses = sp.get_house(loudong.get('loudong_id'), unit.get('unit_number'), floor.get('floor'))
                    du.set_house_data(houses)
                    house_infos = du.get_house_info(floor_id)
                    for house_info in house_infos:
                        house_id = rp.repeat_house(house_info.get('spider_room_id'))
                        if house_id:
                            ap.update_room_tag(house_id, house_info.get('house_status'))
                        else:
                            ap.write_house(house_info)
            ap.commit()
    except Exception as e:
        raise e
    finally:
        ap.rollback()
    ap.close()
    sp.close()
    rp.close()


def run_thread():
    while True:
        lock.acquire()
        if not loupan_ids:
            lock.release()
            break
        loupan_id = loupan_ids.pop()
        lock.release()
        extract_info(loupan_id.get('loupan_id'))


def main():
    thread_list = []
    for i in range(10):
        t = threading.Thread(target=run_thread)
        thread_list.append(t)
        t.start()

    for t in thread_list:
        t.join()


if __name__ == "__main__":
    print(loupan_ids)
    main()
