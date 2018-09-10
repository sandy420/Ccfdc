# @Author  : SandyHeng
# @File    : DataUmwandlung
# @Software: PyCharm
import re
from config import CITY_CODE
from utils import clean_flag


class DataUmwandlung(object):

    def __init__(self, batch):
        self.__project_data = None
        self.__salenumber_data = None
        self.__loudong_data = None
        self.__houses_data = None
        self.__batch = batch

    def set_project_data(self, project):
        self.__project_data = project

    def set_salenumber_data(self, salenumber):
        self.__salenumber_data = salenumber

    def set_loudong_data(self, loudong):
        self.__loudong_data = loudong

    def set_house_data(self, houses):
        self.__houses_data = houses

    def get_project_data(self, building_id):
        d = {}
        d['city_code'] = CITY_CODE
        d['batch'] = self.__batch
        d['name'] = self.__project_data.get('name')
        d['original_name'] = self.__project_data.get('name')
        d['building_id'] = building_id
        d['address'] = self.__project_data.get('addr')
        d['opening_date'] = self.__project_data.get('open_date')
        d['opening_price'] = self.__clean_price(self.__project_data.get('open_price'))
        d['green_rate'] = self.__project_data.get('green_rate')
        d['developer_name'] = self.__project_data.get('developer')
        d['create_time'] = self.__project_data.get('ts')
        d['update_time'] = self.__project_data.get('ts')
        d['spider_base_id'] = self.__project_data.get('loupan_id')
        return d

    def get_salenumber_data(self, project_base_id, building_id):
        d = {}
        d['project_base_id'] = project_base_id
        d['building_id'] = building_id
        d['batch'] = self.__batch
        d['presale_license_number'] = self.__salenumber_data.get('sale_number').strip()
        d['license_date'] = self.__salenumber_data.get('sale_number_send_date')
        d['presale_range'] = self.__salenumber_data.get('pre_sale_range')
        d['housing_number'] = self.__clean_house_count(self.__salenumber_data.get('residential_count'))
        d['house_usage'] = self.__salenumber_data.get('house_useing')
        d['total_area'] = self.__clean_area(self.__salenumber_data.get('pre_sale_area'))
        d['housing_area'] = self.__clean_area(self.__salenumber_data.get('residential_area'))
        d['create_time'] = self.__salenumber_data.get('ts')
        d['update_time'] = self.__salenumber_data.get('ts')
        return d

    def get_loudong_data(self, project_base_id, building_id):
        return_l = []
        for loudong in self.__loudong_data:
            d = {}
            d['presale_license_number'] = loudong.get('pre_sale_number')
            d['name'] = loudong.get('name')
            d['building_id'] = building_id
            d['spider_building_id'] = loudong.get('loudong_id')
            d['batch'] = self.__batch
            d['project_base_id'] = project_base_id
            d['total_number'] = self.__clean_tatol_house(loudong.get('tatol_house'))
            d['housing_number'] = self.__clean_house_count(loudong.get('residential_house'))
            d['total_area'] = self.__clean_area(loudong.get('residential_area'))
            d['pool_area'] = self.__clean_area(loudong.get('pubilc_area'))
            d['create_time'] = loudong.get('ts')
            d['update_time'] = loudong.get('ts')
            return_l.append(d)
            del d
        return return_l

    def get_house_info(self, floor_id):
        return_l = []
        for house in self.__houses_data:
            d = {}
            d['name'] = house.get('room_number')
            d['house_structure'] = house.get('engin_struct')
            d['project_floor_id'] = floor_id
            d['house_declared_usage'] = house.get('house_useing_declare')
            d['room_area'] = self.__clean_area(house.get('house_area'))
            d['pool_area'] = self.__clean_area(house.get('apportion_area'))
            d['house_type'] = house.get('house_type')
            d['balcony_area'] = self.__clean_area(house.get('balcony_area'))
            d['balcony_type'] = house.get('balcony_type')
            d['batch'] = self.__batch
            d['spider_room_id'] = house.get('house_id')
            d['house_status'] = clean_flag(house.get('flag'))
            d['is_seal_up'] = self.__seal(house.get('is_seal'))
            d['is_mortgage'] = self.__mortgage(house.get('is_mortgage'))
            d['presale_price'] = self.__clean_price_re(house.get('pre_sale_price'))
            d['house_planned_usage'] = house.get('house_useing_plan')
            d['building_area'] = self.__clean_area(house.get('building_area'))
            d['storey_height'] = house.get('floor_height')
            d['create_time'] = house.get('ts')
            d['update_time'] = house.get('ts')
            return_l.append(d)
            del d
        return return_l

    def __seal(self, seal):
        if '已查封' in seal:
            return 1
        elif '未查封' in seal:
            return 0
        else:
            return 1

    def __mortgage(self, mortgage):
        if '未' in mortgage:
            return 0
        else:
            return 1

    def __clean_price(self, price):
        if price:
            price = price.replace('\t', '').replace('\n', '').replace('\r', '').replace(' ', '')
            if len(price) != 0:
                return float(price)
            else:
                return 0
        return 0

    def __clean_price_re(self, price):
        price = re.findall(r'(\d+)', price)
        if price:
            return int(price[0])
        return 0

    def __clean_house_count(self, house_count):
        if house_count:
            return int(float(house_count.replace('\t', '').replace('\n', '').replace('\r', '').replace(' ', '')))
        return 0

    def __clean_area(self, area):
        if area:
            area = area.replace('\t', '').replace('\n', '').replace('\r', '').replace(' ', '')
            if len(area) != 0:
                return float(area)
            else:
                return 0
        return 0

    def __clean_tatol_house(self, house):
        tatol_house = re.findall(r'\d+', house)
        if tatol_house:
            return int(tatol_house[0])
        else:
            return 0
