# @Software: PyCharm
# @Author  : SandyHeng
def clean_flag(flag):
    key = ['06', '07', '03', '04', '12', '27', '01', '11', '02', '08', '09']
    val = ['可售', '已备案', '已抵押', '已办产权', '已预告抵押', '正在受理', '不可售', '已预告', '已查封/已冻结/已限制', '自留', '正在受理']

    for k, v in zip(key, val):
        if k in flag:
            return v

    print('未知flag:{}'.format(flag))
    """
    06  可售
    07  已备案
    03  已抵押
    04  已办产权
    12  已预告抵押
    27  正在受理
    01  不可售
    11  已预告
    02  已查封/已冻结/已限制
    09  正在受理
    08  自留
    """
