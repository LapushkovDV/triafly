from netdbclient import Connection
import Triafly_API_DeleteDuplicates
import pprint
import pandas as pd
from IPython.display import display
import ssl

import datetime
import re

def get_id_catalog_by_value(catalog, value):
    for one_elem in catalog:
        #print("'",one_Abon_PU[0],"'",type(one_Abon_PU[0]),"'", serial,"'", type(serial))#
        if str(one_elem[0]) == str(value):
            #print('EQUAL')
            return one_elem[1]
    return ''


def _load_excel_update_electroStructureInfo_inTriafly(excel_file):
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)

    catalog_ElectroStructure = 2576576 # справочник Э_структура электросети
    triaflyRegistr_TypeElemElectr = 2576668 #реестр API Э_тип элемента справочника электросети ID

    rspn_TypeElemElectr = triafly_conn.get(triaflyRegistr_TypeElemElectr) ##это Э_тип элемента справочника электросети ID
    print(datetime.datetime.now(),'получили Э_тип элемента справочника электросети ID')
    tip_serial_pu = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'Серийный номер ПУ'))
    tip_abonent = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'Абонент'))


    # df = triafly_conn.get_block(block_or_descrs_list=[10296464,-3], from_set=catalog_ElectroStructure)
    # display(rspn_TypeElemElectr)
    catalog_ElectroStructure_df = triafly_conn.get_set(catalog_ElectroStructure)
    # pprint(catalog_ElectroStructure_df.info())

    # print(datetime.datetime.now(),'получили справочник Э_структура электросети')
    #  show structure catalog_ElectroStructure_df.
    column_list_raw = list(catalog_ElectroStructure_df.columns.values)
    print(datetime.datetime.now(),'catalog_ElectroStructure_columns = ',column_list_raw)
    # for index, row in catalog_ElectroStructure_df.iterrows():
    #     print(datetime.datetime.now(),'catalog_ElectroStructure --------------------------------- ')
    #     for column in column_list_raw:
    #         print(datetime.datetime.now(),'catalog_ElectroStructure column ', column, '| value = ',row[column])


    excel_file_df = pd.read_excel(excel_file, skiprows=range(0), dtype='object')
    column_list_raw = list(excel_file_df.columns.values)
    print('column_list_raw =', column_list_raw)

    # cells_excel_file_df = excel_file_df[(excel_file_df['Тип ячейки'] == 'Ячейка присоединения')] # сначала пойдем по ячейкам только.. т.к. для трансформаторов какая бубуйня в заполнении
    # print('cells_excel_file_df =', cells_excel_file_df)
    for index, row in excel_file_df.iterrows():
        address = ''
        address_elem = str(row['Тип региона'])
        if address_elem!='nan':
            address_elem = address_elem + '.'
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Регион'])
        if address_elem!='nan':
            address_elem = address_elem + ' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Тип района'])
        if address_elem!='nan':
            address_elem = address_elem + '.'
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Район'])
        if address_elem!='nan':
            address_elem = address_elem + ' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Тип города(район)'])
        if address_elem!='nan':
            address_elem = address_elem + '.'
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Город(район)'])
        if address_elem!='nan':
            address_elem = address_elem + ' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Тип нас. пункта'])
        if address_elem!='nan':
            address_elem = address_elem + '.'
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Нас. пункт'])
        if address_elem!='nan':
            address_elem = address_elem + ' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Тип ул.'])
        if address_elem!='nan':
            address_elem = address_elem + '.'
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Улица'])
        if address_elem!='nan':
            address_elem = address_elem + ' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Дом'])
        if address_elem!='nan':
            address_elem = 'д.'+address_elem +' '
        else:
            address_elem = ''
        address = address + address_elem

        address_elem = str(row['Корпус'])
        if address_elem!='nan':
            address_elem = 'к.'+address_elem +' '
        else:
            address_elem = ''
        address = address + address_elem

        ls = row['ЛС']
        fio = row['Название абонента']
        pu_serial = str(row['№ ПУ'])

        _row = catalog_ElectroStructure_df[
            (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == tip_serial_pu)
            & (catalog_ElectroStructure_df['Название'] == pu_serial)]
        # _row = catalog_ElectroStructure_df[(catalog_ElectroStructure_df['Название'] == pu_serial)]


        print('*---------------------------')
        # print('_row[Э_тип элемента справочника электросети]=',_row['Э_тип элемента справочника электросети'])
        print('tip_serial_pu=',tip_serial_pu)
        print('pu_serial=',pu_serial)
        parent = _row['prt']
        parentlst = parent.values.tolist()
        abonentid = ''
        for elem in parentlst:
            abonentid = str(int(elem))
        # parentlst = parent[1]
        # print('type parent=',type(parentlst))
        # print('parent=', parentlst)
        # print('abonentid=',abonentid)
        # print('ls=',ls)
        print('fio=',fio)
        # print('address=',address)

        if _row.empty:
            s = 1
        else:
            if abonentid !='':
                print('update ')
                triafly_conn.update_objects({str(abonentid): {
                                                'Э_Структура Адрес': address,
                                                'Э_Структура ФИО':fio,
                                                'Э_Структура ЛС':ls
                                                }
                                            }
                                           )

    # if cur_id = header_row['id'].values[0]
    # print('-----')
    # print('cur_id = ',cur_id)
    # row_1 = catalog_ElectroStructure_df[(catalog_ElectroStructure_df['prt'] == cur_id)]
    # print('row_1 =', row_1)
# file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240312\Копия Стандартный опросный лист.xlsx'
file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240320\Копия Копия ФИО, ЛС.xlsx'

_load_excel_update_electroStructureInfo_inTriafly(file)
