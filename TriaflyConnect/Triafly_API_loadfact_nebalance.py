from netdbclient import Connection
# import Triafly_API_DeleteDuplicates
import pandas as pd
from IPython.display import display
import ssl
import os
import datetime
import time
import pprint
import numpy as np
import openpyxl

# Установим параметры подключения к Триафлай и "координаты" хранения времени последней загрузки данных
def get_value_catalog_by_id(catalog, id):
    # print('get_value_catalog_by_id id = ', id)
    for one_elem in catalog:
        # print("'",one_Abon_PU[0],"'",type(one_Abon_PU[0]),"'", serial,"'", type(serial))#
        if int(one_elem[1]) == int(id):
            #print('EQUAL')
            return one_elem[0]
    return ''

def get_id_catalog_by_value(catalog, value):
    for one_elem in catalog:
        #print("'",one_Abon_PU[0],"'",type(one_Abon_PU[0]),"'", serial,"'", type(serial))#
        if str(one_elem[0]) == str(value):
            #print('EQUAL')
            return one_elem[1]
    return ''

def get_info_elem_from_registry( _registry, serial, npp):
    for one_elem in _registry:
        #print("'",one_Abon_PU[0],"'",type(one_Abon_PU[0]),"'", serial,"'", type(serial))#
        # print('strdate = ', strdate)
        # print('one_Abon_PU[0] =', one_Abon_PU[0])
        # print('one_Abon_PU[1] =', one_Abon_PU[1])
        # curDate = datetime.datetime.strptime(strdate, "%d.%m.%Y").date()
        # begDate = datetime.datetime.strptime(one_elem[0], "%d.%m.%Y").date()
        # endDate = datetime.datetime.strptime(one_elem[1], "%d.%m.%Y").date()

        if  (str(one_elem[npp]) == str(serial)):
            #print('EQUAL')
            return one_elem

def getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name):
    # print('getnamecatalogtosearch element_name =', element_name, 'parentid=',parentid)
    element_name = str(element_name)
    if parentid == '':
        _row = registry_ElectroStructure_df[
            ((registry_ElectroStructure_df['Э_Структура электросети'].isna()) & (
                    registry_ElectroStructure_df['Э_структура электросети варианты названия в опросном листе'].str.contains(element_name, regex=False)))]
    else:
        _row = registry_ElectroStructure_df[
            ((registry_ElectroStructure_df['Э_Структура электросети'] == parentid) & (
                    registry_ElectroStructure_df['Э_структура электросети варианты названия в опросном листе'].str.contains(element_name, regex=False)))]
    if _row.empty:
        return element_name, False
    else:
        return _row['Название'].values[0], True

def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False
def _load_excel_nebalance_toTriafly(excel_file):
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)
    print('_load_excel_nebalance_toTriafly')

    print(datetime.datetime.now(), "Читатем EXCEL-файл", excel_file)
    excel_file_df=pd.read_excel(excel_file, skiprows=range(5), dtype='object')
    print(datetime.datetime.now(),"Прочитан EXCEL-файл",excel_file)
    # pprint(excel_file_df.info())

    wookbook = openpyxl.load_workbook(excel_file)
    # Define variable to read the active sheet:
    worksheet = wookbook.active
    # Iterate the loop to read the cell values
    cell_with_date = worksheet['B3'].value

    strdate = datetime.datetime.strptime(cell_with_date[12:22], "%d.%m.%Y").date().strftime("%Y-%m-%d")
    print(strdate)


    pokazatelDataPokaz = 474283  # Э_дата показаний
    registry_nebalance = 27864053 # Э_небалансы для загрузки API
    catalog_ElectroStructure = 2576576 # справочник Э_структура электросети
    triaflyRegistr_TypeElemElectr = 2576668  # реестр API Э_тип элемента справочника электросети ID
    triaflyRegistr_ElectroStructure = 2605550  # реестр Э_Структура сети полная

    print(datetime.datetime.now(), 'пытаемся получить Э_тип элемента справочника электросети ID')
    rspn_TypeElemElectr = triafly_conn.get(triaflyRegistr_TypeElemElectr) ##это Э_тип элемента справочника электросети ID
    print(datetime.datetime.now(),'получили Э_тип элемента справочника электросети ID')

    print(datetime.datetime.now(), 'пытаемся получить справочник Э_структура электросети')
    catalog_ElectroStructure_df = triafly_conn.get_set(catalog_ElectroStructure)
    print(datetime.datetime.now(),'получили справочник Э_структура электросети')
    # catalog_TipPu_df = triafly_conn.get_set(catalog_TipPu)
    print(datetime.datetime.now(), 'пытаемся получить registry_ElectroStructure_df')
    registry_ElectroStructure_df = triafly_conn.get_registry(triaflyRegistr_ElectroStructure)
    print(datetime.datetime.now(), 'получили registry_ElectroStructure_df')




    #  show structure catalog_ElectroStructure_df.
    # column_list_raw = list(catalog_ElectroStructure_df.columns.values)
    # print(datetime.datetime.now(),'catalog_ElectroStructure_columns = ',column_list_raw)
    # for index, row in catalog_ElectroStructure_df.iterrows():
    #     print(datetime.datetime.now(),'catalog_ElectroStructure --------------------------------- ')
    #     for column in column_list_raw:
    #         print(datetime.datetime.now(),'catalog_ElectroStructure column ', column, '| value = ',row[column])

    # show structure catalog_TipPu_df.
    # column_list_raw = list(catalog_TipPu_df.columns.values)
    # print(datetime.datetime.now(),'catalog_ElectroStructure_columns = ',column_list_raw)
    # for index, row in catalog_TipPu_df.iterrows():
    #     print(datetime.datetime.now(),'catalog_TipPu_df --------------------------------- ')
    #     for column in column_list_raw:
    #         print(datetime.datetime.now(),'catalog_TipPu_df column ', column, '| value = ',row[column])

    type_element_filial = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'Филиал'))
    type_element_res = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'РЭС'))
    type_element_ps = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'ПС'))
    type_element_fider = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'Линия/фидер'))
    type_element_tp = int(get_id_catalog_by_value(rspn_TypeElemElectr, 'ТП'))

    # print('type_element_filial=',type_element_filial)
    # print('type_element_res=',type_element_res)
    # print('type_element_ps=',type_element_ps)
    # print('type_element_fider=',type_element_fider)
    # print('type_element_tp=',type_element_tp)


    excel_file_df = excel_file_df.reset_index()
    column_list_raw = list(excel_file_df.columns.values )
    column_list_date_time =[]
    pu_list_filtr_all = []
    pu_list_filtr = []

    column_list_raw = list(excel_file_df.columns.values)
    # print('column_list_raw=',column_list_raw)

    pokazatelData_nebalance = 14757125  # Э_дата показаний
    registry_nebalance = 27864053 # Э_небалансы для загрузки API

    parameter =[{  'param_id': pokazatelData_nebalance
                , 'param_val': [strdate]
                , 'param_index': 0
               }
                ]
    print('parameter= ',parameter)
    registry_nebalance_df = triafly_conn.get_registry(registry_nebalance, parameter)
    # print('registry_nebalance_d', registry_nebalance_df)

    for index, row in excel_file_df.iterrows():
        # print('row', row)
        if pd.isnull(row['Питающий Центр\nВЛ/фидер 10(6) кВ']):
            continue

        filial_name = str(row['ПО '])
        res_name = str(row['Наименование РЭС'])
        ps_name = str(row['Питающий Центр\nПодстанция\n110/35/10(6) кВ'])
        fider_name = str(row['Питающий Центр\nВЛ/фидер 10(6) кВ'])
        tp_cell = str(row['Наименование ТП'])
        tp_list = tp_cell.split("\\")
        tp_name = tp_list[-1]
        if res_name == 'Ясногорский РЭС' or filial_name == 'Ясногорский РЭС':
            #  костыль на ясногорский рэс... структура совсем кривая там
            filial_name ='Тулэнерго'
            res_name = 'Ясногорский РЭС'

        element_name = filial_name
        typeelem = type_element_filial
        parentid = ''
        element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
        if isfound == False:
            print('Не нашли в реестре',element_name)
            continue
        _row = catalog_ElectroStructure_df[
                     ((catalog_ElectroStructure_df['prt'].isna()) &
                      (catalog_ElectroStructure_df['Название'] == element_name) &
                      (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem)
                     )
                    ]
        if _row.empty:
            print('Не нашли в справочнике',element_name)
            continue

        element_name = res_name
        typeelem = type_element_res
        parentid = _row['id'].values[0]
        element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
        if isfound == False:
            print('Не нашли в реестре',element_name)
            continue
        _row = catalog_ElectroStructure_df[
                    (catalog_ElectroStructure_df['prt'] == parentid) &
                    (catalog_ElectroStructure_df['Название'] == element_name) &
                    (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem)]
        if _row.empty:
            print('Не нашли в справочнике', element_name)
            continue

        element_name = ps_name
        typeelem = type_element_ps
        parentid = _row['id'].values[0]
        element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
        if isfound == False:
            print('Не нашли в реестре',element_name)
            continue
        _row = catalog_ElectroStructure_df[
                    (catalog_ElectroStructure_df['prt'] == parentid) &
                    (catalog_ElectroStructure_df['Название'] == element_name) &
                    (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem)]
        if _row.empty:
            print('Не нашли в справочнике', element_name)
            continue

        element_name = fider_name
        typeelem = type_element_fider
        parentid = _row['id'].values[0]
        element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
        if isfound == False:
            print('Не нашли в реестре',element_name)
            continue
        _row = catalog_ElectroStructure_df[
                    (catalog_ElectroStructure_df['prt'] == parentid) &
                    (catalog_ElectroStructure_df['Название'] == element_name) &
                    (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem)]
        if _row.empty:
            print('Не нашли в справочнике', element_name)
            continue

        element_name = tp_name
        typeelem = type_element_tp
        parentid = _row['id'].values[0]
        element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
        if isfound == False:
            print('Не нашли в реестре',element_name)
            continue
        _row = catalog_ElectroStructure_df[
                    (catalog_ElectroStructure_df['prt'] == parentid) &
                    (catalog_ElectroStructure_df['Название'] == element_name) &
                    (catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem)]
        if _row.empty:
            print('Не нашли в справочнике', element_name)
            continue

        print('TP=',element_name,' id=',_row['id'].values[0])
        opros_perc = row['% опроса']
        if str(opros_perc) == 'nan':
            opros_perc = ''
        nebal_kvt = row['Небаланс, кВт*ч']
        if str(nebal_kvt) == 'nan':
            nebal_kvt = ''
        nebal_perc = row['Небаланс, %']
        if str(nebal_perc) == 'nan':
            nebal_perc = ''
        if not is_number(nebal_perc):
            nebal_perc = ''
        tp_id = _row['id'].values[0]
        _row = registry_nebalance_df[(registry_nebalance_df['Э_Структура электросети'] == tp_id)]

        if _row.empty :
            listvalue = [tp_id
                        ,opros_perc
                        ,nebal_kvt
                        ,nebal_perc
                        ,strdate
                        ]
            print('inserting  listvalue = ',listvalue)
            triafly_conn.put([listvalue], 14757084)



# Произведем новую сессию загрузки данных

# file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240315\06.2. ТУ на ПС с показаниями, 30 минут.xlsx'
# _load_excel_toTriafly(file)
print(datetime.datetime.now(),'Обрабатываем файлы "Баланс ТП"')
path_attach = "./attachments/"
path_archive = "./attachments/archive/"




# _load_excel_toTriafly("./attachments/2024-04-05_130248.977648_06. Ту на ПС с показаниями, 30 минут.xlsx")

filedataset = os.listdir(path_attach)
for file in filedataset:
    filefullpath = path_attach+file
    if os.path.isfile(filefullpath):
        if filefullpath.endswith('Баланс ТП.xlsx'):
            print(filefullpath)
            _load_excel_nebalance_toTriafly(filefullpath)
            os.replace(path_attach+file, path_archive+file)



#check_all_dates()

