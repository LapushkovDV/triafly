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
def getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name):
    print('getnamecatalogtosearch element_name =', element_name)
    element_name = str(element_name)
    if parentid == '':
        _row = registry_ElectroStructure_df[
            ((registry_ElectroStructure_df['Э_Структура электросети'].isna()) & (
                    registry_ElectroStructure_df['Э_структура электросети варианты названия в опросном листе'].str.contains(element_name)))]
    else:
        _row = registry_ElectroStructure_df[
            ((registry_ElectroStructure_df['Э_Структура электросети'] == parentid) & (
                    registry_ElectroStructure_df['Э_структура электросети варианты названия в опросном листе'].str.contains(element_name)))]
    if _row.empty:
        return element_name, False
    else:
        return _row['Название'].values[0], True

def get_insert_catalog(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df, catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, parentid, element_name, rspn_TypeElemElectr, name_type_element,unknown_id, _type_PU, strdate):
    # header_row = catalog_ElectroStructure_df[(catalog_ElectroStructure_df['prt'].isna())]
    id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, name_type_element)

    # 'Название'
    # 'Э_Структура электросети'
    # 'Э_тип элемента справочника электросети '
    # 'Э_структура электросети варианты названия в опросном листе'
    type_PU = _type_PU # 'Э_Тип ПУ'
    unknown_NasPunkt_ID = '' # 'Э_Населенный пункт'
    longitude = ''       # 'Долгота'
    latitude = ''        # 'Широта'
    p_tranf = ''         # 'Э_Пропускная способность сил. тр-та, кВт'
    unknown_TransfAV = ''# 'Э_АВ трансформатора (автоматический выключатель/рубильник)'
    ktt = ''             # 'Э_Коэффициент трансформации тока Ктт'
    unknown_LineAV = ''  # 'Э_АВ линии (автоматический выключатель/рубильник)'

    if name_type_element == 'Трансформатор':
        unknown_TransfAV = unknown_id
        ktt = '--не определено--'
        p_tranf = 0
    if name_type_element == 'Ячейка':
        unknown_LineAV = unknown_id
    if name_type_element == 'ТП':
        unknown_NasPunkt_ID = unknown_id
        longitude = 0 # 'Долгота'
        latitude = 0 # 'Широта'


    element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
    if parentid == '':
        _row = catalog_ElectroStructure_df[
            ((catalog_ElectroStructure_df['prt'].isna()) & (
                    catalog_ElectroStructure_df['Название'] == element_name))]
    else:
        _row = catalog_ElectroStructure_df[
            ((catalog_ElectroStructure_df['prt'] == parentid) & (
                    catalog_ElectroStructure_df['Название'] == element_name))]

    #print('header_row = ', _row)
    print(datetime.datetime.now(),'get_insert_catalog element_name =',element_name)
    #print('get_insert_catalog parentid = ',parentid)
    if _row.empty:  # не нашли элемент
        print(datetime.datetime.now(),'not found, inserting')

        listvalue = [ element_name         # 'Название'
                    , parentid             # 'Э_Структура электросети'
                    , id_type_element      # 'Э_тип элемента справочника электросети '
                    , element_name         # 'Э_структура электросети варианты названия в опросном листе'
                    , type_PU              # 'Э_Тип ПУ'
                    , unknown_NasPunkt_ID  # 'Э_Населенный пункт'
                    , longitude            # 'Долгота'
                    , latitude             # 'Широта'
                    , p_tranf              # 'Э_Пропускная способность сил. тр-та, кВт'
                    , unknown_TransfAV     # 'Э_АВ трансформатора (автоматический выключатель/рубильник)'
                    , ktt                  # 'Э_Коэффициент трансформации тока Ктт'
                    , unknown_LineAV       # 'Э_АВ линии (автоматический выключатель/рубильник)'
                    , strdate
                    ]
        triafly_conn.put([listvalue], triaflyRegistr_ElectroStructure)
        print(datetime.datetime.now(),'done inserting')
        catalog_ElectroStructure_df = triafly_conn.get_set(catalog_ElectroStructure)
        if parentid == '':
            _row = catalog_ElectroStructure_df[
                ((catalog_ElectroStructure_df['prt'].isna()) & (
                            catalog_ElectroStructure_df['Название'] == element_name))]
        else:
            _row = catalog_ElectroStructure_df[
                ((catalog_ElectroStructure_df['prt'] == parentid) & (
                            catalog_ElectroStructure_df['Название'] == element_name))]
    # print('strdate=',strdate)
    # print('_row.index =',_row.index.values[0])
    if strdate != '':
        triafly_conn.update_objects({str(_row.index.values[0]): {'Э_дата установки ПУ': strdate}})

    return catalog_ElectroStructure_df, _row['id'].values[0]

# def get_insert_catalog_like(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df, catalog_ElectroStructure, triaflyRegistr_ElectroStructure, parentid, element_name, element_name_like, id_type_element):
#     # header_row = catalog_ElectroStructure_df[(catalog_ElectroStructure_df['prt'].isna())]
#     element_name, isfound = getnamecatalogtosearch(registry_ElectroStructure_df, parentid, element_name)
#     if parentid == '':
#         _row = catalog_ElectroStructure_df[
#             ((catalog_ElectroStructure_df['prt'].isna()) & (
#                     catalog_ElectroStructure_df['Название'].str.contains(element_name_like)))]
#     else:
#         _row = catalog_ElectroStructure_df[
#             ((catalog_ElectroStructure_df['prt'] == parentid) & (
#                     catalog_ElectroStructure_df['Название'].str.contains(element_name_like)))]
#
#     #print('header_row = ', _row)
#     print('get_insert_catalog element_name =',element_name, 'element_name_like =',element_name_like)
#
#     #print('get_insert_catalog parentid = ',parentid)
#     if _row.empty:  # не нашли элемент
#         print('not found, inserting')
#         listvalue = [element_name, parentid, id_type_element,element_name]
#         triafly_conn.put([listvalue], triaflyRegistr_ElectroStructure)
#         catalog_ElectroStructure_df = triafly_conn.get_set(catalog_ElectroStructure)
#         if parentid == '':
#             _row = catalog_ElectroStructure_df[
#                 ((catalog_ElectroStructure_df['prt'].isna()) & (
#                     catalog_ElectroStructure_df['Название'].str.contains(element_name_like)))]
#         else:
#             _row = catalog_ElectroStructure_df[
#                 ((catalog_ElectroStructure_df['prt'] == parentid) & (
#                     catalog_ElectroStructure_df['Название'].str.contains(element_name_like)))]
#
#     return catalog_ElectroStructure_df, _row['id'].values[0]
def getIdTransUnderTP_withThisCell(catalog_ElectroStructure_df, registry_ElectroStructure_df, _idParent, element_name, typeelemtranf):
    # print('-------def getIdTransUnderTP_withThisCell(  _idParent=',_idParent)
    # print('-------def getIdTransUnderTP_withThisCell(  typeelemtranf=',typeelemtranf)
    # print('-------def getIdTransUnderTP_withThisCell(  element_name = ', element_name)
    typeelemtranf = int(typeelemtranf)
    trnsformator_df = catalog_ElectroStructure_df[
        ((catalog_ElectroStructure_df['prt'] == _idParent) & (
                catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelemtranf))]
    # trnsformator_df = catalog_ElectroStructure_df[
    #     (catalog_ElectroStructure_df['prt'] == _idParent)]

    # display(trnsformator_df)
    for index, row in trnsformator_df.iterrows():
        id = row['id']
        # print('Э_тип элемента справочника электросети = ',row['Э_тип элемента справочника электросети'])
        element_name_1, isFound = getnamecatalogtosearch(registry_ElectroStructure_df, id, element_name)
        if isFound == True:
            # print('isFound == True')
            return id
    return _idParent



def _load_excel_electroStructure_toTriafly(excel_file):
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)

    catalog_ElectroStructure = 2576576 # справочник Э_структура электросети
    triaflyRegistr_ElectroStructure = 2605550 # реестр Э_Структура сети полная
    triaflyRegistr_TypeElemElectr = 2576668 #реестр API Э_тип элемента справочника электросети ID
    triaflyRegistr_TypePU = 2605877 # реестр API Э_тип ПУ
    triaflyReport_TypePU_ID = 2605958 # отчет API Э_Тип ПУ ID
    triaflyReportNasPunkt_ID = 598302  # отчет Э_Населенный пункт ID
    triaflyReportLineAV = 876586  # API Э_АВ линии (автоматический выключатель/рубильник) ID
    triaflyReportTransfAV = 876613  # API Э_АВ трансформатора (автоматический выключатель/рубильник) ID

    rspn_TypeElemElectr = triafly_conn.get(triaflyRegistr_TypeElemElectr) ##это Э_тип элемента справочника электросети ID
    print(datetime.datetime.now(),'получили Э_тип элемента справочника электросети ID')
    rspn_TypePU = triafly_conn.get(triaflyRegistr_TypePU) # реестр API Э_тип ПУ
    print(datetime.datetime.now(),'получили реестр API Э_тип ПУ')
    rspn_TypePU_ID = triafly_conn.get(triaflyReport_TypePU_ID) # API Э_Тип ПУ ID
    print(datetime.datetime.now(),'получили отчет API Э_Тип ПУ ID')

    rspn_ReportNasPunkt_ID = triafly_conn.get(triaflyReportNasPunkt_ID) # отчет Э_Населенный пункт ID
    print(datetime.datetime.now(),'получили отчет Э_Населенный пункт ID')
    rspn_ReportLineAV = triafly_conn.get(triaflyReportLineAV)   # API Э_АВ линии (автоматический выключатель/рубильник) ID
    print(datetime.datetime.now(),'получили отчет API Э_АВ линии (автоматический выключатель/рубильник) ID')
    rspn_ReportTransfAV = triafly_conn.get(triaflyReportTransfAV)  # API Э_АВ трансформатора (автоматический выключатель/рубильник) ID
    print(datetime.datetime.now(),'получили API Э_АВ трансформатора (автоматический выключатель/рубильник) ID')

    unknown_NasPunkt_ID = get_id_catalog_by_value(rspn_ReportNasPunkt_ID, '--не определено--')
    print(datetime.datetime.now(),'получили unknown_NasPunkt_ID = ', unknown_NasPunkt_ID)
    unknown_LineAV = get_id_catalog_by_value(rspn_ReportLineAV, '--не определено--')
    print(datetime.datetime.now(),'получили unknown_LineAV = ', unknown_LineAV)
    unknown_TransfAV = get_id_catalog_by_value(rspn_ReportTransfAV, '--не определено--')
    print(datetime.datetime.now(),'получили unknown_TransfAV = ', unknown_TransfAV)

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

    registry_ElectroStructure_df = triafly_conn.get_registry(triaflyRegistr_ElectroStructure)
    print(datetime.datetime.now(),'Получили реестр Э_Структура сети полная')
    #  show structure registry_ElectroStructure_df
    column_list_raw = list(registry_ElectroStructure_df.columns.values)
    print(datetime.datetime.now(),'registry_ElectroStructure_df_columns = ',column_list_raw)

    # for index, row in registry_ElectroStructure_df.iterrows():
    #     print(datetime.datetime.now(),'catalog_ElectroStructure --------------------------------- ')
    #     for column in column_list_raw:
    #         print(datetime.datetime.now(),'registry_ElectroStructure_df column ', column, '| value = ',row[column])

    excel_file_df = pd.read_excel(excel_file, skiprows=range(2), dtype='object')
    column_list_raw = list(excel_file_df.columns.values)
    print('column_list_raw =', column_list_raw)

    # cells_excel_file_df = excel_file_df[(excel_file_df['Тип ячейки'] == 'Ячейка присоединения')] # сначала пойдем по ячейкам только.. т.к. для трансформаторов какая бубуйня в заполнении
    # print('cells_excel_file_df =', cells_excel_file_df)
    for index, row in excel_file_df.iterrows():
        # for column in column_list_raw:
        #     print('column ', column, '| value = ',row[column])

        # print('row =', row)
        # print('ПС =',row['ПС'])
        _idParent = ''
        # ['ПЭС', '2576583']
        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'ПЭС')
        abonent_name = row['Unnamed: 0']
        element_name = row['Unnamed: 1']
        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df,registry_ElectroStructure_df, catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name,rspn_TypeElemElectr, 'ПЭС', '','','')

        #['ПС', '2576584'],
        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'ПС')
        if str(row['ПС']) != 'nan':
            element_name = row['ПС']
        else:
            element_name = '-'
        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df,catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name, rspn_TypeElemElectr, 'ПС','','','')

        #'Линия/фидер', '2576585']
        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'Линия/фидер')
        if str(row['Линия/фидер']) != 'nan':
            element_name = row['Линия/фидер']
        else:
            element_name = '-'

        if element_name == 'ф. 5': # пошел костыль на кривой EXCEL
            if str(row['ТП']) != 'nan':
                tp_name = row['ТП']
                nums = re.findall(r'\b\d+\b', tp_name)
                if nums:
                    tp_name_like = str(nums[0])
                    if tp_name_like == '11004':
                        element_name = 'ф. 15'

        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df,registry_ElectroStructure_df, catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name, rspn_TypeElemElectr, 'Линия/фидер','','','')

        #['ТП', '2576586']
        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'ТП')
        # element_name_like = ''
        if str(row['ТП']) != 'nan':
            element_name = row['ТП']
        else:
            element_name = '-'
        # if element_name_like != '':
        #     catalog_ElectroStructure_df,_idParent = get_insert_catalog_like(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df, catalog_ElectroStructure,
        #                    triaflyRegistr_ElectroStructure, _idParent, element_name, element_name_like,id_type_element)
        # else:
        #     catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df,catalog_ElectroStructure,
        #                    triaflyRegistr_ElectroStructure, _idParent, element_name,id_type_element)
        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df,catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name,rspn_TypeElemElectr, 'ТП',unknown_NasPunkt_ID,'','')

        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'Ячейка')
        name_type_element = 'Ячейка'
        unknown_id = unknown_LineAV
        if str(row['Тип ячейки.3']) != 'nan':
            if str(row['Тип ячейки.3']) == 'Ячейка нижней обмотки силового трансформатора':
                # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'Трансформатор')
                name_type_element = 'Трансформатор'
                unknown_id = unknown_TransfAV


        #  ['Трансформатор', '2578734'], ['Ячейка', '2576587']
        if str(row['Ячейка.3']) != 'nan':
            element_name = row['Ячейка.3']
            # print('********** element_name = ', element_name)
            _idParent = getIdTransUnderTP_withThisCell(catalog_ElectroStructure_df, registry_ElectroStructure_df,
                                                       _idParent, element_name,
                                                       get_id_catalog_by_value(rspn_TypeElemElectr, 'Трансформатор') )
        else:
            element_name = '-'

        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df, registry_ElectroStructure_df, catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name, rspn_TypeElemElectr, name_type_element,unknown_id,'','')

        if name_type_element != 'Трансформатор':
            element_name = abonent_name
            catalog_ElectroStructure_df, _idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df,
                                                                        registry_ElectroStructure_df,
                                                                        catalog_ElectroStructure,
                                                                        triaflyRegistr_ElectroStructure, _idParent,
                                                                        element_name, rspn_TypeElemElectr, 'Абонент',
                                                                        '', '','')

        # ['Серийный номер ПУ', '2576588'],
        # id_type_element = get_id_catalog_by_value(rspn_TypeElemElectr, 'Серийный номер ПУ')
        if str(row['Серийный номер']) != 'nan':
            element_name = row['Серийный номер']
        else:
            element_name = '-'

        if str(row['Тип ПУ']) != 'nan':
            type_PU_name = row['Тип ПУ']
            type_PU_name =  type_PU_name.replace('Приборы с поддержкой протокола СПОДЭС - ','')
            type_PU_id = get_id_catalog_by_value(rspn_TypePU_ID, type_PU_name)
            if type_PU_id == '':
                listvalue = [type_PU_name, '', 0]
                triafly_conn.put([listvalue], triaflyRegistr_TypePU)
                type_PU_id = get_id_catalog_by_value(rspn_TypePU_ID, type_PU_name)
        else:
            type_PU_name = '-'

        type_element = 'Серийный номер ПУ'
        if name_type_element == 'Трансформатор':
            type_element = 'ПУ трансформатора'

        strdate = ''
        if str(row['Дата установки']) != 'nan':
            strdate = str(row['Дата установки'])
            strdate = datetime.datetime.strptime(strdate, "%d.%m.%Y").date().strftime("%Y-%m-%d")

        catalog_ElectroStructure_df,_idParent = get_insert_catalog(triafly_conn, catalog_ElectroStructure_df,registry_ElectroStructure_df, catalog_ElectroStructure,
                       triaflyRegistr_ElectroStructure, _idParent, element_name, rspn_TypeElemElectr, type_element,'', type_PU_id, strdate)

    # if cur_id = header_row['id'].values[0]
    # print('-----')
    # print('cur_id = ',cur_id)
    # row_1 = catalog_ElectroStructure_df[(catalog_ElectroStructure_df['prt'] == cur_id)]
    # print('row_1 =', row_1)
# file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240312\Копия Стандартный опросный лист.xlsx'
file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240315\Стандартный опросный лист (2).xlsx'

_load_excel_electroStructure_toTriafly(file)
