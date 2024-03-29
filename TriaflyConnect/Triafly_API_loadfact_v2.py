from netdbclient import Connection
# import Triafly_API_DeleteDuplicates
import pandas as pd
from IPython.display import display
import ssl
import os
import datetime


# Установим параметры подключения к Триафлай и "координаты" хранения времени последней загрузки данных

def delete_duplicates(_strdate, triafly_conn_delete):
    # params = [
    #           {  'param_id': pokazatelSerialPU
    #             ,'param_val':[477]
    #             ,'param_index':0
    #           }
    #          ,{
    #               'param_id': pokazatelDataPokaz
    #             , 'param_val': [_strdate]
    #             , 'param_index': 0
    #
    #           }
    #          ]
    triaflyRegistr_electro_structfor_filtr = 14752279
    triaflyRspn_electro_structfor_filtr = triafly_conn_delete.get(triaflyRegistr_electro_structfor_filtr)
    for one_elem in triaflyRspn_electro_structfor_filtr:
        # print('one_elem = ', one_elem)
        triaflyRegistr_FactwithFiltr = 2699280
        pokazatelDataPokaz = 474283  # Э_дата показаний
        params = [{
                      'param_id': pokazatelDataPokaz
                    , 'param_val': [_strdate]
                    , 'param_index': 0
                  },
                 {
                      'param_id': 2576577
                    , 'param_val': [one_elem[1]]
                    , 'param_index': 0

                 }
                 ]

        print(datetime.datetime.now(),'Загружаем реестр API Э_фактические показания 2 для удаления дублей ',one_elem, _strdate)
        rspn_Registr_Fact = triafly_conn_delete.get_registry(triaflyRegistr_FactwithFiltr, params) ##params это реестр серийных номеров приборов учета
        #print(datetime.datetime.now(),"Получен реестр реестр серийных номеров приборов учета") # Название таблицы
        #display(rspn_Registr_Fact)
        print(datetime.datetime.now(), 'Ищем дубликаты по',one_elem,'за', _strdate)
        duplicateRows = rspn_Registr_Fact[rspn_Registr_Fact.duplicated ()]
        #print(duplicateRows)
        #file_name = 'rspn_Registr_Fact.xlsx'

        # saving the excel
        #duplicateRows.to_excel(file_name)
        # print('DataFrame is written to Excel File successfully.')
        #
        #column_list_raw = list(duplicateRows.columns.values )
        #
        list_to_delete = []
        for index, row in duplicateRows.iterrows():
            list_to_delete.append(index)

        if list_to_delete:
            print(datetime.datetime.now(),'Удялем дубликаты по',one_elem,'за',_strdate )
            triafly_conn_delete.delete_objects(list_to_delete)

def check_all_dates():
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)

    triaflyReportDataPokazKolvoAbonent = 3700515

    rspn_ReportDataPokazKolvoAbonent = triafly_conn.get(triaflyReportDataPokazKolvoAbonent)
    print('rspn_ReportDataPokazKolvoAbonent = ', rspn_ReportDataPokazKolvoAbonent)
    for row in rspn_ReportDataPokazKolvoAbonent:
        datepokaz = datetime.datetime.strptime(row[0], "%d.%m.%Y").date()
        #print(datepokaz)
        delete_duplicates(datepokaz.strftime("%Y-%m-%d"), triafly_conn)


def check_one_date(strdate):
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)
    delete_duplicates(strdate, triafly_conn)

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
def _load_excel_toTriafly(excel_file):
    triafly_url = 'http://194.169.192.155:55556/'
    triafly_api_key = '8EBEA456a6'
    ssl._create_default_https_context = ssl._create_unverified_context
    triafly_conn = Connection(triafly_url, triafly_api_key)
    print('_load_excel_toTriafly')

    pokazatelDataPokaz = 474283  # Э_дата показаний
    triaflyRegistr_FactwithFiltr = 2699280
    catalog_ElectroStructure = 2576576 # справочник Э_структура электросети
    triaflyRegistr_TypeElemElectr = 2576668  # реестр API Э_тип элемента справочника электросети ID
    catalog_TipPu = 577 # справочник Э_Тип ПУ
    triaflyRegistr_TipPuFaza = 2605877 # API Э_тип ПУ с фазами и производителем
    triaflyRegistr_TipPuID = 2605958 # API Э_Тип ПУ ID
    triaflyReportPoluchasyID = 498110 # отчет Э_Получасы ID

    triaflyRegistr_Fact2 = 2581738 # Э_фактические показания 2

    catalog_ElectroStructure_df = triafly_conn.get_set(catalog_ElectroStructure)
    print(datetime.datetime.now(),'получили справочник Э_структура электросети')
    # catalog_TipPu_df = triafly_conn.get_set(catalog_TipPu)
    # print(datetime.datetime.now(),'получили справочник Э_Тип ПУ')

    rspn_TipPuID = triafly_conn.get(triaflyRegistr_TipPuID) # API Э_Тип ПУ ID
    print(datetime.datetime.now(), 'получили реестр API Э_Тип ПУ ID')
    rspn_TypeElemElectr = triafly_conn.get(triaflyRegistr_TypeElemElectr) ##это Э_тип элемента справочника электросети ID
    print(datetime.datetime.now(),'получили Э_тип элемента справочника электросети ID')
    rsp_TipPuFaza = triafly_conn.get(triaflyRegistr_TipPuFaza) # API Э_тип ПУ с фазами и производителем
    print(datetime.datetime.now(), 'получили API Э_тип ПУ с фазами и производителем')

    rspPoluchasyID = triafly_conn.get(triaflyReportPoluchasyID) # тут список получасов с их ID
    print(datetime.datetime.now(),"Получен отчет список получасов с их ID")

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

    type_element_serial_pu = int(get_id_catalog_by_value(rspn_TypeElemElectr,'Серийный номер ПУ'))
    type_element_transform = int(get_id_catalog_by_value(rspn_TypeElemElectr,'ПУ трансформатора'))

    excel_file_df=pd.read_excel(excel_file, skiprows=range(4), dtype='object')
    print(datetime.datetime.now(),"Прочитан EXCEL-файл",excel_file)

    excel_file_df = excel_file_df.reset_index()
    column_list_raw = list(excel_file_df.columns.values )
    column_list_date_time =[]

    # оставляем только те колонки где дата и время
    for column in column_list_raw:
        if column[:2].isdigit():
            column_list_date_time.append(column)


    # идем по всем значениям по нужным колонкам
    lpull_list_values = []
    strdate_list = []
    for index, row in excel_file_df.iterrows():
        for column in column_list_date_time:
            strdate = column[:5] + '.' + str(datetime.datetime.now().year)
            if not (strdate in strdate_list):
                strdate_list.append(strdate)
            strtime = column[-5:]
            # print(row['Серийный номер ПУ'])

            typeelem = type_element_serial_pu
            pu_df = catalog_ElectroStructure_df[
                ((catalog_ElectroStructure_df['Название'] == str(row['Серийный номер ПУ'])) & (
                        catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem))]
            if pu_df.empty:
                typeelem = type_element_transform
                pu_df = catalog_ElectroStructure_df[
                    ((catalog_ElectroStructure_df['Название'] == str(row['Серийный номер ПУ'])) & (
                            catalog_ElectroStructure_df['Э_тип элемента справочника электросети'] == typeelem))]

            if pu_df.empty:
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!! не нашли прибор учета!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' , str(row['Серийный номер ПУ']))
                continue

            seriap_pu = pu_df['id'].values[0]
            # print('seriap_pu = ', seriap_pu)
            poluChasy_id = get_id_catalog_by_value(rspPoluchasyID, strtime)
            fact_value = row[column]
            # print('fact_value = ', fact_value)
            if str(fact_value) != 'nan':
                fact_value = float(fact_value)
            else:
                fact_value = ''
            dogovor_power_int = ''
            MoreThenP09 = ''
            MoreThenP = ''
            listvalue=[]
            if typeelem == type_element_serial_pu: # это абонент
                MoreThenP09 = 0
                MoreThenP = 0

                tip_pu_name = get_value_catalog_by_id(rspn_TipPuID, pu_df['Э_Тип ПУ'].values[0])
                # print('tip_pu_name = ',tip_pu_name)
                tippu_faza = get_info_elem_from_registry(rsp_TipPuFaza, tip_pu_name, 0)
                # print('tippu_faza=',tippu_faza)
                # print('tippu_faza[2] =',tippu_faza[2])
                dogovor_power_int = int(tippu_faza[2]) * 5
                if str(fact_value) != '':
                    if fact_value >= 0:
                        if dogovor_power_int > 0:
                            if (fact_value >= dogovor_power_int*0.9) and (fact_value <= dogovor_power_int):
                                MoreThenP09 = 1
                            if (fact_value >= dogovor_power_int):
                                MoreThenP = 1

            # listvalue = [ seriap_pu
            #             , strdate
            #             , poluChasy_id
            #             , fact_value
            #             , typeelem
            #             , dogovor_power_int
            #             , int(MoreThenP09)
            #             , int(MoreThenP)
            #             ]

            # 2576577
            dictValue = {
                 'Э_Структура электросети' : str(seriap_pu)
                ,'Э_дата показаний'  : datetime.datetime.strptime(strdate, "%d.%m.%Y").date().strftime("%Y-%m-%d")
                ,'Э_Получасы'      : poluChasy_id
                # ,'факт кВт'        : fact_value
                ,'Э_тип элемента справочника электросети' : str(typeelem)
                # ,'Э_Выделенная мощность по договору, кВт'  : dogovor_power_int if dogovor_power_int == '' else int(dogovor_power_int)
                  }
            if fact_value != '':
                dictValue['факт кВт'] = fact_value
            if dogovor_power_int != '':
                dictValue['Э_Выделенная мощность по договору, кВт'] = dogovor_power_int
            if MoreThenP09 !='':
                dictValue['Э_Признак показаний абонента ТУ>Р>0,9*ТУ']= MoreThenP09
            if MoreThenP != '':
                dictValue['Э_Признак показаний абонента ТУ>Р'] = MoreThenP


            # print('typeelem =', typeelem)
            # print('listvalue =',listvalue)

            # lpull_list_values.append(listvalue)
            lpull_list_values.append(dictValue)
            # print(lpull_list_values)

            if len(lpull_list_values) > 19999:
                #print(lpull_list_values)
                print('row ', index)
                print(datetime.datetime.now(),'Inserting values')
                # triafly_conn.put(lpull_list_values, triaflyRegistr_Fact2)
                res = triafly_conn.create_objects(lpull_list_values)
                #triafly_conn.create_objects(lpull_list_values)
                print(datetime.datetime.now(), 'end Inserting values')
                lpull_list_values = []
        #print(row[column])
    # print(lpull_list_values)
    if len(lpull_list_values) > 0 :
        print(datetime.datetime.now(), 'Inserting values')
        # triafly_conn.put(lpull_list_values, triaflyRegistr_Fact2)
        res = triafly_conn.create_objects(lpull_list_values)
        print(datetime.datetime.now(), 'end Inserting values')

    # print('Запуск удалений дублей',strdate_list)

    print('Запуск удалений дублей', strdate_list)
    for strdateone in strdate_list:
        delete_duplicates(datetime.datetime.strptime(strdateone, "%d.%m.%Y").date().strftime("%Y-%m-%d"), triafly_conn)
    #print(column_list_date_time)
    #display(rspn_registry_Abon_PU)

    # if rspn_registry_Abon_PU.code == 200:
    #     for row in rspn_registry_Abon_PU:
    #         triafly_conn([],triaflyRegistr_Fact)





# Произведем новую сессию загрузки данных

# file =r'C:\Users\Дмитрий\YandexDisk\Work\Систематика\Энсис АСКУЭ\20240315\06.2. ТУ на ПС с показаниями, 30 минут.xlsx'
# _load_excel_toTriafly(file)
print(datetime.datetime.now(),'Обрабатываем файлы "ТУ на ПС с показаниями"')
path_attach = "./attachments/"
path_archive = "./attachments/archive/"
filedataset = os.listdir(path_attach)


for file in filedataset:
    filefullpath = path_attach+file
    if os.path.isfile(filefullpath):
        if filefullpath.endswith('ТУ на ПС с показаниями, 30 минут.xlsx'):
            print(filefullpath)
            _load_excel_toTriafly(filefullpath)
            os.replace(path_attach+file, path_archive+file)


check_all_dates()

