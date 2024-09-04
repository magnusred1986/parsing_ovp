# блок логирования
import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_comparison_all.log", filemode="w", format="%(asctime)s %(levelname)s %(message)s")
# https://habr.com/ru/companies/wunderfund/articles/683880/   - ссылка на статью логирования
logging.info("Запуск скрипта comparison_all.py")


import pandas as pd
import openpyxl # для сохранения эксель файлов
from datetime import date
from datetime import datetime

# import os
import warnings
warnings.filterwarnings("ignore")
import csv
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 0)
pd.set_option('chained_assignment', None)
pd.options.display.max_colwidth = 100 # увеличить максимальную ширину столбца
pd.set_option('display.max_columns', None) # макс кол-во отображ столбц


# функция проверки шапки по первой строке если шапка не в первой строке и такм нен упоминания слова vin значит ищем по всем столцам и находим первое вхождение возвращаем строку и обрезаем df
def header_df(df):
    """Преобразование шапки df  
    
    если названия заголовков в таблице не в первой строке, скрипт ищет шапку по ключевому значению vin, 
    удаляет лишние строки и  переопределяет строку в заголовок

    Args:
        df (_type_): df - принимает

    Returns:
        _type_: df - возвращает 
    """
    logging.info(f"{header_df.__name__} - ЗАПУСК")
    try:
        count_col = 0
        for i in df.columns:
            if str(i).lower() == 'vin':
                count_col +=1

            counter_vin = df[i].apply(lambda x: str(x).lower()).str.contains('^vin').sum() # ^ - в регулярке используется для поиска когда слово начинается с 
            name_column = i
            row_number = None
            if counter_vin >0:
                row_number = df[df[name_column].apply(lambda x: str(x).lower())=='vin'].index[0]
                break

        if count_col != 0:
            return df # если шапка в первой строке, ничего не изменяем
        else:
            new_header = df.iloc[row_number] # берем первую строку как заголовок
            df = df[row_number+1:]  # отбрасываем исходный заголовок
            df.rename(columns=new_header, inplace=True) # переименовываем столбцы
            return df
    except Exception as e_:
        print(f'ОШибка {e_}')
        logging.error(f"{header_df.__name__} - ОШИБКА", exc_info=True)
        
        
def plan_price(vin, df):
    """поиск планируемой цены продажи по фину с сайта в фактических файлах ОВП

    Args:
        vin (str): vin номер
        df (df): df региона

    Returns:
        int: сумма
    """
    logging.info(f"запуск функции {plan_price.__name__}")
    try:
        result = sum(df[(df['VIN'].str.contains(vin, na=False)) & (df['Дата выдачи']).isna()]['План цена продажи'])
        return result
    except:
        logging.error(f"{plan_price.__name__} - ОШИБКА", exc_info=True)


def vidaca(vin, df):
    """проверка выданных авто - проверят вин в выдачах по общему файлу temp_result.xlsx

    Args:
        vin (str): _description_
        df (df): temp_result.xlsx

    Returns:
        str_list: возвращает строку сос писком дат
    """
    logging.info(f"ЗАПУСК {vidaca.__name__}")
    try:
        result = [i.split()[1] for i in str(df[df['vin'].str.contains(vin, na=False) & (df['выдача'] == 1)]['дата']).split('\n') if 'дата' not in i.split()[1]]
        if 'name' not in result[0].lower(): return f'Выдавалась по NP {result} но есть на сайте'
        else: '-'
    except:
        logging.error(f"{vidaca.__name__} - ОШИБКА", exc_info=True)
    
    
def proverka_pustou_vidachi(vin, df):
    logging.info(f"ЗАПУСК {proverka_pustou_vidachi.__name__}")
    
    try:
        result = [i.split()[1] for i in str(df[df['VIN'].str.contains(vin, na=False)]['Дата выдачи']).split('\n') if 'дата' not in i.split()[1].lower()]
        if 'NaN' in result:
            return f'на складе'
        elif 'name' in str(result).lower():
            return '-'
        else:
            return result
    except:
        logging.error(f"{proverka_pustou_vidachi.__name__} - ОШИБКА", exc_info=True)
    
    
def sttus_sclad(vin, df):
    """Функция результируещего столбца с данными 
    Проверяет наличе авто по 4 столбцам складам ОВП в ЯР САР МСК
    ['дата_выдачи_по_NP','есть_ли_продажа_по_МСК', 'есть_ли_продажа_по_ЯР', 'есть_ли_продажа_по_САР']

    Args:
        vin (str): vin номер
        df (df): dataframe - по которому будет происходить поиск

    Returns:
        _type_: _description_
    """
    logging.info(f"ЗАПУСК {sttus_sclad.__name__}")
    
    try:
        result = list(df[(df['VIN'].str.contains(vin, na=False))]
                    [['дата_выдачи_по_NP','есть_ли_продажа_по_МСК', 'есть_ли_продажа_по_ЯР', 'есть_ли_продажа_по_САР']].iloc[0])
        result = list(map(str, result)) # все значения в строку, чтоб не было ошибки по non type из=за None
        if 'на складе' in result:
            reg_spr = {1: 'МСК', 2:'ЯР', 3:'САР'}
            reg = [i for i in range(len(result)) if 'на складе' in result[i]]
            return f'в продаже на складе {[reg_spr[i] for i in reg]}'

        elif result.count('-')==3:
            return 'нет на складах ОВП'
        else:
            return f'авто продан, {result[0]}'
    except:
        logging.error(f"{sttus_sclad.__name__} - ОШИБКА", exc_info=True)
    
    
def nan_cels(vin, df, columns=[]):
    """Проверка заполненности ячеек 
    если в каком либо солбце пустое значение, то собирается список с названиями стобцов для каждого VIN
    возвращается список с результатом

    Args:
        vin (str): _description_
        df (df): _description_
        columns (list, optional): список столбцов для проверки на заполнение ['Модель', 'Модификация',...]

    Returns:
        _type_: _description_
    """
    logging.info(f"ЗАПУСК {nan_cels.__name__}")
    
    try:
        result = df[(df['VIN'].str.contains(vin, na=False))][columns]
        res = []
        for i in result.columns:
            if 'nan' in str(result[i].iloc[0]):
                res.append(i)
        
        return f'Не заполнены {res}' if len(res)>=1 else '-'
    except:
        logging.error(f"{nan_cels.__name__} - ОШИБКА", exc_info=True)


def all_result(vin, df):
    """результирующая функция - ищет столбцы по слову "отчет" в имени столбца - таких три 
    и по каждому сверяет параметры. Как результат собирается описание с ошибками, если они есть 

    Args:
        vin (str): _description_
        df (vin): _description_

    Returns:
        _type_: _description_
    """
    logging.info(f"ЗАПУСК {all_result.__name__}")
    
    try:
        result = list(df[(df['VIN'].str.contains(vin, na=False))][[i for i in df.columns if 'отчет' in i]].iloc[0])
        result = list(map(str, result)) # приводим все к строке для исключения ошибок
        svod = []
        if int(result[0])!=0: svod.append(f'Разница цены сайта и планируемой {result[0]}')
        if 'авто продан' in result[1] or 'нет на складах' in result[1]: svod.append(result[1])
        if '-' not in result[2]: svod.append(result[2])
        
        return svod if len(svod)>=1 else '-'
    except:
        logging.error(f"{all_result.__name__} - ОШИБКА", exc_info=True)


def serch_in_site(vin, df):
    """просматривает VIN на складе и проверяет есть ли они на сайте
    
    Args:
        vin (str): _description_
        df (df): _description_

    Returns:
        str: _description_
    """
    
    logging.info(f"ЗАПУСК {serch_in_site.__name__}")
    
    try:
        vin = vin.strip()
        if '/' in vin:
            vin = vin.split('/')[0]
        try:
            res = df[df['VIN'].str.contains(vin, na=False)][['VIN']].iloc[0]
            res = list(map(str, res))[0]
            return res
        except: 
            return f'нет на сайте'
    except:
        logging.error(f"{serch_in_site.__name__} - ОШИБКА", exc_info=True)


def corted_auto_sclad_fact(df, columns=['Марка', 'Модель','VIN', 'Дата прихода', 'Дата заказа /контракта', 'Примечание', 'Площадка']):
    """сортировка фактического склада если нет даты выдачи и есть VIN

    Args:
        df (df): _description_
        columns (str): столбцы которые хотим видеть

    Returns:
        df: _description_
    """
    logging.info(f"ЗАПУСК {corted_auto_sclad_fact.__name__}")
    
    try:
        result = df[(df['Дата выдачи'].isna()) & ((df['VIN'].notna()))][columns]
        return result
    except:
        logging.error(f"{corted_auto_sclad_fact.__name__} - ОШИБКА", exc_info=True)


def conversorrrrrr_date(df, name_date_columns:str):
    """функция для преобразования кривых формат дат, в том числе формата 41253   
      
    Подается df и имя столбца

    Args:
        df (dataframe): df
        name_date_columns (str): имя столбца с датой (который хотим преобразовать)  

    Returns:
        _type_: возварщает преобразованный df  
    """
    logging.info(f"ЗАПУСК {conversorrrrrr_date.__name__}")
    
    try:
        formating = (lambda x: datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(x) - 2))
        df[name_date_columns] = df[name_date_columns].apply(lambda x: str(x).replace('00:00:00','').strip() if '00:00:00' in str(x) else x)
        df[name_date_columns] = df[name_date_columns].apply(lambda x: formating(x) if len(str(x))==5 and str(x)[0] == '4' else x)
        df[name_date_columns] = pd.to_datetime(df[name_date_columns], format='mixed')
        return df
    except:
        logging.error(f"{conversorrrrrr_date.__name__} - ОШИБКА", exc_info=True)
        
def vz_all_in_one(x):
    """Преобразует площадку ЮЗЧери в ЮЗ

    Args:
        x (str): Площадка

    Returns:
        _type_: _description_
    """
    logging.info(f"ЗАПУСК {vz_all_in_one.__name__}")
    
    try:
        x = x.strip()
        if 'ЮЗЧери' in x:
            return 'ЮЗ'
        else:
            return x
    except:
        logging.error(f"{vz_all_in_one.__name__} - ОШИБКА", exc_info=True)


# подтягиваем информацию
logging.info("Считываем базы данных")
#df_ovp_msc = pd.read_csv(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\auto_ovp.csv', delimiter=';', skiprows=0, low_memory=False) # выгрузка с сайта по МСК https://www.sim-autopro.ru
df_ovp_all = pd.read_csv(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\auto_ovp_all.csv', delimiter=';', skiprows=0, low_memory=False) # выгрузка со всех площадок https://sim-auto.ru/

ovp_fact_msc = pd.read_excel(fr'\\SERVER-VM15.SIM.LOCAL\Varsh1$\DPA\Юго-Запад\Payment\ОВП ЮЗ.xlsx', sheet_name='Склад')
ovp_fact_msc = header_df(ovp_fact_msc) # находим шапку

ovp_fact_yar = pd.read_excel(fr'\\SERVERY34.SIM.LOCAL\YCommon$\Старая папка Общая\TRADE-IN\Отчеты для Москвы\Новый ОВП\ОВП-Яр..xlsx', sheet_name='Склад')
ovp_fact_yar = header_df(ovp_fact_yar) # находим шапку

ovp_fact_sar = pd.read_excel(fr'\\SERVER-VM15.SIM.LOCAL\Varsh1$\DPA\САРАТОВ\ОВП-Саратов.xlsx', sheet_name='Склад')
ovp_fact_sar = header_df(ovp_fact_sar) # находим шапку

result_temp = pd.read_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\temp_\temp_result.xlsx', sheet_name='Sheet1')
result_temp = header_df(result_temp) # находим шапку

result_kum = pd.read_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\kum\result_svod.xlsx', sheet_name='Sheet1')
result_kum = header_df(result_kum) # находим шапку


df_ovp_all_res = df_ovp_all
df_ovp_all_res

# сравнение выгрузки сайта с df OVP MSC
df_ovp_all_res['сравнение_цены_мск'] = df_ovp_all_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_msc))
# сравнение выгрузки сайта с df OVP ЯР
df_ovp_all_res['сравнение_цены_яр'] = df_ovp_all_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_yar))
# сравнение выгрузки сайта с df OVP САР
df_ovp_all_res['сравнение_цены_сар'] = df_ovp_all_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_sar))
df_ovp_all_res['сравнение_цены_итог'] = df_ovp_all_res['сравнение_цены_мск'] + df_ovp_all_res['сравнение_цены_яр'] + df_ovp_all_res['сравнение_цены_сар']
df_ovp_all_res['разница_сайта_и_плана_отчет'] = df_ovp_all_res['Цена'] - df_ovp_all_res['сравнение_цены_итог']


# проевряем были ли выданы авто на сайте - если да, тянем даты выдач
df_ovp_all_res['дата_выдачи_по_NP'] = df_ovp_all_res['VIN'].apply(lambda x: vidaca(x, result_temp))


# Проверка по МСК
df_ovp_all_res['есть_ли_продажа_по_МСК'] = df_ovp_all_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_msc))
# Проверка по ЯР
df_ovp_all_res['есть_ли_продажа_по_ЯР'] = df_ovp_all_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_yar))
# Проверка по САР
df_ovp_all_res['есть_ли_продажа_по_САР'] = df_ovp_all_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_sar))
# результирующий столбец
df_ovp_all_res['продажа_отчет'] = df_ovp_all_res['VIN'].apply(lambda x: sttus_sclad(x, df_ovp_all))

# результирующий столбец
df_ovp_all_res['пустоты_отчет'] = df_ovp_all_res['VIN'].apply(lambda x: nan_cels(x, df_ovp_all_res, columns=['Марка' , 'Модель', 'Комплектация', 'Год выпуска', 'Пробег', 'Модификация', 'Объем двигателя', 'Привод', 'Мощность', 'КПП', 'Тип кузова',
                                                                                                             'Цвет кузова', 'Количество владельцев', 'Руль', 'Цена', 'Локация']))

# результирующий столбец
df_ovp_all_res['свод_ошибок'] = df_ovp_all_res['VIN'].apply(lambda x: all_result(x, df_ovp_all_res))

df_ovp_all_res = df_ovp_all_res[df_ovp_all_res['свод_ошибок']!='-'][['Марка', 'Модель',  'VIN', 'Локация', 'Ссылка', 'свод_ошибок']]
# сохраняем результат проверки данных с сайта
logging.info(f"Сохраняем результат result_ovp_all_sravnenie_saita.xlsx")
df_ovp_all_res.to_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\result_ovp_all_sravnenie_saita.xlsx')

logging.info(f"Блок проверки авто в наличии но отсутсвующих на сайте")
auto_na_sclade_msc_ =  corted_auto_sclad_fact(ovp_fact_msc)
auto_na_sclade_yar_ =  corted_auto_sclad_fact(ovp_fact_yar, columns=['Марка', 'Модель','VIN', 'Дата прихода', 'Дата заказа /контракта', 'Примечание', 'Регион'])
auto_na_sclade_yar_ = auto_na_sclade_yar_.rename(columns={'Регион':'Площадка'})
auto_na_sclade_sar_ =  corted_auto_sclad_fact(ovp_fact_sar)
auto_na_sclade_all_ = pd.concat([auto_na_sclade_msc_, auto_na_sclade_yar_, auto_na_sclade_sar_])

# определеили авто которых нет на сайте
auto_na_sclade_all_['наличие_на_сайте'] = auto_na_sclade_all_['VIN'].apply(lambda x: serch_in_site(x, df_ovp_all))
# отсортировади авто которых нет на сайте
auto_na_sclade_all_ = auto_na_sclade_all_[auto_na_sclade_all_['наличие_на_сайте']=='нет на сайте']

# конвертируем даты
auto_na_sclade_all_ = conversorrrrrr_date(auto_na_sclade_all_, 'Дата прихода')

auto_na_sclade_all_['сегодня'] = date.today().isoformat()
auto_na_sclade_all_['сегодня'] = pd.to_datetime(auto_na_sclade_all_['сегодня'])
auto_na_sclade_all_['Дата заказа /контракта'] = pd.to_datetime(auto_na_sclade_all_['Дата заказа /контракта'])


auto_na_sclade_all_['дней_с_момента_прихода'] = auto_na_sclade_all_['сегодня'] - auto_na_sclade_all_['Дата прихода']

auto_na_sclade_all_ = auto_na_sclade_all_.sort_values(by=['дней_с_момента_прихода']) 
auto_na_sclade_all_ = auto_na_sclade_all_[['VIN','Марка', 'Модель', 'Дата прихода', 'Дата заказа /контракта', 'дней_с_момента_прихода', 'Примечание', 'Площадка']]
# конвертируем ЮЗЧери в ЮЗ
auto_na_sclade_all_['Площадка'] = auto_na_sclade_all_.apply(lambda x: vz_all_in_one(x.Площадка), axis=1)


# сохраняем результат проверки данных склада с сайтом
logging.info(f"Сохраняем результат result_ovp_all_sravnenie_sclada_i_saita.xlsx")
auto_na_sclade_all_.to_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\result_ovp_all_sravnenie_sclada_i_saita.xlsx')
