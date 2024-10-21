# блок логирования
import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_comparison_msk.log", filemode="w", format="%(asctime)s %(levelname)s %(message)s")
# https://habr.com/ru/companies/wunderfund/articles/683880/   - ссылка на статью логирования
logging.info("Запуск скрипта comparison_msk.py")

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

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import random

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



def corted_auto_sclad_fact(df, columns=['Марка', 'Модель','VIN', 'Дата прихода', 'Дата заказа /контракта', 'Примечание']):
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
        
# собиарем ссылки с сайта
all_links = []
page = 0
try:
    while True:
        url = f'https://www.sim-autopro.ru/cars/?filter_auto_marka=&filter_auto_model=&filter_auto_kpp=&filter_price_from=&filter_price_to=&filter_auto_god_from=&filter_auto_god_to=&filter_auto_tip_dvigatela=&filter_auto_privod=&filter_auto_tip_kuzova=&filter_auto_moshnost6=75%7C277&filter_auto_probeg=6%7C339090&page={page}'
        session = requests.Session()
        ua = UserAgent()
        headers = {'user-agen': ua.random}
        response = session.get(url, headers=headers, verify=False) # verify=False игнорировать проверку SSL-сертификата
        # time.sleep(random.randint(1,5))
        if response.status_code != 200:
            print(f'Ошибка ответа {response.status_code}')
            logging.error(f"Ошибка ответа - ОШИБКА {response.status_code}", exc_info=True)

        soup = BeautifulSoup(response.text, 'lxml')
        result = soup.find_all('div', class_='usedcar')
        links = [i.find('a', class_='title')['href'] for i in result]
        all_links+=links
        print(page)
        logging.info(f"ЗАПУСК {page}")

        if len(links) < 1:
            break
        page +=1

        print(links)
except Exception as ex_:
    logging.error(f"{ex_} - ОШИБКА", exc_info=True)
    



# блок сбора инфо по карточкам
all_auto = []

try:
    for link_ in all_links:
        session = requests.Session()
        ua = UserAgent()
        headers = {'user-agen': ua.random}
        #time.sleep(random.randint(1,5))
        static_url = f'https://www.sim-autopro.ru{link_}'
        response = session.get(static_url, headers=headers, verify=False) # verify=False игнорировать проверку SSL-сертификата
        if response.status_code != 200:
            print(f'Ошибка ссылки {response.status_code}')
            logging.error(f"Ошибка ответа - ОШИБКА {response.status_code}", exc_info=True)

        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')

        # вся инфа с 1 блока
        items = [i.text.replace("₽","").replace(" ","")  if "₽" in  i.text else i.text for i in soup.find('div', class_='items').find_all('strong')]+[static_url]
        all_auto.append(items)
        print(items)
        logging.info(f"ЗАПУСК {items}")
        

except Exception as ex_:
    print(ex_)
    logging.error(f"{ex_} - ОШИБКА", exc_info=True)
    
logging.info(f"заполняем df данными df_ovp_msc")
df_ovp_msc = pd.DataFrame(all_auto, columns=['Марка', 'Модель', 'Модификация', 'Комплектация', 'Цвет кузова', 'Год выпуска', 'Тип кузова',
                            'Цена', 'Пробег', 'Тип двигателя', 'Объем двигателя', 'Привод', 'Мощность', 'КПП', 'Тип дисков',
                            'Размер дисков', 'Сезонность шин', 'Размер шин', 'Состояние', 'Руль', 'Статус', 'VIN',
                            'Хозяев по ПТС', 'Таможня', 'Ссылка'])



# подтягиваем информацию
logging.info("Считываем базы данных")

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


df_ovp_msc_res = df_ovp_msc


# сравнение выгрузки сайта с df OVP MSC
df_ovp_msc_res['сравнение_цены_мск'] = df_ovp_msc_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_msc))
# сравнение выгрузки сайта с df OVP ЯР
df_ovp_msc_res['сравнение_цены_яр'] = df_ovp_msc_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_yar))
# сравнение выгрузки сайта с df OVP САР
df_ovp_msc_res['сравнение_цены_сар'] = df_ovp_msc_res['VIN'].apply(lambda x: plan_price(x, ovp_fact_sar))

df_ovp_msc_res['сравнение_цены_итог'] = df_ovp_msc_res['сравнение_цены_мск'] + df_ovp_msc_res['сравнение_цены_яр'] + df_ovp_msc_res['сравнение_цены_сар']
df_ovp_msc['разница_сайта_и_плана_отчет'] = df_ovp_msc.apply(lambda df_ovp_msc: (int(df_ovp_msc.Цена) - int(df_ovp_msc.сравнение_цены_итог)), axis=1)

# проевряем были ли выданы авто на сайте - если да, тянем даты выдач
df_ovp_msc_res['дата_выдачи_по_NP'] = df_ovp_msc_res['VIN'].apply(lambda x: vidaca(x, result_temp))

# Проверка по МСК
df_ovp_msc_res['есть_ли_продажа_по_МСК'] = df_ovp_msc_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_msc))
# Проверка по ЯР
df_ovp_msc_res['есть_ли_продажа_по_ЯР'] = df_ovp_msc_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_yar))
# Проверка по САР
df_ovp_msc_res['есть_ли_продажа_по_САР'] = df_ovp_msc_res['VIN'].apply(lambda x: proverka_pustou_vidachi(x, ovp_fact_sar))

# результирующий столбец
df_ovp_msc_res['продажа_отчет'] = df_ovp_msc_res['VIN'].apply(lambda x: sttus_sclad(x, df_ovp_msc))
# результирующий столбец
df_ovp_msc_res['пустоты_отчет'] = df_ovp_msc_res['VIN'].apply(lambda x: nan_cels(x, df_ovp_msc_res, columns=['Марка' , 'Модель', 'Модификация', 'Комплектация', 'Цвет кузова', 'Год выпуска', 
                                                                                                 'Тип кузова', 'Цена', 'Пробег', 'Тип двигателя', 'Объем двигателя', 'Привод', 'Мощность', 'КПП',
                                                                                                 'Тип дисков', 'Размер дисков', 'Сезонность шин', 'Размер шин', 'Состояние', 'Руль', 'Статус',
                                                                                                 'Хозяев по ПТС', 'Таможня']))

# результирующий столбец
df_ovp_msc_res['свод_ошибок'] = df_ovp_msc_res['VIN'].apply(lambda x: all_result(x, df_ovp_msc_res))
# отсекаем нужные столбцы
df_ovp_msc_res = df_ovp_msc_res[df_ovp_msc_res['свод_ошибок']!='-'][['Марка', 'Модель',  'VIN', 'Ссылка', 'свод_ошибок']]
# сохраняем результат проверки данных с сайта
logging.info(f"Сохраняем результат result_ovp_sravnenie_saita.xlsx")
df_ovp_msc_res.to_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\result_ovp_sravnenie_saita.xlsx')

logging.info(f"Блок проверки авто в наличии но отсутсвующих на сайте")
# Блок проверки авто в наличии но отсутсвующих на сайте
auto_na_sclade_msc =  corted_auto_sclad_fact(ovp_fact_msc)
# определеили авто которых нет на сайте
auto_na_sclade_msc['наличие_на_сайте'] = auto_na_sclade_msc['VIN'].apply(lambda x: serch_in_site(x, df_ovp_msc))
# отсортировали авто которых нет на сайте
auto_na_sclade_msc = auto_na_sclade_msc[auto_na_sclade_msc['наличие_на_сайте']=='нет на сайте']  
# конвертируем даты
auto_na_sclade_msc = conversorrrrrr_date(auto_na_sclade_msc, 'Дата прихода')
auto_na_sclade_msc['сегодня'] = date.today().isoformat()
auto_na_sclade_msc['сегодня'] = pd.to_datetime(auto_na_sclade_msc['сегодня'])
auto_na_sclade_msc['Дата заказа /контракта'] = pd.to_datetime(auto_na_sclade_msc['Дата заказа /контракта'])
auto_na_sclade_msc['дней_с_момента_прихода'] = auto_na_sclade_msc['сегодня'] - auto_na_sclade_msc['Дата прихода']

auto_na_sclade_msc = auto_na_sclade_msc.sort_values(by=['дней_с_момента_прихода', 'VIN']) 
auto_na_sclade_msc = auto_na_sclade_msc[['VIN','Марка', 'Модель', 'Дата прихода', 'Дата заказа /контракта', 'дней_с_момента_прихода', 'Примечание']]

# сохраняем результат проверки данных склада с сайтом
logging.info(f"Сохраняем результат result_ovp_sravnenie_sclada_i_saita.xlsx")
auto_na_sclade_msc.to_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\result_ovp_sravnenie_sclada_i_saita.xlsx')


logging.info(f"Скрипт завершен")
