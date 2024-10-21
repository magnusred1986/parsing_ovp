import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_COMPARISON_avito_reg.log",filemode="w", format="%(asctime)s %(levelname)s %(message)s")
# https://habr.com/ru/companies/wunderfund/articles/683880/   - ссылка на статью логирования
# filemode="a" дозапись "w" - перезапись
logging.info("Запуск скрипта COMPARISON_avito_reg.py")

import pandas as pd
import random
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 0)
pd.set_option('chained_assignment', None)
pd.options.display.max_colwidth = 100 # увеличить максимальную ширину столбца
pd.set_option('display.max_columns', None) # макс кол-во отображ столбц

from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from fake_useragent import UserAgent
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def header_df(df):
    """Преобразование шапки df  
    
    если названия заголовков в таблице не в первой строке, скрипт ищет шапку по ключевому значению vin, 
    удаляет лишние строки и  переопределяет строку в заголовок

    Args:
        df (_type_): df - принимает

    Returns:
        _type_: df - возвращает 
    """
    logging.info(f"запуск функции {header_df.__name__}")
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
        print(f'Ошибка {e_}')
        logging.error(f"{header_df.__name__} - ОШИБКА", exc_info=True)
        
def search_car(year_auto, probeg_km, price, df_searching, otklonenie_km:float = 0.00, otklonenie_price: float = 0.00):
    """_summary_

    Args:
        year_auto (_type_): _description_           - дата выпуска авто
        probeg_km (_type_): _description_           - пробег авто 
        price (_type_): _description_               - цена авто 
        df_searching (_type_): _description_        - df по которому осуществляем посик
        otlonenie_km (float, optional): _description_. Defaults to 0.00. Не обязательный аргумент - допустимое отклоненеи посика по пробегу
        otlonenie_price (float, optional): _description_. Defaults to 0.00. Не обязательный аргумент - допустимое отклоненеи посика по цене

    Returns:
        _type_: _description_
    """
    logging.info(f"запуск функции {search_car.__name__}")
    
    try:
        year_auto = int(year_auto)
        probeg_km = int(probeg_km)
        price = int(price)
        
        result_vin = df_searching[(df_searching['год выпуска'] == year_auto) & (df_searching['Дата выдачи'].isna()) &
                    ((df_searching['Пробег, км.'] >= probeg_km-(probeg_km*otklonenie_km)) & (df_searching['Пробег, км.'] <= probeg_km+(probeg_km*otklonenie_km))) & 
                    ((df_searching['План цена продажи'] >= price-(price*otklonenie_price)) & (df_searching['План цена продажи'] <= price+(price*otklonenie_price) ))]['VIN'].tolist()

        return result_vin
    except:
        logging.error(f"{search_car.__name__} - ОШИБКА", exc_info=True)
        
def approximately(*args):
    """максимальное кол-во сопадений - принимает неограниченное кол-во вводных данных в list объединяет все в один список
    далее подчитывает кол-во совпажающих значений и выводит значение с максимальным кол-вом совпадений

    Returns:
        _type_: _description_
    """
    logging.info(f"запуск функции {approximately.__name__}")
    
    try:
        from collections import Counter
        res = []
        for i in args:
            res+=i
        res = Counter(res)
        res_sort = sorted(res.items(), key=lambda item: item[1], reverse=True)
        if len(res_sort)>0:
            return res_sort[0][0]
        else:
            return None
    except:
        logging.error(f"{approximately.__name__} - ОШИБКА", exc_info=True)
        
def data_vidach(vin, df):
    """Проверяет выдавался ли авто и когда

    Args:
        vin (_type_): вин авто 
        df (_type_): df по каоторому проверяем (склад ОВП)

    Returns:
        _type_: _description_
    """
    logging.info(f"запуск функции {data_vidach.__name__}")
    
    try:
        import datetime
        res = df[df['VIN'] == vin]['Дата выдачи'].tolist()
        if len(res)>=1:
            return [f"выдавалась {i.date().isoformat()}" if 'nan' not in  str(i) else f"есть в продаже {i}" for i in res]
        else:
            return None
    except:
        logging.error(f"{data_vidach.__name__} - ОШИБКА", exc_info=True)
        
def price_sclad(vin, df, columns='План цена продажи'):
    """возвращает результат колонки по вин номеру авто

    Args:
        vin (_type_): вин номер
        df (_type_): фрейм по которому ищем данные
        columns (str, optional): _description_. колонка результат по которой нужен по умолчанию - 'План цена продажи'

    Returns:
        _type_: _description_
    """
    logging.info(f"запуск функции {price_sclad.__name__}")
    
    try:
        res = df[(df['VIN'].str.contains(str(vin), na=False))&(df['Дата выдачи'].isna())][columns].to_list()
        if len(res)>0:
            return res[0]
        else:
            return res
    except:
        logging.error(f"{price_sclad.__name__} - ОШИБКА", exc_info=True)
    
    
def raznitsa(x1, x2):
    """разность данных

    Args:
        x1 (_type_): _description_
        x2 (_type_): _description_

    Returns:
        _type_: разность
    """
    logging.info(f"запуск функции {raznitsa.__name__}")
    try:
        if isinstance(x2, int):
            return int(x1)-int(x2)
        else:
            return f'нет данных'
    except:
        logging.error(f"{raznitsa.__name__} - ОШИБКА", exc_info=True)
        

def pars_avito(url):
    logging.info(f"запуск функции {pars_avito.__name__}")
    try:
        data_web = []
        
        ua = UserAgent()
        options = webdriver.ChromeOptions()
        options.add_argument(f"user-agent={ua.random}")
        #options.add_argument("--start-fullscreen")                              # во всю ширину экрана
        options.add_argument("--incognito")                                      # режим инкогнито
        options.add_argument("--ignore-certificate-errors") 
        
        with webdriver.Chrome(options=options) as browser:
            browser.get(url)
            url_test = browser.current_url # получаем текущий url
            actions = ActionChains(browser)
            time.sleep(random.randint(5,8))
            if 'm.avito' in url_test: # если открылась версия сайта для мобильного
                time.sleep(random.randint(2,4))
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight);") # скролим страницу
                time.sleep(random.randint(2,4))
                WebDriverWait(browser, poll_frequency=0.01, timeout=120).until(EC.element_to_be_clickable(browser.find_element(By.XPATH, '//*[@id="item_list_with_filters"]/div/div[2]/div[3]/div/a/span/span'))).click()
                max_ = [0]
                while True:
                    try:
                        time.sleep(random.randint(2,4))
                        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);") # скролим страницу
                        height = browser.execute_script("return document.body.scrollHeight") # получим ширину видимой области
                        print(f'Диапазон видимости страницы {height}')
                        if height>max(max_):
                            max_.append(int(height))
                            time.sleep(random.randint(2,4))
                        else:
                            break
                    except:
                        break
                time.sleep(random.randint(5,8)) # для тестирования ставим задержку на 5000 сек, проверяем все элементы
                for i,y in zip(browser.find_elements(By.CLASS_NAME, 'ypnas'), browser.find_elements(By.CLASS_NAME, 'QcCQH')): # ypnas все 
                    link = y.get_attribute('href') # ссылка
                    info = i.find_element(By.CLASS_NAME, 'mav-1k60k4y').text      # инфо об авто
                    price = [i.find_element(By.CLASS_NAME, 'KrcOX').text][0].replace(' ₽','').replace(' ','')
                    location = i.find_element(By.CLASS_NAME, '_5l1R').text
                    print([link, info, price, location])
                    data_web.append([link, info, price, location])
        
            else: # открылось в обычном режиме
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight);") # скролим страницу
                time.sleep(random.randint(2,4))
                WebDriverWait(browser, poll_frequency=0.01, timeout=120).until(EC.element_to_be_clickable(browser.find_element(By.XPATH, '//*[@id="item_list_with_filters"]/div[2]/div[2]/div[2]/div/div[2]/a/span/span'))).click()
                max_ = [0]
                while True:
                    try:
                        time.sleep(random.randint(2,4))
                        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);") # скролим страницу
                        height = browser.execute_script("return document.body.scrollHeight") # получим ширину видимой области
                        print(f'Диапазон видимости страницы {height}')
                        if height>max(max_):
                            max_.append(int(height))
                            time.sleep(random.randint(2,4))
                        else:
                            break
                    except:
                        break
                time.sleep(random.randint(5,8))
                for i,y in zip(browser.find_elements(By.CLASS_NAME, 'iva-item-body-KLUuy'), browser.find_elements(By.CLASS_NAME, 'iva-item-title-py3i_')):
                    link = y.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    info = i.find_element(By.CLASS_NAME, 'iva-item-title-py3i_').text
                    price = [i.find_element(By.CLASS_NAME, 'styles-module-root-bLKnd').text][0].replace(' ₽','').replace(' ','')
                    location = i.find_element(By.CLASS_NAME, 'geo-root-zPwRk').text
                    print([link, info, price, location])
                    data_web.append([link, info, price, location])
        return data_web
    except:
        logging.error(f"{pars_avito.__name__} - ОШИБКА", exc_info=True)

        
logging.info(f"запуск сбора информации с АВИТО по РЕГИОНАМ")


# авито СИМ Автомаркет Регионы
url = 'https://www.avito.ru/brands/sim-yaroslavl/all/avtomobili?src=search_seller_info&sellerId=87dd902d98dd4a07ffabce1c59977ae6'

data_web = pars_avito(url)

data_web_2 = data_web.copy()

logging.info(f"преобразование данных")

try:
    # преобразуем данные
    for i in data_web_2:
        i = [i[0]]+i[1].split(', ')+[i[-2]]+[i[-1]]
        
    # преобразуем данные 
    new_data = []
    for i in data_web_2:
        new_data.append([i[0]] + [j.strip() for j in i[-3].replace(' км','').split(',')] + [i[-2]]+[i[-1]])
    new_data

    # преобразуем данные
    for i in new_data:
        print(i)
        logging.info(f"{i}")
except:
    logging.error(f"преобразование данных - ОШИБКА", exc_info=True)
    
logging.info(f"создаем df")
try:
    df = pd.DataFrame(new_data, columns=['ссылка', 'марка_модель_двс_коробка', 'год_выпуска', 'пробег', 'цена', 'регион'])
except:
    logging.error(f"создаем df - ОШИБКА", exc_info=True)
    
logging.info(f"создаем столбцы")
try:
    df['год_выпуска'] = df['год_выпуска'].apply(lambda x: int(x.strip().replace(' ','')))
    df['пробег'] = df['пробег'].apply(lambda x: int(x.strip().replace(' ','')))
    df['цена'] = df['цена'].apply(lambda x: int(x.strip().replace(' ','')))
except:
    logging.error(f"создаем столбцы - ОШИБКА", exc_info=True)
    
logging.info(f"cчитываем склад")
ovp_fact_yar = pd.read_excel(fr'\\sim.local\data\Yar\Старая папка Общая\TRADE-IN\Отчеты для Москвы\Новый ОВП\ОВП-Яр..xlsx', sheet_name='Склад')
ovp_fact_yar = header_df(ovp_fact_yar) # находим шапку

logging.info(f"сравниваем данные")
try:
    df['поиск_без_отклонения'] = df.apply(lambda x: search_car(x.год_выпуска, x.пробег,	x.цена, ovp_fact_yar, otklonenie_price=0.01), axis=1)
    df['поиск_c_отклонением_цены_5_проц'] = df.apply(lambda x: search_car(x.год_выпуска, x.пробег,	x.цена, ovp_fact_yar, otklonenie_price=0.05), axis=1)
    df['поиск_c_отклонением_цены_5_проц_пробега_5_проц'] = df.apply(lambda x: search_car(x.год_выпуска, x.пробег,	x.цена, ovp_fact_yar, otklonenie_km=0.05, otklonenie_price=0.05), axis=1)
    df['наиболее_веорятный_vin'] = df.apply(lambda x: approximately(x.поиск_без_отклонения, x.поиск_c_отклонением_цены_5_проц,	x.поиск_c_отклонением_цены_5_проц_пробега_5_проц), axis=1)
    df['дата_выдачи_по_складу'] = df['наиболее_веорятный_vin'].apply(lambda x: data_vidach(x, ovp_fact_yar))
    df['план_цена_по_складу'] = df['наиболее_веорятный_vin'].apply(lambda x: price_sclad(x, ovp_fact_yar))
    df['разница_цены'] = df.apply(lambda df: raznitsa(df.цена, df.план_цена_по_складу), axis=1)
    df['пробег_по_складу'] = df['наиболее_веорятный_vin'].apply(lambda x: price_sclad(x, ovp_fact_yar, columns='Пробег, км.'))
    df['разница_пробега'] = df.apply(lambda df: raznitsa(df.пробег, df.пробег_по_складу), axis=1)
    # убираем лишние столбцы
    not_colimns = ['поиск_без_отклонения', 'поиск_c_отклонением_цены_5_проц']
    df = df[[i for i in df.columns if i not in not_colimns]]
except:
    logging.error(f"сравниваем данные - ОШИБКА", exc_info=True)


logging.info(f"сохраняем файл")
try:
    df.to_excel(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\result_avito_yar.xlsx')
except:
    logging.error(f"сохраняем файл - ОШИБКА", exc_info=True)

