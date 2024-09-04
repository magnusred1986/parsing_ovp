# сбор всего ОВП https://sim-auto.ru
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv

# блок логирования
import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_parsing_ovp_all_sim_auto.log",filemode="w", format="%(asctime)s %(levelname)s %(message)s")
logging.info("Запуск скрипта parsing.py")


logging.info(f"собираем ссылки на карточки")
all_links = []
page = 0
while True:
    url = f'https://sim-auto.ru/usedcars/?filter_sort=default&filter_direction=up&filter_marka=&filter_model=&filter_price=118000%3B4105000&filter_probeg=653%3B339090&filter_city=&filter_center=&filter_kpp=&filter_year=&filter_power=&filter_complect=&filter_body_type=&page={page}'
    response = requests.get(url, verify=False) # verify=False или ssl=False игнорировать проверку SSL-сертификата
    if response.status_code != 200:
        print(f'Ошибка севрвера {response.status_code}')
        logging.error(f"статус код ответа сервера {response.status_code} - ОШИБКА", exc_info=True)
        
    logging.info(f"страница {page}")
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    res = soup.find_all('div', class_='image_container')
    links = [i.find('a', class_='add_compare')['href'].replace('#','') for i in res]
    all_links+=links
    print([i.find('a', class_='add_compare')['href'].replace('#','') for i in res])
    logging.info(f"ссылки {links}")
    if len(links)<1:
        break
    page+=1
    

# блок сбора инфо по карточкам
logging.info(f"запуск сбора информации по карточкам")
all_auto = []

# создаем csv с заголовками
try:
    save_file_ = '//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/parsing_ovp/auto_ovp_all.csv'
    logging.info(f"создаем файл для заполнения информацией {save_file_}")
    with open(save_file_, 'w', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['Марка', 'Модель', 'Комплектация', 'Год выпуска', 'Пробег', 'Модификация', 'Объем двигателя',
                            'Привод', 'Мощность', 'КПП', 'Тип кузова', 'Цвет кузова', 'VIN', 'Количество владельцев', 'Руль',
                            'Цена', 'Локация', 'Ссылка'])
except:
    logging.error(f"не удалось создать {save_file_} - ОШИБКА", exc_info=True)


# пробегам по каждой карточке авто собираем инфо
logging.info(f"Пробегаем по каждой карточке и собираем информацию")
for link_ in all_links:
    session = requests.Session()
    ua = UserAgent()
    headers = {'user-agen': ua.random}
    #time.sleep(random.randint(1,5))
    static_url = f'https://sim-auto.ru/usedcars/{link_}.html'
    response = session.get(static_url, headers=headers, verify=False) # verify=False игнорировать проверку SSL-сертификата
    if response.status_code != 200:
        print(f'Ошибка ссылки {response.status_code}')
        logging.error(f"статус код ответа сервера {response.status_code} - ОШИБКА", exc_info=True)

    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'lxml')
    price = [i.text.replace(' ','') for i in soup.find('div', class_='price').find_all('span')]
    items = [i.text for i in soup.find('ul', class_='props').find_all('strong')]
    location = [i.text for i in soup.find('ul', class_='center_info').find_all('strong')][0]
    concat_info = items+price+[location]+[static_url]
    all_auto.append(concat_info)
    
    print(static_url, concat_info)
    logging.info(f"ссылка {static_url} инфо по ней {concat_info}")

# записываем данные в файл
logging.info(f"записываем информацию в файл {save_file_}")
for i in all_auto:
    with open('//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/parsing_ovp//auto_ovp_all.csv', 'a', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(i)
            
logging.info(f"работа скрипта завершена")