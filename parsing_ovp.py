# сбор только МСК https://www.sim-autopro.ru
import requests
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import random
import csv

# блок логирования
import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_parsing_ovp_sim_autopro.log",filemode="w", format="%(asctime)s %(levelname)s %(message)s")
logging.info("Запуск скрипта parsing.py")


logging.info(f"собираем ссылки на карточки")
# собираем ссылки на карточки авто
all_links = []
page = 0
while True:
    url = f'https://www.sim-autopro.ru/cars/?filter_auto_marka=&filter_auto_model=&filter_auto_kpp=&filter_price_from=&filter_price_to=&filter_auto_god_from=&filter_auto_god_to=&filter_auto_tip_dvigatela=&filter_auto_privod=&filter_auto_tip_kuzova=&filter_auto_moshnost6=75%7C277&filter_auto_probeg=6%7C339090&page={page}'
    session = requests.Session()
    ua = UserAgent()
    headers = {'user-agen': ua.random}
    response = session.get(url, headers=headers, verify=False) # verify=False игнорировать проверку SSL-сертификата
    # time.sleep(random.randint(1,5))
    if response.status_code != 200:
        print(f'Ошибка ответа {response.status_code}')
        logging.error(f"статус код ответа сервера {response.status_code} - ОШИБКА", exc_info=True)

    soup = BeautifulSoup(response.text, 'lxml')
    result = soup.find_all('div', class_='usedcar')
    links = [i.find('a', class_='title')['href'] for i in result]
    all_links+=links
    print(page)
    logging.info(f"страница сайта -  {page}")
    if len(links) < 1:
        break
    page +=1

    print(links)
    logging.info(f"ссылка -  {links}")

#print(all_links, len(all_links))


logging.info(f"запуск сбора информации по карточкам")
# блок сбора инфо по карточкам
all_auto = []

# создаем csv с заголовками
try:
    save_file_ = '//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/parsing_ovp/auto_ovp.csv'
    logging.info(f"создаем файл для заполнения информацией {save_file_}")
    with open(save_file_, 'w', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['Марка', 'Модель', 'Модификация', 'Комплектация', 'Цвет кузова', 'Год выпуска', 'Тип кузова',
                            'Цена', 'Пробег', 'Тип двигателя', 'Объем двигателя', 'Привод', 'Мощность', 'КПП', 'Тип дисков',
                            'Размер дисков', 'Сезонность шин', 'Размер шин', 'Состояние', 'Руль', 'Статус', 'VIN',
                            'Хозяев по ПТС', 'Таможня', 'Ссылка'])
except:
    logging.error(f"не удалось создать {save_file_} - ОШИБКА", exc_info=True)

# пробегам по каждой карточке авто собираем инфо
logging.info(f"Пробегаем по каждой карточке и собираем информацию")
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
            logging.error(f"ошибка ссылки - ответ сервера {response.status_code} - ОШИБКА", exc_info=True)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')

        # вся инфа с 1 блока
        items = [i.text.replace("₽","").replace(" ","")  if "₽" in  i.text else i.text for i in soup.find('div', class_='items').find_all('strong')]+[static_url]
        all_auto.append(items)

        print(items)
        logging.info(f"собрано {items}")
except Exception as ex_:
    logging.error(f"ошибка {ex_}")
# записываем данные в файл

logging.info(f"записываем информацию в файл {save_file_}")
for i in all_auto:
    with open(save_file_, 'a', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(i)
            
logging.info(f"работа скрипта завершена")