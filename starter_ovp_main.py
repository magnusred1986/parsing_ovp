# блок логирования
import logging
logging.basicConfig(level=logging.INFO, filename=fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_starter_ovp_main.log",filemode="w", format="%(asctime)s %(levelname)s %(message)s")
# https://habr.com/ru/companies/wunderfund/articles/683880/   - ссылка на статью логирования
# filemode="a" дозапись "w" - перезапись
logging.info("Запуск скрипта starter_ovp_main.py")

# блок импортов для обновления сводных
import pythoncom
pythoncom.CoInitializeEx(0)
import win32com.client
import time

# блок импорта отправки почты
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

import subprocess
import pandas as pd

import os
from datetime import datetime, date, timedelta

# считываем актуальный пароль 
def my_pass():
    """функция считывания пароля

    Returns:
        _type_: _description_
    """
    logging.info(f"{my_pass.__name__} - ЗАПУСК")
    
    try:
        with open(f'//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/temp_/password_email.txt', 'r') as actual_pass:
            return actual_pass.read()
        
    except:
        logging.error(f"{my_pass.__name__} - ОШИБКА", exc_info=True)
        

def read_email_adress(mail = fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\Список_адресатов.xlsx'):
    """Функция считывания адресатов для рассылки

    Args:
        mail (_type_, optional): _description_. Defaults to fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\Список_адресатов.xlsx'

    Returns:
        _type_: возфращает строку со списком email
    """
    logging.info(f"запуск функции {read_email_adress.__name__}")
    
    try:
        em_list = pd.read_excel(mail)
        return list(em_list['email'])
    except:
        logging.error(f"{read_email_adress.__name__} - ОШИБКА", exc_info=True)
        
        
# письмо если нет ошибок
def send_mail(send_to:list):
    """рассылка почты

    Args:
        send_to (list): _description_
    """
    logging.info(f"{send_mail.__name__} - ЗАПУСК")
    
    try:
        send_from = 'skrutko@sim-auto.ru'                                                                
        subject = f"Сравнение складов ОВП с данными сайтов на {(datetime.now()).strftime('%d-%m-%Y')}"                                                                  
        text = f"Здравствуйте\nВо вложении проверка складов ОВП с сайтами sim-auto.ru и sim-autopro.ru на {(datetime.now()).strftime('%d-%m-%Y')}"                                                                      
        files = fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\Проверка_ОВП.xlsx"  
        server = "server-vm36.SIM.LOCAL"
        port = 587
        username='skrutko'
        password=my_pass()
        isTls=True
        
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = ','.join(send_to)
        msg['Date'] = formatdate(localtime = True)
        msg['Subject'] = subject
        msg.attach(MIMEText(text))

        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(files, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="OVP.xlsx"') # имя файла должно быть на латинице иначе придет в кодировке bin
        msg.attach(part)

        smtp = smtplib.SMTP(server, port)
        if isTls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()
        logging.info(f"{send_mail.__name__} - ВЫПОЛНЕНО")
        logging.info(f"Адреса рассылки {send_to}")
    except:
        logging.error(f"{send_mail.__name__} - ОШИБКА", exc_info=True)
    

# письмо если есть ошибки
def send_mail_danger(send_to:list):
    """расслыка почты если ошибка

    Args:
        send_to (_type_): _description_
    """
    logging.info(f"{send_mail_danger.__name__} - ЗАПУСК")
    
    try:                                                                                       
        send_from = 'skrutko@sim-auto.ru'                                                                
        subject =  fr"проверьте исходники {'//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/parsing_ovp/'}"                                                                  
        text = f"проверьте исходники {'//sim.local/data/Varsh/OFFICE/CAGROUP/run_python/task_scheduler/parsing_ovp/'}"                                                                      
        files = fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_starter_ovp_main.log'  
        server = "server-vm36.SIM.LOCAL"
        port = 587
        username='skrutko'
        password=my_pass()
        isTls=True
        
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = ','.join(send_to)
        msg['Date'] = formatdate(localtime = True)
        msg['Subject'] = subject
        msg.attach(MIMEText(text))

        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(files, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="log.txt"') # имя файла должно быть на латинице иначе придет в кодировке bin
        msg.attach(part)

        smtp = smtplib.SMTP(server, port)
        if isTls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()
        logging.info(f"{send_mail_danger.__name__} - ВЫПОЛНЕНО")
        logging.info(f"Адреса рассылки {send_to}")
    except:
        logging.error(f"{send_mail_danger.__name__} - ОШИБКА", exc_info=True)
    

def detected_danger(filename_log = fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_starter_ovp_main.log"):
    """обнаружение ошибок в логах   
    ищет 'warning'

    Returns:
        _type_: bool
    """
    logging.info(f"{detected_danger.__name__} - ЗАПУСК")
    
    try:
        with open(filename_log, '+r') as file:
            return 'warning' in file.read().lower()
    except:
        logging.error(f"{detected_danger.__name__} - ОШИБКА", exc_info=True)
        
        
def sending_mail(lst_email, lst_email_error):
    """рассылка почты - если нет ошибок вызываем send_mail(),   
    если есть ошибки send_mail_error()   
    """
    logging.info(f"{sending_mail.__name__} - ЗАПУСК")
    
    try:
        if detected_danger()==False:
            send_mail(lst_email)
        else:
            send_mail_danger(lst_email_error)
            
        logging.info(f"{sending_mail.__name__} - ВЫПОЛНЕНО")
    except:
        logging.error(f"{sending_mail.__name__} - ОШИБКА", exc_info=True)
        




# вызвать сборку с сайта ОВП МСК
logging.info(f"запуск - parsing_ovp.py")
try:
    subprocess.call(['py', fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\parsing_ovp.py'])
except:
    logging.error(f"parsing_ovp.py - ОШИБКА", exc_info=True)

# вызвать сборку с сайта ОВП ALL
logging.info(f"запуск - parsing_ovp_all.py")
try:
    subprocess.call(['py', fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\parsing_ovp_all.py']) 
except:
    logging.error(f"parsing_ovp_all.py - ОШИБКА", exc_info=True)



# вызываем сверку по МСК
logging.info(f"запуск - comparison_msk.py")
try:
    subprocess.call(['py', fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\comparison_msk.py'])
except:
    logging.error(f"comparison_msk.py - ОШИБКА", exc_info=True)

# вызываем сверку по ОВП ALL
logging.info(f"запуск - comparison_all.py")
try:
    subprocess.call(['py', fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\comparison_all.py'])
except:
    logging.error(f"comparison_all.py - ОШИБКА", exc_info=True)


logging.info(f"запуск - обновления файла - Проверка_ОВП.xlsx")

try:
    xlapp = win32com.client.DispatchEx("Excel.Application")
    wb = xlapp.Workbooks.Open(fr"\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\Проверка_ОВП.xlsx")
    wb.Application.AskToUpdateLinks = False   # разрешает автоматическое  обновление связей (файл - парметры - дополнительно - общие - убирает галку запрашивать об обновлениях связей)
    wb.Application.DisplayAlerts = True  # отображает панель обновления иногда из-за перекрестного открытия предлагает ручной выбор обновления True - показать панель
    wb.RefreshAll()
    #xlapp.CalculateUntilAsyncQueriesDone() # удержит программу и дождется завершения обновления. было прописано time.sleep(30)
    time.sleep(40) # задержка 60 секунд, чтоб уж точно обновились сводные wb.RefreshAll() - иначе будет ошибка 
    wb.Application.AskToUpdateLinks = True   # запрещает автоматическое  обновление связей / то есть в настройках экселя (ставим галку обратно)
    wb.Save()
    wb.Close()
    xlapp.Quit()
    wb = None # обнуляем сслыки переменных иначе процесс эксель не завершается и висит в дистпетчере
    xlapp = None # обнуляем сслыки переменных иначе процесс эксел ь не завершается и висит в дистпетчере
    del wb # удаляем сслыки переменных иначе процесс эксель не завершается и висит в дистпетчере
    del xlapp # удаляем сслыки переменных иначе процесс эксель не завершается и висит в дистпетчере
    logging.info(f"обновлено - сохранено")
except:
    logging.error(f"Обновление файла Проверка_ОВП.xlsx - ОШИБКА", exc_info=True)
    
    
logging.info(f"Сборка всех логов в один файл логирования")

# сбор всех логов в общий лог py_log_starter_ovp_main.log
with open(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_comparison_msk.log', 'r') as file:
    for i in file.readlines():
        logging.info(i)
        
with open(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_comparison_all.log', 'r') as file:
    for i in file.readlines():
        logging.info(i)
        
with open(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_parsing_ovp_all_sim_auto.log', 'r') as file:
    for i in file.readlines():
        logging.info(i)
        
with open(fr'\\sim.local\data\Varsh\OFFICE\CAGROUP\run_python\task_scheduler\parsing_ovp\py_log_parsing_ovp_sim_autopro.log', 'r') as file:
    for i in file.readlines():
        logging.info(i)


# список с адресами рассылки
lst_email = read_email_adress()
lst_email_error = ['skrutko@sim-auto.ru', 'zhurin@sim-auto.ru'] # есть ошибки ['skrutko@sim-auto.ru', 'zhurin@sim-auto.ru']
# запуск функции рассылки почты
logging.info(f"детектим ошибки, проверяем почту")
sending_mail(lst_email, lst_email_error)
logging.info(f"почта отправлена")