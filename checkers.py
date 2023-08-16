#0 - id, 1-login
export_type = 0

from classes import Dictionary
from classes import Abonents
from classes import Export
from classes import Critical_errors
import datetime
import psycopg2
from datetime import datetime, timedelta
import os
from string import Template

task_create_date = datetime.today()
date_l = (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d 23:00:00")
date_h = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d 01:00:00")
#Так как большинство партиций суточные, чтобы их зацепить при поиске нужно брать -1 час и +1 час, то-есть 23:00 и 01:00
#date_l = "2023-08-09 23:00:00"
#date_h = "2023-08-11 01:00:00"

telcos = []
if not os.path.isdir('reports'):
    os.mkdir('reports')   
path_date = datetime.today().strftime("%d.%m.%Y_%H%M%S")
con = psycopg2.connect(
  database='vasexperts', 
  user= 'oim_admin', 
  password='admin', 
  host='127.0.0.1', 
  port='54321'
)
cur = con.cursor()  

_dict = Dictionary(cur)

_abon = Abonents(cur)
_e = Export()
_ce = Critical_errors(cur)
operators = _dict.operators()

for i in range(len(operators)):
  oper_path = operators[i][2].replace('"', '')
  if not os.path.isdir('reports/' + oper_path.replace(' ', '_')):
    os.mkdir('reports/' + oper_path.replace(' ', '_'))
  if not os.path.isdir('reports/' + oper_path.replace(' ', '_') + '/' + path_date):
    os.mkdir('reports/' + oper_path.replace(' ', '_') + '/' + path_date)
  operators[i] = operators[i] + ((str('reports/' + oper_path + '/' + path_date + '/')),)


def check_billing():
  for num in range(len(operators)):

    
    print(f'Оператор:  {operators[num][2]}')
    print('> CHECK BILLING')
    
    ca = _abon.count_abonents(operators[num][0])
    _e.psi(operators[num], f'Оператор:  {operators[num][2]}')
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Название','Открытых','Закрытых','Всего'))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Всего физ лиц:',  ca[0],ca[1],ca[0]+ca[1]))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Всего юр лиц:',   ca[2],ca[3],ca[2]+ca[3]))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Всего оконечных:',ca[4],ca[5],ca[4]+ca[5]))
    _e.psi(operators[num], "="*70)
    

    ffm = _abon.fiz_fio_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц с масками в ФИО:',len(ffm[0]),len(ffm[1]),len(ffm[0])+len(ffm[1])))
    fbm = _abon.fiz_bd_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц с масками в дате рождения:',len(fbm[0]),len(fbm[1]),len(fbm[0])+len(fbm[1])))
    fm = _abon.fiz_minor(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Несовершеннолетние физ лица:',len(fm[0]),len(fm[1]),len(fm[0])+len(fm[1])))
    fdm = _abon.fiz_doc_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц с масками в паспорте:',len(fdm[0]),len(fdm[1]),len(fdm[0])+len(fdm[1])))
    fam = _abon.fiz_adr_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц с масками в адресе регистр.:',len(fam[0]),len(fam[1]),len(fam[0])+len(fam[1])))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц с масками в адресе уст об.:',len(fam[2]),len(fam[3]),len(fam[2])+len(fam[3])))
    fn = _abon.fiz_number(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Физ лиц без номера телефона:',len(fn[0]),len(fn[1]),len(fn[0])+len(fn[1])))

    _e.psi(operators[num], "="*70)


    ubo = _abon.ur_bez_okon(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без оконечных:',len(ubo[0]),len(ubo[1]),len(ubo[0])+len(ubo[1])))

    uname = _abon.ur_name(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без названия:',len(uname[0]),len(uname[1]),len(uname[0])+len(uname[1])))
    ubank = _abon.ur_bank(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без банковской инф.:',len(ubank[0]),len(ubank[1]),len(ubank[0])+len(ubank[1])))
    uinn = _abon.ur_inn(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без ИНН:',len(uinn[0]),len(uinn[1]),len(uinn[0])+len(uinn[1])))
    ucontact = _abon.ur_contact(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без контактного лица(26 поле):',len(ucontact[0]),len(ucontact[1]),len(ucontact[0])+len(ucontact[1])))
    unum = _abon.ur_number(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лиц без номера телефона:',len(unum[0]),len(unum[1]),len(unum[0])+len(unum[1])))
    uam = _abon.ur_adr_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лица с масками в адресе регистрации:',len(uam[0]),len(uam[1]),len(uam[0])+len(uam[1])))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лица с масками в почтовом адресе:',len(uam[2]),len(uam[3]),len(uam[2])+len(uam[3])))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лица с масками в адресе дост счета:',len(uam[4]),len(uam[5]),len(uam[4])+len(uam[5])))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Юр лица с масками в адресе уст обор:',len(uam[6]),len(uam[7]),len(uam[6])+len(uam[7])))

    _e.psi(operators[num], "="*70)

    ufm = _abon.ur_fio_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Оконечные с масками в ФИО:',len(ufm[0]),len(ufm[1]),len(ufm[0])+len(ufm[1])))
    ubm = _abon.ur_bd_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Оконечные с масками в дате рождения:',len(ubm[0]),len(ubm[1]),len(ubm[0])+len(ubm[1])))
    um = _abon.ur_minor(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Несовершеннолетние оконечные:',len(um[0]),len(um[1]),len(um[0])+len(um[1])))

    udm = _abon.ur_doc_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Оконечные с масками в паспорте:',len(udm[0]),len(udm[1]),len(udm[0])+len(udm[1])))
    uoam = _abon.ur_ok_adr_masks(operators[num][0])
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Оконечные с масками в адресе регистр.:',len(uoam[0]),len(uoam[1]),len(uoam[0])+len(uoam[1])))
    _e.psi(operators[num], "{:<40}{:<10}{:<10}{:<10}".format('Оконечные с масками в адресе уст обр.:',len(uoam[2]),len(uoam[3]),len(uoam[2])+len(uoam[3])))


    _e.psi(operators[num], '\n')
    _e.psi(operators[num], "="*70)
    _e.psi(operators[num], "="*24 + ': КРИТИЧЕСКИЕ ОШИБКИ :' + "="*24)

    ### CRITICAL ERRORS

    ce_phones = _ce.len_phones(operators[num][0])
    _e.psi(operators[num], "{:<60}{:<10}".format('Длина номера телефона не по приказу [2;32]:',len(ce_phones)))
    _e.list_of_id(operators[num],   ce_phones, 'Длина номера телефона не по приказу [2-32]',export_type)



    subs_p = _abon.subs_payments(operators[num][0])
    _e.psi(operators[num], str("="*30 +'Платежи' + "="*33))
    _e.psi(operators[num], "{:<40}{:<20}".format('Тип платежа', 'Количество'))
    for item in subs_p:
      _e.psi(operators[num], "{:<40}{:<20}".format(item[0], item[1]))

    _e.psi(operators[num], "="*70)

    _e.list_of_id(operators[num],   ffm[0], '[open] физ лица с масками в фио')
    _e.list_of_id(operators[num],   ffm[1],'[close] физ лица с масками в фио')
    _e.list_of_id(operators[num],   fbm[0], '[open] физ лица с масками в дате рождения')
    _e.list_of_id(operators[num],   fbm[1],'[close] физ лица с масками в дате рождения')
    _e.list_of_id(operators[num],   fm[0], '[open] Несовершеннолетние физ лица')
    _e.list_of_id(operators[num],   fm[1],'[close] Несовершеннолетние физ лица')
    _e.list_of_id(operators[num],   fdm[0], '[open] физ лица с масками в паспорте')
    _e.list_of_id(operators[num],   fdm[1],'[close] физ лица с масками в паспорте')
    _e.list_of_id(operators[num],   fam[0], '[open] физ лица с масками в адресе регистрации')
    _e.list_of_id(operators[num],   fam[1],'[close] физ лица с масками в адресе регистрации')
    _e.list_of_id(operators[num],   fam[2], '[open] физ лица с масками в адресе установки оборудования')
    _e.list_of_id(operators[num],   fam[3],'[close] физ лица с масками в адресе установки оборудования')
    _e.list_of_id(operators[num],   fn[0], '[open] физ лиц без номера телефона')
    _e.list_of_id(operators[num],   fn[1],'[close] физ лиц без номера телефона')


    _e.list_of_id(operators[num],   ubo[0], '[open] Юр лица без оконечных',export_type)
    _e.list_of_id(operators[num],   ubo[1], '[close] Юр лица без оконечных',export_type)
    _e.list_of_id(operators[num],   uname[0], '[open] Юр лиц без названия')
    _e.list_of_id(operators[num],   uname[1],'[close] Юр лиц без названия')
    _e.list_of_id(operators[num],   ubank[0], '[open] Юр лиц без банковской информации')
    _e.list_of_id(operators[num],   ubank[1],'[close] Юр лиц без банковской информации')
    _e.list_of_id(operators[num],   uinn[0], '[open] Юр лиц без ИНН')
    _e.list_of_id(operators[num],   uinn[1],'[close] Юр лиц без ИНН')
    _e.list_of_id(operators[num],   ucontact[0], '[open] Юр лиц без контактного лица')
    _e.list_of_id(operators[num],   ucontact[1],'[close] Юр лиц без контактного лица')
    _e.list_of_id(operators[num],   unum[0], '[open] Юр лиц без номера телефона')
    _e.list_of_id(operators[num],   unum[1],'[close] Юр лиц без номера телефона')
    _e.list_of_id(operators[num],   uam[0], '[open] Юр лица с масками в адресе регистрации')
    _e.list_of_id(operators[num],   uam[1],'[close] Юр лица с масками в адресе регистрации')
    _e.list_of_id(operators[num],   uam[2], '[open] Юр лица с масками в почтовом адресе')
    _e.list_of_id(operators[num],   uam[3],'[close] Юр лица с масками в почтовом адресе')
    _e.list_of_id(operators[num],   uam[4], '[open] Юр лица с масками в адресе доставки счета')
    _e.list_of_id(operators[num],   uam[5],'[close] Юр лица с масками в адресе доставки счета')
    _e.list_of_id(operators[num],   uam[6], '[open] Юр лица с масками в адресе уст оборудования')
    _e.list_of_id(operators[num],   uam[7],'[close] Юр лица с масками в адресе уст оборудования')

    _e.list_of_id(operators[num],   ufm[0], '[open] Оконечные с масками в фио')
    _e.list_of_id(operators[num],   ufm[1],'[close] Оконечные с масками в фио')
    _e.list_of_id(operators[num],   ubm[0], '[open] Оконечные с масками в дате рождения')
    _e.list_of_id(operators[num],   ubm[1],'[close] Оконечные с масками в дате рождения')
    _e.list_of_id(operators[num],   um[0], '[open] Оконечные несовершеннолетние')
    _e.list_of_id(operators[num],   um[1],'[close] Оконечные несовершеннолетние')
    _e.list_of_id(operators[num],   udm[0], '[open] Оконечные с масками в паспорте')
    _e.list_of_id(operators[num],   udm[1],'[close] Оконечные с масками в паспорте')
    _e.list_of_id(operators[num],   uoam[0], '[open] Оконечные с масками в адресе регистрации')
    _e.list_of_id(operators[num],   uoam[1],'[close] Оконечные с масками в адресе регистрации')
    _e.list_of_id(operators[num],   uoam[2], '[open] Оконечные с масками в адресе установки оборудования')
    _e.list_of_id(operators[num],   uoam[3],'[close] Оконечные с масками в адресе установки оборудования')


    _e.dictionary(operators[num], f'Оператор:  {operators[num][2]}')

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник телко кодов :" + "="*30) 
    d_telcos = _dict.telcos()
    _e.dictionary(operators[num], "{:<10}{:<10}{:<30}{:<20}{:<20}".format('Статус','Телко-код','Название','Дата начала', 'Дата окончания'))
    for i in range(len(d_telcos[0])):
      _e.dictionary(operators[num], "{:<10}{:<10}{:<30}{:<20}{:<20}".format(d_telcos[1][i],
                                                    str(d_telcos[0][i][1])[:8],
                                                    str(d_telcos[0][i][2])[:28],
                                                    d_telcos[0][i][3].strftime("%d.%m.%Y %H:%M:%S"),
                                                    d_telcos[0][i][4].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник типов документов :" + "="*30) 
    d_doc_types = _dict.doc_types()
    _e.dictionary(operators[num], "{:<10}{:<3}{:<35}{:<20}{:<20}".format('Статус','Код','Название','Дата начала', 'Дата окончания'))
    for i in range(len(d_doc_types[0])):
      _e.dictionary(operators[num], "{:<10}{:<3}{:<35}{:<20}{:<20}".format(d_doc_types[1][i],
                                                   str(d_doc_types[0][i][0]),
                                                   str(d_doc_types[0][i][1])[:33],
                                                   d_doc_types[0][i][2].strftime("%d.%m.%Y %H:%M:%S"),
                                                   d_doc_types[0][i][3].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник нумерации :" + "="*30) 
    d_ip_num = _dict.ip_numbering_plan(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<18}{:<40}{:<20}{:<20}".format('Статус','Подсеть','Название','Дата начала', 'Дата окончания'))
    for i in range(len(d_ip_num[0])):
      _e.dictionary(operators[num], "{:<10}{:<18}{:<40}{:<20}{:<20}".format(d_ip_num[1][i],
                                                    str(d_ip_num[0][i][2])[:16],
                                                    str(d_ip_num[0][i][3])[:38],
                                                    d_ip_num[0][i][4].strftime("%d.%m.%Y %H:%M:%S"),
                                                    d_ip_num[0][i][5].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')   
    _e.dictionary(operators[num], "="*30 + ": Справочник типов вызовов :" + "="*30) 
    d_call_types = _dict.call_types(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<7}{:<45}{:<20}{:<20}".format('Статус','id','Название','Дата начала', 'Дата окончания'))
    for i in range(len(d_call_types[0])):
      _e.dictionary(operators[num], "{:<10}{:<7}{:<45}{:<20}{:<20}".format(d_call_types[1][i],
                                                   str(d_call_types[0][i][2])[:6],
                                                   str(d_call_types[0][i][3])[:43],
                                                   d_call_types[0][i][4].strftime("%d.%m.%Y %H:%M:%S"),
                                                   d_call_types[0][i][5].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник точек подключения :" + "="*30) 
    d_ip_dp = _dict.ip_data_points(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<40}{:<20}{:<20}".format('Статус','id','Название','Дата начала', 'Дата окончания'))
    for i in range(len(d_ip_dp[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<40}{:<20}{:<20}".format(d_ip_dp[1][i],
                                                   str(d_ip_dp[0][i][2])[:4],
                                                   str(d_ip_dp[0][i][3])[:38],
                                                   d_ip_dp[0][i][4].strftime("%d.%m.%Y %H:%M:%S"),
                                                   d_ip_dp[0][i][5].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник коммутаторов :" + "="*30) 
    d_switches = _dict.switches(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<10}{:<30}{:<30}{:<70}{:<30}{:<20}{:<20}".format('Статус', 'Тип','id','Описание','Адрес установки','Телефонный id коммутатора','Дата начала', 'Дата окончания'))
    for i in range(len(d_switches[0])):
      _e.dictionary(operators[num], "{:<10}{:<10}{:<30}{:<30}{:<70}{:<30}{:<20}{:<20}".format(d_switches[1][i],
                                                                      str(d_switches[0][i][11])[:9],
                                                                      str(d_switches[0][i][4])[:28],
                                                                      str(d_switches[0][i][5])[:28],
                                                                      str(d_switches[0][i][6])[:68],
                                                                      str(d_switches[0][i][7])[:28],
                                                                      d_switches[0][i][8].strftime("%d.%m.%Y %H:%M:%S"),
                                                                      d_switches[0][i][9].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник шлюзов :" + "="*30) 
    d_gates = _dict.gates(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<10}{:<10}{:<30}{:<70}{:<15}{:<7}{:<20}{:<20}".format('Статус', 'Тип','id','Описание','Адрес установки','Адрес','Порт', 'Дата начала','Дата окончания'))
    for i in range(len(d_gates[0])):
      _e.dictionary(operators[num], "{:<10}{:<10}{:<10}{:<30}{:<70}{:<15}{:<7}{:<20}{:<20}".format(d_gates[1][i],
                                                                           str(d_gates[0][i][1])[:8],
                                                                           str(d_gates[0][i][0])[:8],
                                                                           str(d_gates[0][i][2])[:28],
                                                                           str(d_gates[0][i][3])[:68],
                                                                           str(d_gates[0][i][4])[:13],
                                                                           str(d_gates[0][i][5])[:6],
                                                                           d_gates[0][i][6].strftime("%d.%m.%Y %H:%M:%S"),
                                                                           d_gates[0][i][7].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник спец номеров :" + "="*30) 
    d_special_numbers = _dict.special_numbers(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<15}{:<15}{:<30}{:<20}{:<20}".format('Статус','Спец номер','Адрес','Описание','Дата начала', 'Дата окончания'))
    for i in range(len(d_special_numbers[0])):
      _e.dictionary(operators[num], "{:<10}{:<15}{:<15}{:<30}{:<20}{:<20}".format(d_special_numbers[1][i],
                                                          str(d_special_numbers[0][i][2])[:13],
                                                          str(d_special_numbers[0][i][3])[:13],
                                                          str(d_special_numbers[0][i][4])[:28],
                                                          d_special_numbers[0][i][5].strftime("%d.%m.%Y %H:%M:%S"),
                                                          d_special_numbers[0][i][6].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник дополнительных услуг :" + "="*30) 
    d_supplement_services = _dict.supplement_services(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<5}{:<40}{:<40}{:<20}{:<20}".format('Статус','id','Тип','Название','Описание','Дата начала', 'Дата окончания'))
    for i in range(len(d_supplement_services[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<5}{:<40}{:<40}{:<20}{:<20}".format(d_supplement_services[1][i],
                                                              str(d_supplement_services[0][i][2])[:4],
                                                              str(d_supplement_services[0][i][7])[:4],
                                                              str(d_supplement_services[0][i][3])[:38],
                                                              str(d_supplement_services[0][i][4])[:38],
                                                              d_supplement_services[0][i][5].strftime("%d.%m.%Y %H:%M:%S"),
                                                              d_supplement_services[0][i][6].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник карты пучков :" + "="*30) 
    d_bunches_map = _dict.bunches_map(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<50}{:<5}{:<50}{:<20}{:<20}".format('Статус','id','Внутренний пучок','id','Внешний пучок','Дата начала', 'Дата окончания'))
    for i in range(len(d_bunches_map[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<50}{:<5}{:<50}{:<20}{:<20}".format(d_bunches_map[1][i],
                                                              str(d_bunches_map[0][i][0])[:4],
                                                              str(d_bunches_map[0][i][1])[:48],
                                                              str(d_bunches_map[0][i][2])[:4],
                                                              str(d_bunches_map[0][i][3])[:48],
                                                              d_bunches_map[0][i][4].strftime("%d.%m.%Y %H:%M:%S"),
                                                              d_bunches_map[0][i][5].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')      
    _e.dictionary(operators[num], "="*30 + ": Справочник пучков :" + "="*30) 
    d_bunches = _dict.bunches(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<15}{:<30}{:<50}{:<20}{:<20}{:<20}{:<20}{:<20}".format('Статус','id','Тип','Коммутатор','Описание','MAC','Номер пути АТМ', 'Номер канала АТМ','Дата начала','Дата окончания'))
    for i in range(len(d_bunches[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<15}{:<30}{:<50}{:<20}{:<20}{:<20}{:<20}{:<20}".format(str(d_bunches[1][i]),
                                                              str(d_bunches[0][i][0])[:4],
                                                              str(d_bunches[0][i][1])[:13],
                                                              str(d_bunches[0][i][2])[:28],
                                                              str(d_bunches[0][i][6])[:48],
                                                              str(d_bunches[0][i][3])[:18],
                                                              str(d_bunches[0][i][4])[:18],
                                                              str(d_bunches[0][i][5])[:18],
                                                              d_bunches[0][i][7].strftime("%d.%m.%Y %H:%M:%S"),
                                                              d_bunches[0][i][8].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник номерной емкости :" + "="*30) 
    d_phone_numbering_plan = _dict.phone_numbering_plan(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<20}{:<7}{:<50}{:<50}{:<20}{:<20}".format('Статус','Тип','Диапазон','Кол-во','Адрес','Статус','Дата начала', 'Дата окончания'))
    for i in range(len(d_phone_numbering_plan[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<20}{:<7}{:<50}{:<50}{:<20}{:<20}".format(d_phone_numbering_plan[1][i],
                                                              str(d_phone_numbering_plan[0][i][2])[:4],
                                                              str(d_phone_numbering_plan[0][i][13] + ' - ' + d_phone_numbering_plan[0][i][14])[:18],
                                                              str(d_phone_numbering_plan[0][i][15])[:6],
                                                              str(d_phone_numbering_plan[0][i][16])[:48],
                                                              str(d_phone_numbering_plan[0][i][19])[:48],
                                                              d_phone_numbering_plan[0][i][22].strftime("%d.%m.%Y %H:%M:%S"),
                                                              d_phone_numbering_plan[0][i][23].strftime("%d.%m.%Y %H:%M:%S")))

    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник типов платежей :" + "="*30) 
    d_pay_types = _dict.pay_types(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<30}{:<50}".format('Статус','id','Название'))
    for i in range(len(d_pay_types[0])):
      _e.dictionary(operators[num], "{:<10}{:<30}{:<50}".format(d_pay_types[1][i],
                                       str(d_pay_types[0][i][0])[:28],
                                       str(d_pay_types[0][i][1])[:48]))


    _e.dictionary(operators[num], '\n')
    _e.dictionary(operators[num], "="*30 + ": Справочник причин завершения :" + "="*30) 
    d_termination_causes = _dict.termination_causes(operators[num])
    _e.dictionary(operators[num], "{:<10}{:<5}{:<10}{:<70}{:<20}{:<20}".format('Статус','Тип','Код','Описание','Дата начала','Дата окончания'))
    for i in range(len(d_termination_causes[0])):
      _e.dictionary(operators[num], "{:<10}{:<5}{:<10}{:<70}{:<20}{:<20}".format(str(d_termination_causes[1][i]),
                                                         str(d_termination_causes[0][i][2])[:5],
                                                         str(d_termination_causes[0][i][3])[:8],
                                                         str(d_termination_causes[0][i][4])[:68],
                                                         d_termination_causes[0][i][5].strftime("%d.%m.%Y %H:%M:%S"),
                                                         d_termination_causes[0][i][6].strftime("%d.%m.%Y %H:%M:%S")))









    _e.psi(operators[num], '='*30 + 'Справочники' + '='*29)

    #100
    _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' ID', 'Название', 'Кол-во', 'Комментарий'))
    if '[ ERR ]' in d_bunches[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 100 ', 'пучки', len(d_bunches[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_bunches[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 100 ', 'Пучки', len(d_bunches[0]), 'Ошибок нет'))

    #103
    if '[ ERR ]' in d_switches[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 103 ', 'Коммутаторы', len(d_switches[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_switches[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 103 ', 'Коммутаторы', len(d_switches[0]), 'Ошибок нет'))

    #104
    if '[ ERR ]' in d_gates[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 104 ', 'шлюза', len(d_gates[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_gates[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 104 ', 'Шлюза', len(d_gates[0]), 'Ошибок нет'))

    #105
    if '[ ERR ]' in d_call_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 105 ', 'Справочник типов вызовов', len(d_call_types[0]), 'Есть ошибки'))
    elif '[ WRN ]' in d_call_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 105 ', 'Справочник типов вызовов', len(d_call_types[0]), 'Неизвестный тип вызова'))
    elif '[ ERR ]' not in d_call_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 105 ', 'Справочник типов вызовов', len(d_call_types[0]), 'Ошибок нет'))

    #106
    if '[ ERR ]' in d_supplement_services[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 106 ', 'Справочник услуг', len(d_supplement_services[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_supplement_services[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 106 ', 'Справочник услуг', len(d_supplement_services[0]), 'Ошибок нет'))

    #107
    if '[ ERR ]' in d_pay_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 107 ', 'Справочник типов платежей', len(d_pay_types[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_pay_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 107 ', 'Справочник типов платежей', len(d_pay_types[0]), 'Ошибок нет'))

    #108
    if '[ ERR ]' in d_termination_causes[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 109 ', 'Справочник причин завершения', len(d_termination_causes[0]), 'Есть ошибки'))
    elif '[ WRN ]' in d_termination_causes[1]:
       _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 109 ', 'Справочник причин завершения', len(d_termination_causes[0]), 'Код завершения неизвестен'))
    elif '[ ERR ]' not in d_termination_causes[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 109 ', 'Справочник причин завершения', len(d_termination_causes[0]), 'Ошибок нет'))

    #109
    if '[ ERR ]' in d_ip_num[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 109 ', 'Справочник ip нумерации', len(d_ip_num[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_ip_num[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 109 ', 'Справочник ip нумерации', len(d_ip_num[0]), 'Ошибок нет'))

    #110
    if '[ ERR ]' in d_phone_numbering_plan[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 110 ', 'Справочник нумерной емкости', len(d_phone_numbering_plan[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_phone_numbering_plan[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 110 ', 'Справочник нумерной емкости', len(d_phone_numbering_plan[0]), 'Ошибок нет'))

    #111
    if '[ ERR ]' in d_doc_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 111 ', 'Справочник типов документов', len(d_doc_types[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_doc_types[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 111 ', 'Справочник типов документов', len(d_doc_types[0]), 'Ошибок нет'))

    #112
    if '[ ERR ]' in d_telcos[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 112 ', 'Список операторов связи', len(d_telcos[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_telcos[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 112 ', 'Список операторов связи', len(d_telcos[0]), 'Ошибок нет'))

    #113
    if '[ ERR ]' in d_ip_dp[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 113 ', 'Справочник точек подключения', len(d_ip_dp[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_ip_dp[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 113 ', 'Справочник точек подключения', len(d_ip_dp[0]), 'Ошибок нет'))

    #114
    if '[ ERR ]' in d_special_numbers[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 114 ', 'Справочник специальных номеров', len(d_special_numbers[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_special_numbers[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 114 ', 'Справочник специальных номеров', len(d_special_numbers[0]), 'Ошибок нет'))

    #115
    if '[ ERR ]' in d_bunches_map[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 115 ', 'Справочник карты пучков', len(d_bunches_map[0]), 'Есть ошибки'))
    elif '[ ERR ]' not in d_bunches_map[1]:
      _e.psi(operators[num], "{:<7}{:<40}{:<10}{:<10}".format(' 115 ', 'Справочник карты пучков', len(d_bunches_map[0]), 'Ошибок нет'))


    _e.psi(operators[num], "="*70)
    print('> BILLING CHECKED')
    print("="*70)
    print('> CHECK NAT')

# NAT

    oper_id = operators[num][0]
    select_nat = Template("""select
                (select count(*) from ${partition}
                where ${type_}_telco_code = ${oper_id}
                and (${type_}_client_address <<inet '10.0.0.0/8'       
                or ${type_}_client_address <<inet '172.16.0.0/12'        
                or ${type_}_client_address  <<inet '192.168.0.0/16'      
                or ${type_}_client_address  <<inet '100.64.0.0/10')      
                and not (${type_}_server_address <<inet '10.0.0.0/8'     
                or ${type_}_server_address <<inet '172.16.0.0/12'        
                or ${type_}_server_address  <<inet '192.168.0.0/16'      
                or ${type_}_server_address  <<inet '100.64.0.0/10') 
                and ${type_}_protocol_code != 65025    
                and ${type_}_nat_address is null
                ${ip_num}
                ),(
                select count(*) from ${partition}                      
                where ${type_}_telco_code = ${oper_id}
                and (${type_}_client_address <<inet '10.0.0.0/8'       
                or ${type_}_client_address <<inet '172.16.0.0/12'        
                or ${type_}_client_address  <<inet '192.168.0.0/16'      
                or ${type_}_client_address  <<inet '100.64.0.0/10')      
                and not (${type_}_server_address <<inet '10.0.0.0/8'     
                or ${type_}_server_address <<inet '172.16.0.0/12'        
                or ${type_}_server_address  <<inet '192.168.0.0/16'      
                or ${type_}_server_address  <<inet '100.64.0.0/10'))
                """)




    cur = con.cursor()  
    partitions = f"""SELECT ppl1.\"partition\", ppl2.*                  
                  FROM public.pathman_partition_list ppl1             
                  join (select parent,min(range_min),max(range_max)   
                  from public.pathman_partition_list                  
                  where range_min > '{date_l}'                        
                  and range_max < '{date_h}'                          
                  and (parent = 'oimc.http_requests'::regclass        
                  or parent = 'oimc.raw_flows'::regclass              
                  or parent = 'oimc.voip_connections'::regclass       
                  or parent = 'oimc.mail_connections'::regclass       
                  or parent = 'oimc.im_connections'::regclass         
                  or parent = 'oimc.ftp_connections'::regclass        
                  or parent = 'oimc.terminal_connections'::regclass)  
                  group by parent) ppl2                               
                  on ppl1.parent = ppl2.parent                        
                  where  range_min > '{date_l}'                       
                  and range_max < '{date_h}'                          
                  order by range_max """
    cur.execute(partitions)
    partitions = cur.fetchall()

    ip_nums = f"""select oinp_subnet from oims.oper_ip_numbering_plan_history where oinp_oper_id = {oper_id}"""
    cur.execute(ip_nums)
    ip_nums = cur.fetchall()
    print(f'Operator subnets are excluded from statistics: {len(ip_nums)}')

    def ip_num(type_):
        if len(ip_nums) > 0:
            ip_num = 'and not ('
            for item in ip_nums:
                ip_num += f"{type_}_server_address <<inet '{item[0]}' or "
            ip_num = ip_num[:-4] + ')'
            return ip_num
        else: 
            return ''


    results_http,results_raw,results_voip,results_mail,results_im,results_ftp,results_term = [],[],[],[],[],[],[]

    for i in range(len(partitions)):

        if 'oimc.http' in partitions[i][0]:
            part_type_ = 'htrq'
            cur.execute(select_nat.substitute(partition=partitions[i][0], type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_http.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.raw' in partitions[i][0]:
            part_type_ = 'rawf'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_raw.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.voip' in partitions[i][0]:
            part_type_ = 'vipc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_voip.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.mail' in partitions[i][0]:
            part_type_ = 'emlc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_mail.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.im_c' in partitions[i][0]:
            part_type_ = 'imcn'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_im.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.ftp_c' in partitions[i][0]:
            part_type_ = 'ftpc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_ftp.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.term' in partitions[i][0]:
            part_type_ = 'trmc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_term.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

    def unification(result,name):
        sum = 0
        procent = 0
        if len(result) > 1:
          for i in range(len(result)):
            sum += result[i][2]
            procent += result[i][0]
          procent = procent / len(result)
          _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format(name, f'{100 - procent*100:.3f}', result[0][1], sum))
        elif len(result) == 1:
          _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format(name, f'{100 - result[0][0]*100:.3f}', result[0][1], result[0][2]))
        else:
          _e.psi(operators[num], "{:<38}{:<12}{:<45}".format(name, 'Данных нет', date_l + ' - ' + date_h))


    _e.psi(operators[num], '\n')
    _e.psi(operators[num], "="*30 + ": Статистика НАТа :" + "="*30)
    _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format('Name','Percent','Range','Count'))
    unification(results_http,'Интернет-посещения HTTP')
    unification(results_raw,'Передача данных (закрытые протоколы)')
    unification(results_voip,'VoIP-соединения')
    unification(results_mail,'Email-сообщения')
    unification(results_im,'IM-сообщения')
    unification(results_ftp,'FTP-соединения')
    unification(results_term,'Терминальный доступ')

    print("="*70)
    print('> CHECK AAA')


    #AAA
    select_aaa = Template("""select
            (select count(*) from ${partition}
            where ${type_}_subscriber_id != 'operator'
            and ${type_}_telco_code = ${oper_id}),
            (select count(*) from ${partition}
            where ${type_}_telco_code = ${oper_id})""")

    results_http,results_raw,results_voip,results_mail,results_im,results_ftp,results_term = [],[],[],[],[],[],[]

    for i in range(len(partitions)):

        if 'oimc.http' in partitions[i][0]:
            part_type_ = 'htrq'
            cur.execute(select_aaa.substitute(partition=partitions[i][0], type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_http.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
                #if result[0][0] / result[0][1] < 0.95:
                #   print(f"[AAA] export {partitions[i][0]} to file!")
                #   _e.export_aaa(operators[num],partitions[i][0],part_type_)
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.raw' in partitions[i][0]:
            part_type_ = 'rawf'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_raw.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.voip' in partitions[i][0]:
            part_type_ = 'vipc'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_voip.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.mail' in partitions[i][0]:
            part_type_ = 'emlc'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_mail.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.im_c' in partitions[i][0]:
            part_type_ = 'imcn'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_im.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.ftp_c' in partitions[i][0]:
            part_type_ = 'ftpc'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_ftp.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

        elif 'oimc.term' in partitions[i][0]:
            part_type_ = 'trmc'
            cur.execute(select_aaa.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=oper_id))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_term.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[AAA] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not records'))

    def unification(result,name):
        sum = 0
        procent = 0
        if len(result) > 1:
          for i in range(len(result)):
            sum += result[i][2]
            procent += result[i][0]
          procent = procent / len(result)
          _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format(name, f'{procent*100:.3f}', result[0][1], sum))
        elif len(result) == 1:
          _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format(name, f'{result[0][0]*100:.3f}', result[0][1], result[0][2]))
        else:
          _e.psi(operators[num], "{:<38}{:<12}{:<45}".format(name, 'Данных нет', date_l + ' - ' + date_h))


    _e.psi(operators[num], '\n')
    _e.psi(operators[num], "="*30 + ": Статистика ААА  :" + "="*30)
    _e.psi(operators[num], "{:<38}{:<12}{:<45}{:<10}".format('Name','Percent','Range','Count'))
    unification(results_http,'Интернет-посещения HTTP')
    unification(results_raw,'Передача данных (закрытые протоколы)')
    unification(results_voip,'VoIP-соединения')
    unification(results_mail,'Email-сообщения')
    unification(results_im,'IM-сообщения')
    unification(results_ftp,'FTP-соединения')
    unification(results_term,'Терминальный доступ')

    print("="*70)
    print('> CHECK LOGINS')
    # LOGINS

    select_logins = Template("""
          select distinct ${type_}_subscriber_id
          from ${partition}
          where ${type_}_subscriber_id not in 
          (select sgh.sgnh_identity
          from oims.subscribers s join
          oims.subs_generic_history sgh 
          on s.subs_id = sgh.sgnh_subs_id 
          where s.subs_oper_id = ${operator})
          and ${type_}_telco_code = ${operator};
          """)

    select_all_logins = Template("""
          select distinct ${type_}_subscriber_id
          from ${partition}
          where ${type_}_telco_code = ${operator};
          """)
    
    logins = []
    results_http,results_raw,results_voip,results_mail,results_im,results_ftp,results_term = [],[],[],[],[],[],[]

    for i in range(len(partitions)):

        if 'oimc.http' in partitions[i][0]:
            part_type_ = 'htrq'
            cur.execute(select_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_http.append(['Интернет-посещения HTTP', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)



        elif 'oimc.raw' in partitions[i][0]:
            part_type_ = 'rawf'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_raw.append(['Передача данных (закрытые протоколы)', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.voip' in partitions[i][0]:
            part_type_ = 'vipc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_voip.append(['VoIP-соединения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.mail' in partitions[i][0]:
            part_type_ = 'emlc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_mail.append(['Email-сообщения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.im_c' in partitions[i][0]:
            part_type_ = 'imcn'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_im.append(['IM-сообщения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.ftp_c' in partitions[i][0]:
            part_type_ = 'ftpc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_ftp.append(['FTP-соединения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.term' in partitions[i][0]:
            part_type_ = 'trmc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=oper_id))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=oper_id))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_term.append(['Терминальный доступ', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f'[LOG] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

    def unification(result):
        sum = 0
        procent = 0
        if len(result) > 1:
            for i in range(len(result)):
                sum += result[i][3]
                procent += result[i][1]
            procent = procent / len(result)
            _e.psi(operators[num], "{:<38} {:<10} {:<45}".format(result[0][0], f'{100 - procent*100:.3f}', result[0][2]))
        elif len(result) == 1:
            _e.psi(operators[num], "{:<38} {:<10} {:<45}".format(result[0][0], f'{100 - result[0][1]*100:.3f}', result[0][2]))

    _e.psi(operators[num], '\n')
    _e.psi(operators[num], "="*30 + ": Принадлежность логинов  :" + "="*22)
    _e.psi(operators[num], "{:<38} {:<10} {:<45}".format('Name','Percent','Range'))
    unification(results_http)
    unification(results_raw)
    unification(results_voip)
    unification(results_mail)
    unification(results_im)
    unification(results_ftp)
    unification(results_term)


    result = ''
    file_name = f"""{operators[num][5].replace(' ','_')}logins_without_billing_{datetime.today().strftime("%Y%m%d%H%M%S")}.txt"""
    f = open(file_name,'w') 
    for login in logins:
        result += login[0] + '\n'
    f.write(result)
    f.close()

    print("="*70)
    


check_billing()






con.close() 

