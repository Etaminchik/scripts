'''pip install psycopg2'''
import psycopg2
from datetime import datetime, timedelta
from string import Template

con = psycopg2.connect(
  database="vasexperts", 
  user="oim_admin", 
  password="admin", 
  host="127.0.4.11", 
  port="5434"
)

select_nat = Template("select\
            (select count(*) from ${partition}\
            where (${type}_client_address <<inet '10.0.0.0/8'       \
            or ${type}_client_address <<inet '172.16.0.0/12'        \
            or ${type}_client_address  <<inet '192.168.0.0/16'      \
            or ${type}_client_address  <<inet '100.64.0.0/10')      \
            and not (${type}_server_address <<inet '10.0.0.0/8'     \
            or ${type}_server_address <<inet '172.16.0.0/12'        \
            or ${type}_server_address  <<inet '192.168.0.0/16'      \
            or ${type}_server_address  <<inet '100.64.0.0/10')      \
            and ${type}_nat_address is not null),                   \
            (select count(*) from ${partition}                      \
            where (${type}_client_address <<inet '10.0.0.0/8'       \
            or ${type}_client_address <<inet '172.16.0.0/12'        \
            or ${type}_client_address  <<inet '192.168.0.0/16'      \
            or ${type}_client_address  <<inet '100.64.0.0/10')      \
            and not (${type}_server_address <<inet '10.0.0.0/8'     \
            or ${type}_server_address <<inet '172.16.0.0/12'        \
            or ${type}_server_address  <<inet '192.168.0.0/16'      \
            or ${type}_server_address  <<inet '100.64.0.0/10'))")


date_l = (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d 23:00:00")
date_h = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d 01:00:00")

cur = con.cursor()  
partitions = f"SELECT ppl1.\"partition\", ppl2.*                  \
              FROM public.pathman_partition_list ppl1             \
              join (select parent,min(range_min),max(range_max)   \
              from public.pathman_partition_list                  \
              where range_min > '{date_l}'                        \
              and range_max < '{date_h}'                          \
              and (parent = 'oimc.http_requests'::regclass        \
              or parent = 'oimc.raw_flows'::regclass              \
              or parent = 'oimc.voip_connections'::regclass       \
              or parent = 'oimc.mail_connections'::regclass       \
              or parent = 'oimc.im_connections'::regclass         \
              or parent = 'oimc.ftp_connections'::regclass        \
              or parent = 'oimc.terminal_connections'::regclass)  \
              group by parent) ppl2                               \
              on ppl1.parent = ppl2.parent                        \
              where  range_min > '{date_l}'                       \
              and range_max < '{date_h}'                          \
              order by range_max "
cur.execute(partitions)
partitions = cur.fetchall()

results_http,results_raw,results_voip,results_mail,results_im,results_ftp,results_term = [],[],[],[],[],[],[]

for i in range(len(partitions)):
    print('Check: ',partitions[i][0])
    if 'oimc.http' in partitions[i][0]:
        part_type = 'htrq'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_http.append(['Интернет-посещения HTTP', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.raw' in partitions[i][0]:
        part_type = 'rawf'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_raw.append(['Передача данных (закрытые протоколы)', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.voip' in partitions[i][0]:
        part_type = 'vipc'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_voip.append(['VoIP-соединения', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.mail' in partitions[i][0]:
        part_type = 'emlc'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_mail.append(['Email-сообщения', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.im_c' in partitions[i][0]:
        part_type = 'imcn'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_im.append(['IM-сообщения', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.ftp_c' in partitions[i][0]:
        part_type = 'ftpc'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_ftp.append(['FTP-соединения', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

    elif 'oimc.term' in partitions[i][0]:
        part_type = 'trmc'
        cur.execute(select_nat.substitute(partition=partitions[i][0],type=part_type))
        result = cur.fetchall()
        results_term.append(['Терминальный доступ', result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])

def unification(result):
    sum = 0
    procent = 0
    if len(result) > 1:
        for i in range(len(result)):
            sum += result[i][3]
            procent += result[i][1]
        procent = procent / len(result)
        print("{:<38} {:<10} {:<45} {:<10}".format(result[0][0], f'{procent*100:.3f}', result[0][2], sum))
    else:
        print("{:<38} {:<10} {:<45} {:<10}".format(result[0][0], f'{result[0][1]*100:.3f}', result[0][2], result[0][3]))

print("{:<38} {:<10} {:<45} {:<10}".format('Name','Percent','Range','Count'))
unification(results_http)
unification(results_raw)
unification(results_voip)
unification(results_mail)
unification(results_im)
unification(results_ftp)
unification(results_term)
