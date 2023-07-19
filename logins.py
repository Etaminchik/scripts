'''pip install psycopg2'''
import psycopg2
from datetime import datetime, timedelta
from string import Template

date_l = (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d 23:00:00")
date_h = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d 01:00:00")



con = psycopg2.connect(
  database="vasexperts", 
  user="oim_admin", 
  password="admin", 
  host="127.0.0.1", 
  port="54321"
)
cur = con.cursor()  

cur.execute('select oper_telco_code,oper_name from oims.operators order by oper_id desc')
operators = cur.fetchall()



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

for operator in operators:
    print(f"""Operator: {operator[1]}""")
    logins = []
    results_http,results_raw,results_voip,results_mail,results_im,results_ftp,results_term = [],[],[],[],[],[],[]

    for i in range(len(partitions)):

        if 'oimc.http' in partitions[i][0]:
            part_type_ = 'htrq'
            cur.execute(select_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_http.append(['Интернет-посещения HTTP', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)



        elif 'oimc.raw' in partitions[i][0]:
            part_type_ = 'rawf'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_raw.append(['Передача данных (закрытые протоколы)', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.voip' in partitions[i][0]:
            part_type_ = 'vipc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_voip.append(['VoIP-соединения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.mail' in partitions[i][0]:
            part_type_ = 'emlc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_mail.append(['Email-сообщения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.im_c' in partitions[i][0]:
            part_type_ = 'imcn'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_im.append(['IM-сообщения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.ftp_c' in partitions[i][0]:
            part_type_ = 'ftpc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_ftp.append(['FTP-соединения', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

        elif 'oimc.term' in partitions[i][0]:
            part_type_ = 'trmc'
            cur.execute(select_logins.substitute(partition=partitions[i][0],type_=part_type_,  operator=operator[0]))
            result = cur.fetchall()
            cur.execute(select_all_logins.substitute(partition=partitions[i][0], type_=part_type_,  operator=operator[0]))
            result_all = cur.fetchall()
            if len(result_all) > 0:
                results_term.append(['Терминальный доступ', len(result) / len(result_all), f'{partitions[i][2]} - {partitions[i][3]}', len(result)])
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], len(result)))
            else:
                print("{:<10} {:<32} {:<20}".format(f' ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'The partition is empty'))
            for login in result:
                if login not in logins:
                    logins.append(login)

    def unification(result):
        procent = 0
        if len(result) > 1:
            for i in range(len(result)):
                procent += result[i][1]
            procent = procent / len(result)
            print("{:<38} {:<10} {:<45} {:<10}".format(result[0][0], f'{100 - procent*100:.3f}', result[0][2]))
        elif len(result) == 1:
            print("{:<38} {:<10} {:<45} {:<10}".format(result[0][0], f'{100 - result[0][1]*100:.3f}', result[0][2]))


    print("{:<38} {:<10} {:<45} {:<10}".format('Name','Percent','Range','Count'))
    unification(results_http)
    unification(results_raw)
    unification(results_voip)
    unification(results_mail)
    unification(results_im)
    unification(results_ftp)
    unification(results_term)


    result = ''
    file_name = operator[1] + ' ' + datetime.today().strftime("%Y%m%d %H%M%S") + '.txt'
    f = open(file_name,'w') 
    for login in logins:
        result += login[0] + '\n'
    f.write(result)
    f.close()


