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
cur.execute(partitions)
partitions = cur.fetchall()

for operator in operators:
    print(f"""Operator: {operator[1]}""")
    ip_nums = f"""select oinp_subnet from oims.oper_ip_numbering_plan_history where oinp_oper_id = {operator[0]}"""
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
            cur.execute(select_nat.substitute(partition=partitions[i][0], type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_http.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.raw' in partitions[i][0]:
            part_type_ = 'rawf'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_raw.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.voip' in partitions[i][0]:
            part_type_ = 'vipc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_voip.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.mail' in partitions[i][0]:
            part_type_ = 'emlc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_mail.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.im_c' in partitions[i][0]:
            part_type_ = 'imcn'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_im.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.ftp_c' in partitions[i][0]:
            part_type_ = 'ftpc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
            result = cur.fetchall()
            if result[0][1] > 0:
                results_ftp.append([result[0][0] / result[0][1], f'{partitions[i][2]} - {partitions[i][3]}', result[0][1]])
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 100 - result[0][0] / result[0][1] * 100))
            else:
                print("{:<10}{:<32}{:<20}".format(f'[NAT] ({i+1}/{len(partitions)}) Checked: ', partitions[i][0], 'Not NAT records'))

        elif 'oimc.term' in partitions[i][0]:
            part_type_ = 'trmc'
            cur.execute(select_nat.substitute(partition=partitions[i][0],type_=part_type_, ip_num=ip_num(part_type_), oper_id=operator[0]))
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
          print("{:<38}{:<12}{:<45}{:<10}".format(name, f'{100 - procent*100:.3f}', result[0][1], sum))
        elif len(result) == 1:
          print("{:<38}{:<12}{:<45}{:<10}".format(name, f'{100 - result[0][0]*100:.3f}', result[0][1], result[0][2]))
        else:
          print("{:<38}{:<12}{:<45}".format(name, 'Данных нет', date_l + ' - ' + date_h))


    print('\n')
    print("="*30 + ": NAT stats :" + "="*30)
    print("{:<38}{:<12}{:<45}{:<10}".format('Name','Percent','Range','Count'))
    unification(results_http,'Интернет-посещения HTTP')
    unification(results_raw,'Передача данных (закрытые протоколы)')
    unification(results_voip,'VoIP-соединения')
    unification(results_mail,'Email-сообщения')
    unification(results_im,'IM-сообщения')
    unification(results_ftp,'FTP-соединения')
    unification(results_term,'Терминальный доступ')
