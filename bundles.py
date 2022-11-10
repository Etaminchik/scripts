'''pip install psycopg2'''
import psycopg2
from datetime import datetime, timedelta

rawf_telco_code = 1

con = psycopg2.connect(
  database="vasexperts", 
  user="oim_admin", 
  password="admin", 
  host="127.0.0.1", 
  port="54321"
)

date_now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

'''Num partition and ip masks'''

cur = con.cursor()  
cur.execute(f"SELECT \"partition\" FROM public.pathman_partition_list where parent = 'oimc.raw_flows'::regclass and range_max < '{date_now}' order by range_max desc limit 1")
partition = cur.fetchall()[0][0]
partition = ('oimc.raw_flows_' + str(int(partition[15:])-1))
print('Partition number:', partition)


'''Radius with'''

select_radius = 'select count(*) from ' + partition +  " where rawf_telco_code =" + str(rawf_telco_code) + " and rawf_subscriber_id = 'operator'"


cur.execute(select_radius)
count_radius = cur.fetchall()[0][0]


select_radius_all = 'select count(*) from ' + partition +  " where rawf_telco_code =" + str(rawf_telco_code)
cur.execute(select_radius_all)

count_radius_all = cur.fetchall()[0][0]

procent_radius_with = format(100 - (count_radius/count_radius_all*100),'.2f')

print('Radius: ', procent_radius_with, "% (", str(count_radius), '/', str(count_radius_all), ')')

'''Logins'''

cur.execute(f"select count(distinct rawf_subscriber_id) from {partition} where rawf_telco_code = {rawf_telco_code} and rawf_subscriber_id not in (SELECT sgnh_identity FROM oims.subs_generic_history)")
count_login = cur.fetchall()[0][0]

cur.execute(f"SELECT count(*) FROM oims.subscribers where subs_oper_id = {rawf_telco_code}")
count_logins = cur.fetchall()[0][0]
procent_logins = format(100-(count_login/count_logins*100),'.2f')

print('Logins: ', procent_logins, "% (", count_login, '/', count_logins, ')' )

'''NAT'''

cur.execute(f"select count(*) from {partition} where rawf_telco_code = {rawf_telco_code} and (rawf_client_address <<inet '10.0.0.0/8' OR rawf_client_address <<inet '172.16.0.0/12' or rawf_client_address  <<inet '192.168.0.0/16' or rawf_client_address  <<inet '100.64.0.0/10')  and not (rawf_server_address <<inet '10.0.0.0/8' OR rawf_server_address <<inet '172.16.0.0/12' or rawf_server_address  <<inet '192.168.0.0/16' or rawf_server_address <<inet '100.64.0.0/10')  and rawf_protocol_code != 65025 and rawf_nat_address is not null")
count_nat = cur.fetchall()[0][0]

cur.execute(f"select count(*) from {partition} where rawf_telco_code = {rawf_telco_code} and (rawf_client_address <<inet '10.0.0.0/8' OR rawf_client_address <<inet '172.16.0.0/12' or rawf_client_address  <<inet '192.168.0.0/16' or rawf_client_address  <<inet '100.64.0.0/10')  and not (rawf_server_address <<inet '10.0.0.0/8' OR rawf_server_address <<inet '172.16.0.0/12' or rawf_server_address  <<inet '192.168.0.0/16' or rawf_server_address <<inet '100.64.0.0/10')")
count_nat_all = cur.fetchall()[0][0]

procent_nat = format((count_nat/count_nat_all*100),'.2f')

print('Nat: ', procent_nat, "% (", count_nat, '/', count_nat_all, ')'  )

con.close() 
