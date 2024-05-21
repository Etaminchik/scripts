#Config
CRITICAL_PERCENTAGE = 95
CLEANING_LIMIT_IS_DAYS = 190
PATH_DB = '/var/lib/pgsql'

#Database
HOST="127.0.0.1"
PORT="54321"
NAME="vasexperts"
USER="postgres"
PASSWORD=""

#Log format:
# [N] - The table was not deleted due to the day limit.
# [D] - The table has been deleted.
# [C] - The script has finished working due to the large free disk space.
import psycopg2
from datetime import datetime,timedelta
import os, time

class Cleaner:
    def __init__(self) -> None:
        self.con = psycopg2.connect(database=NAME,
                                    user=USER,
                                    password=PASSWORD,
                                    host=HOST,
                                    port=PORT)
        self.statvfs = os.statvfs(PATH_DB)
        self.critical_space = CRITICAL_PERCENTAGE
        self.critical_days = CLEANING_LIMIT_IS_DAYS
        self.cur = self.con.cursor()


    def check_scheme(self):
        _select = "select cmvr_version from oimm.component_versions where cmvr_name = 'oimc_scheme'"
        self.cur.execute(_select)
        return int(self.cur.fetchall()[0][0].split('.')[0])

    def check_occupied_space(self):
        return 100 - (self.statvfs.f_bfree / self.statvfs.f_blocks * 100)
        
    def search_table(self):
        if self.check_scheme() < 8:
            _select = """SELECT "partition",range_min,range_max 
            FROM public.pathman_partition_list 
            where parent = 'oimc.raw_flows'::regclass 
            order by range_min 
            limit 1"""
            self.cur.execute(_select)
            return self.cur.fetchall()[0]

        else:
            _select = """select rngp_partition,rngp_from_value,rngp_to_value 
            from oimc.range_partitions 
            where rngp_base_table = 'oimc.raw_flows'::regclass 
            order by rngp_from_value 
            limit 1 """
            self.cur.execute(_select)
            return self.cur.fetchall()[0]

    def drop_table(self,table):
        _drop_table = f"DROP TABLE IF EXISTS {table};"
        self.cur.execute(_drop_table)
        self.con.commit() #save

    def run(self):
        if self.check_occupied_space() > self.critical_space:
            while self.check_occupied_space() > self.critical_space:
                occupied_space = self.check_occupied_space()
                table,range_min,range_max = self.search_table()

                if range_min > datetime.now() - timedelta(days=self.critical_days):
                    print("[ {} ] [N] BUSY: {:.2f}% | CRIT: {}% | TABLE: {} | MIN: {} | The table was not deleted due to the date limit".format(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            occupied_space,
                            self.critical_space,
                            table,
                            range_min))
                    break
                else:
                    self.drop_table(table)
                    print("[ {} ] [D] BUSY: {:.2f}% | CRIT: {}% | TABLE: {} | MIN: {} | The table has been deleted".format(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            occupied_space,
                            self.critical_space,
                            table,
                            range_min))
        else:
            print("[ {} ] [C] BUSY: {:.2f}% | CRIT: {}% | There is more free space than the threshold value.".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.check_occupied_space(),
                self.critical_space))

if __name__ == "__main__":
    Cleaner().run()
