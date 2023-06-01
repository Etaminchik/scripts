import sys, re
from collections import defaultdict
import logging
from ftplib import FTP
from netaddr import IPNetwork, IPAddress, cidr_merge
from netaddr.core import AddrFormatError
from fdpi import Profile, Service, Bind
from shell import Shell

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

logger = logging.getLogger('checker')


class Checker(Shell):

    def __init__(self, abonents_data):
        self.abonents_data = abonents_data
        self.excess_limit = 200

    def get_abonents_data(self):
        for login, row in self.abonents_data.items():
            ips = set()
            try:
                for cidrs in row['ips'].split(','):
                    for cidr in IPNetwork(cidrs):
                        ips.add(cidr)
            except AddrFormatError:
                logger.critical('Invalid IPS for "{}" ({})'.format(login, row['ips']))
                raise Exception(login, row['ips'])
            ips = cidr_merge(ips)
            self.abonents_data[login]['raw_ips'] = row['ips']
            self.abonents_data[login]['ips'] = ips
        logger.info('Count of abonents in DB: {}'.format(len(self.abonents_data)))
        return self.abonents_data

    def get_skat_data(self):
        binds = Bind().all()
        profiles = defaultdict(dict)
        for login, profile in binds.items():
            profiles[login]['ips'] = cidr_merge(binds[login])
        logger.info('Count of abonents in SKAT: {}'.format(len(profiles)))
        return profiles

    def fill_rate(self, login, rate, add_service=False, service=''):
        logger.info('{} set profile {}'.format(login, rate))
        commands = [
            '/usr/sbin/fdpi_ctrl load --policing --profile.name {} --login {}'.format(rate, login),
            '/usr/sbin/fdpi_ctrl del --service 5 --login {}'.format(login)
        ]
        logger.debug(self._execute_multi(commands))

    def add_service(self, login, service):
        commands = []
        if service == '':
            logger.debug('{} add service'.format(login))
            commands.append(
                '/usr/sbin/fdpi_ctrl load --service 5 --login {}'.format(login)
            )
        else:
            logger.debug('{} add service name {}'.format(login, service))
            commands.append(
                '/usr/sbin/fdpi_ctrl load --service 5 --profile.name {} --login {}'.format(service, login)
            )
        logger.debug(self._execute_multi(commands))

    def fill_cidrs(self, login, cidrs):
        cidrs = cidrs.replace(', ', ',')
        logger.info('{} add cidrs'.format(login))
        commands = [
            '/usr/sbin/fdpi_ctrl del --bind_multi --login {}'.format(login),
            '/usr/sbin/fdpi_ctrl load --bind_multi --user {}:{}'.format(login, cidrs),
        ]
        logger.debug(self._execute_multi(commands))

    def remove_from_skat(self, login):
        self.excess_limit -= 1
        if self.excess_limit > 0:
            logger.info('remove excess {} profile'.format(login))
            commands = [
                '/usr/sbin/fdpi_ctrl del --bind_multi --login {}'.format(login),
                '/usr/sbin/fdpi_ctrl del --policing --login {}'.format(login),
                '/usr/sbin/fdpi_ctrl del --service 5 --login {}'.format(login)
            ]
            logger.debug(self._execute_multi(commands))

    def __diff(self, first, second):
        second = set(second)
        return [item for item in first if item not in second]

    def check(self):
        profiles = self.get_skat_data()
        dump = self.get_abonents_data()
        errors = {
            'excess': 0,
            'not_found': 0,
            'invalid_binds': 0,
        }

        result = defaultdict(list)

        for login, row in dump.items():
            if login in profiles:
                profile = profiles.pop(login)
                if sorted(profile['ips']) != sorted(row['ips']):
                    logger.error('{} cidr binds is invalid:'.format(login))
                    logger.error('\t\t{}'.format(profile['ips']))
                    logger.error('\t\t{}'.format(row['ips']))
                    logger.error('\t\t{}'.format(
                        self.__diff(profile['ips'], row['ips'])))
                    logger.error('\t\t{}'.format(
                        self.__diff(row['ips'], profile['ips'])))
                    self.fill_cidrs(login, row['raw_ips'])
                    errors['invalid_binds'] += 1
                    result['invalid_binds'].append(login)
            else:
                logger.info('{} not found on skat'.format(login))
                self.fill_cidrs(login, row['raw_ips'])
                errors['not_found'] += 1
                result['not_found'].append(login)

        for login in profiles.keys():
            self.remove_from_skat(login)
            errors['excess'] += 1

        logger.info('Summary:'.format(errors))
        logger.info('\texcess count {}'.format(errors['excess']))
        logger.info('\tinvalid cidr binds count {}'.format(
            errors['invalid_binds']))
        logger.info('\tnot found count {}'.format(errors['not_found']))
        logger.info('\tall count {}'.format(sum(errors.values())))

if __name__ == '__main__':
    ftp = FTP('192.168.1.1')
    ftp.login('cdr', 'YrGd7Tgehtw=')
    ftp.retrbinary('RETR binds.txt', open('binds.txt', 'wb').write)
    ftp.quit()

    data = {}
    with open('binds.txt') as f:
        for row in f:
            login, ips = row.split(';')
            ips = re.sub("^\s+|\n|\r|\s+$", '', ips)
            data[login] = {'ips': ips}

    checker = Checker(data)
    checker.check()
