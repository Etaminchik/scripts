from logging import getLogger

from collections import defaultdict

from netaddr.ip import IPAddress

from executor import Executor

logger = getLogger(__name__)


class Fdpi:

    _profile = None
    _service = None
    _bind = None

    @property
    def profile(self):
        if self._profile is None:
            self._profile = Profile()
        return self._profile

    @property
    def service(self):
        if self._service is None:
            self._service = Service()
        return self._service

    @property
    def bind(self):
        if self._bind is None:
            self._bind = Bind()
        return self._bind


class Base(Executor):

    def parse_output(self, payload):
        payload = payload.split('\n')
        result = payload[-2] if len(payload[-1]) < 5 else payload[-1]
        result = result.split(' ')[-1].split('/')
        return tuple(int(i) for i in result)

    def get_payload(self, command):
        """

        :param command:
        :return string:
        """
        code, payload = self.execute(command)
        result = False
        if code == 0:
            result = self.parse_output(payload)
            payload = payload.split('\n')[4:][:-4]
        return result, payload


class Profile(Base):

    def all(self):
        data = self.get_payload('/usr/sbin/fdpi_ctrl list all --policing')
        profiles = {}
        if data:
            _, data = data
            for line in data:
                line = line.strip().split('\t')
                login, *_, profile = line
                profiles[login] = profile
            logger.debug('loaded {} profiles from skat'.format(len(profiles)))
        return profiles


class Service(Base):

    def all(self):
        data = self.get_payload('/usr/sbin/fdpi_ctrl list all --service')
        services = {}
        if data:
            _, data = data
            for line in data:
                line = line.strip().split('\t')
                login, service, *_ = line
                services[login] = service
            logger.debug('loaded {} services from skat'.format(len(services)))
        return services


class Bind(Base):

    def all(self):
        data = self.get_payload('/usr/sbin/fdpi_ctrl list all --bind_multi')
        binds = defaultdict(set)
        if data:
            _, data = data
            for line in data:
                line = line.strip().split(':', 1)
                login, cidr = line
                binds[login].add(IPAddress(cidr))
            logger.debug('loaded {} cidr binds from skat'.format(len(binds)))
        return binds

