import subprocess
import sys


class Shell:

    def _execute(self, command):
        buffer = 'run command: {}\n'.format(command)
        try:
            result = subprocess.Popen(command.split(' '), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            buffer += result.stdout.read().decode()
        except:
            buffer += str(sys.exc_info())
        return buffer

    def _execute_multi(self, commands):
        buffer = ''
        for row in commands:
            result = self._execute(row)
            buffer += result
        return buffer
