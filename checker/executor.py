import subprocess


class Executor:

    def execute(self, command):
        """
        Выполняет комманду в консоли
        :param command: тестовая коммнда для выполнения
        :return: set(code, output) возвращает код завершения комманды и текстовый буффер с результатом выполнения
        """
        result = subprocess.Popen(command.split(' '), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        buffer, stderr = result.communicate()
        return result.returncode, buffer.decode() + (stderr.decode() if stderr is not None else '')

    def execute_multi(self, commands, force=False):
        """
        Выполняет несколько комманд последовательно, возвращает общий буффер выполнения
        :param commands: список комманд для выполенния
        :param force: флаг, при установке которого выполнения коммант будет продолжено, если одна комманда из списка не выполнится (по умолчанию False)
        :return: set(code, output) возвращает код завершения комманд и текстовый буффер с результатом выполнения
        """
        code, buffer = 0, ''
        for row in commands:
            _code, result = self.execute(row)
            code = code or _code
            buffer += result
            if _code > 0 and not force:
                break
        return code, buffer
