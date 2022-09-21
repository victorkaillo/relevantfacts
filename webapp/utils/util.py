from kinvolog import LogService
from environs import Env

env = Env()
env.read_env()

def handlerToken(Authorization):
    if Authorization == env.str("LOCAL_HASH"):
        return True, {'message': "Authorized."}
    elif Authorization == None:
        LogService.sendLog(success=False, job="handlerToken", source="kinvo.crawler.conta.azul",
            textMessage="User tried get Authorization without a token", level="WARN", environment="PRODUCTION")
        return False, {'message': 'Unauthorized. Try use a valid Token in Authorization header'}
    else:
        LogService.sendLog(success=False, job="handlerToken", source="kinvo.crawler.conta.azul",
            textMessage="User tried get Authorization with an invalid Token", level="WARN", environment="PRODUCTION")
        return False, {'message': 'Unauthorized. Token Invalid'}

class NullFile(Exception):
    # Erro para quando o arquivo gerado for um arquivo vazio
    def __init__(self, passo, message = "Falha ao coletar e concatenar os dados") -> None:

        self.passo = passo
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'{self.passo} : {self.message}'