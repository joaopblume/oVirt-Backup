import ovirtsdk4 as sdk
import logging
from config import *
import sys


def create_connection():
    connection = sdk.Connection(
    url=url,
    username=user,
    password=passwd,
    ca_file=certificate,
    log=logging.getLogger(),
    debug=True
    )

    return connection

def get_vm(connection, vm_name):
    vms_service = connection.system_service().vms_service()
    vm = vms_service.list(search=f'name={vm_name}', all_content=True)
    if vm:
        vm_service = vms_service.vm_service(id=vm[0].id)
        vm_service.stop()
        vm_service.remove()
        print('VM removida com sucesso')
    else:
        raise NameError(f'Nenhuma VM {vm_name} encontrada')
    
# CRIA A CONEXAO
conn = create_connection()
vm_name = sys.argv[1]
# ENCONTRA A VM PELO NOME
vm = get_vm(conn, vm_name)

