import os
import ovirtsdk4 as sdk
import logging
import ovirtsdk4.types as types
from datetime import date, datetime
import sys
from helpers import imagetransfer
from time import sleep
from helpers.common import progress
import glob
from ovirt_imageio import client
import checkpoints
from config import *


# cria a conexao com o ovirt-engine
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

# acessar propriedades da vm: vm.id, vm.name, etc...
def get_vm(connection, vm_name):
    vms_service = connection.system_service().vms_service()
    vm = vms_service.list(search=f'name={vm_name}', all_content=True)[0]
    if vm:
        return vm
    else:
        raise NameError('Nome da vm incorreto')

def get_backup(connection, vm_id):
    vm_service = connection.system_service().vms_service().vm_service(id=vm_id)
    backups_service = vm_service.backups_service().list()
    if len(backups_service) > 0:
        for bkp in backups_service:
            if bkp.description == uniq_bkp:
                return bkp
    else:
        raise FileNotFoundError ('Não existem backups para esta VM')

# coloca a maquina em modo backup e tira um backup
def take_backup(connection, vm_id, checkpoint):
    from_checkpoint = checkpoint
    vm_service = connection.system_service().vms_service().vm_service(id=vm_id)
    backups_service = vm_service.backups_service()

    disks = get_disks(connection, vm_id)

    if len(disks) > 0:
        backup = backups_service.add(
            types.Backup(
                disks=disks,
                from_checkpoint_id=from_checkpoint,
                description=uniq_bkp
            )
        )

    else:
        raise RuntimeError ('Não é possivel fazer o backup de uma máquina sem discos')
    
    progress("Aguardando a finalização do backup.")

    # enquanto o backup nao tiver terminado, continua verificando o status dele
    while backup.phase != types.BackupPhase.READY:
        sleep(1)
        backup = get_backup(connection, vm_id)

    if backup.to_checkpoint_id is not None:
        progress(f"Checkpoint id criado: {backup.to_checkpoint_id}")
    chk = checkpoints.chk_list
    chk[vm_name] = backup.to_checkpoint_id
    update_checkpoints(chk)

    return backup

# busca os discos para o backup
def get_disks(connection, vm_id):

    system_service = connection.system_service()
    vm_service = system_service.vms_service().vm_service(id=vm_id)
    disk_attachments = vm_service.disk_attachments_service().list()

    disks = []
    for disk_attachment in disk_attachments:
        disk_id = disk_attachment.disk.id
        disk = system_service.disks_service().disk_service(disk_id).get()
        disks.append(disk)

    return disks

# tira a vm do modo backup
def finalize_backup(connection, vm_id):
    bkp = get_backup(connection, vm_id)
    system_service = connection.system_service()
    vms_service = system_service.vms_service()
    backups_service = vms_service.vm_service(id=vm_id).backups_service()
    backups_service.backup_service(id=bkp.id).finalize()

# faz o download do backup
def download_backup(connection, backup, incremental=False):
    if modo_bkp and modo_bkp.lower() == 'incremental':
        incremental = True
    system_service = connection.system_service()
    vms_service = system_service.vms_service()
    backups_service = vms_service.vm_service(id=vm.id).backups_service()
    backup_service = backups_service.backup_service(id=backup.id)

    backup_disks = backup_service.disks_service().list()

    for disk in backup_disks:
        
        # verifica se há algum modo incremental nos discos
        has_incremental = disk.backup_mode == types.DiskBackupMode.INCREMENTAL

        # se o modo incremental não estiver disponível para o disco
        if incremental and not has_incremental:
            progress("Incremental não disponível para o disco: %r" % disk.id)

        final_dir = f'{backup_dir}/{vm_name}'

        # cria o diretorio dos backups da vm se ainda nao existir
        os.system(f'mkdir -p {final_dir}')
        if has_incremental:
            level = disk_chain_level(final_dir, disk.id)
        else:
            level = str(1)
            limpa_backups(final_dir)
        
        # nome do arquivo = <nome_da_vm>_<checkpoint>_<disk_id>_<modo_backup>_<chain_level>
        file_name = "{}_{}_{}_{}_{}_{}.qcow2".format(
            vm.name, hoje, backup.to_checkpoint_id, disk.id, disk.backup_mode, level)
        disk_path = os.path.join(final_dir, file_name)

        # quando incremental, busca pelo ultimo full para incrementar
        if has_incremental:
            backing_file = find_backing_file(
                backup.from_checkpoint_id, disk.id)
        else:
            backing_file = None

        # faz o download
        download_disk(
            connection, backup.id, disk, disk_path,
            incremental=has_incremental,
            backing_file=backing_file)
        
# inicia o download dos discos
def download_disk(connection, backup_id, disk, disk_path, incremental=True, backing_file=None):
    progress(f"Fazendo download do backup {'incremental' if incremental else 'full'} DISCO: {disk.name}")
    progress(f"Criando arquivo de backup: {disk_path}")
    if backing_file:
        progress(f"Utilizando arquivo de backup: {backing_file}")

    transfer = imagetransfer.create_transfer(
        connection,
        disk,
        types.ImageTransferDirection.DOWNLOAD,
        backup=types.Backup(id=backup_id))
    try:
        progress(f"Transferencia de imagem {transfer.id} está pronta")
        download_url = transfer.transfer_url

        with client.ProgressBar() as pb:
            client.download(
                download_url,
                disk_path,
                certificate,
                incremental=incremental,
                progress=pb,
                backing_file=backing_file,
                backing_format="qcow2"
            )
    finally:
        progress("Finalizando transferencia de imagem")
        imagetransfer.finalize_transfer(connection, transfer, disk)

    progress("Download completado com sucesso")    

# busca algum backup anterior para incrementar junto no novo arquivo de backup
def find_backing_file(checkpoint_uuid, disk_uuid):
    pattern = os.path.join(backup_dir, f"*_{checkpoint_uuid}_{disk_uuid}_*")
    matches = glob.glob(pattern)
    if not matches:
        return None

    return os.path.relpath(matches[0], backup_dir)

# atualiza o arquivo de checkpoints com o ultimo checkpoint criado
def update_checkpoints(final_dict):
    f = open(checkpoints_location, "w")
    f.write('chk_list = ' + str(final_dict))
    f.close()


# determina a posição na cadeia de backups do disco
# full = 1
# primeiro incremental = 2
# segundo incremental = 3
# ... 
def disk_chain_level(path, disk_id):
    biggest = 0
    files = os.listdir(path)
    for file in files:
        if disk_id in file:
            atual = int(file[-7])
            if atual > biggest:
                biggest = atual
    
    return str(biggest + 1)

def limpa_backups(path):
    files = os.listdir(path)
    for file in files:
        os.remove(file)

# Validação e entrada dos parâmetros
try:
    vm_name = sys.argv[1]
except:
    raise ValueError('O nome da vm deve ser especificado')
try:
    dia_bkp = sys.argv[2]
    day_name= ['segunda', 'terca', 'quarta', 'quinta', 'sexta', 'sabado','domingo']
    day = date.today().weekday()

    if day_name[day] == dia_bkp:
        modo_bkp = 'full'
        checkpoint = None
    else:
        modo_bkp = 'incremental'
        checkpoint = checkpoints.chk_list[vm_name]
except:
    raise ValueError ('O modo do backup deve ser especificado [full / incremental]')



#####################################
######## INICIO DO PROGRAMA #########
#####################################

# CRIA A CONEXAO
conn = create_connection()

# ENCONTRA A VM PELO NOME
vm = get_vm(conn, vm_name)

# DATA DO BACKUP
hoje = date.today().strftime("%Y%m%d")

# DATETIME PARA O BACKUP
uniq_bkp = str(datetime.now())

# TIRA O BACKUP
bkp = take_backup(conn, vm.id, checkpoint)
# FAZ O DOWNLOAD DO BACKUP
download_backup(conn, bkp)
# FINALIZA O MODO BACKUP DA VM
finalize_backup(conn, vm.id)

# FECHA A CONEXAO COM A API E LIBERA OS RECURSOS
conn.close()
 