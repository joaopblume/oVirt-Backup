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


# Create connection to the oVirt engine
def create_connection():
    connection = sdk.Connection(
    url=url,
    username=user,
    password=passwd,
    ca_file=certificate,
    log=logging.getLogger(),
    debug=True
    )

    print('Connection successful')
    return connection

# Access VM properties: vm.id, vm.name, etc...
def get_vm(connection, vm_name):
    vms_service = connection.system_service().vms_service()
    vm = vms_service.list(search=f'name={vm_name}', all_content=True)[0]
    if vm:
        return vm
    else:
        raise NameError('Incorrect VM name')

def get_backup(connection, vm_id):
    vm_service = connection.system_service().vms_service().vm_service(id=vm_id)
    backups_service = vm_service.backups_service().list()
    if len(backups_service) > 0:
        for bkp in backups_service:
            if bkp.description == uniq_bkp:
                return bkp
    else:
        raise FileNotFoundError('No backups exist for this VM')

# Put the machine in backup mode and take a backup
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
        print('Backup started')
    else:
        raise RuntimeError('Cannot backup a VM without disks')
    
    progress("Waiting for backup to complete.")

    # While the backup has not finished, keep checking its status
    while backup.phase != types.BackupPhase.READY:
        sleep(1)
        backup = get_backup(connection, vm_id)

    if backup.to_checkpoint_id is not None:
        progress(f"Checkpoint ID created: {backup.to_checkpoint_id}")
    chk = checkpoints.chk_list
    chk[vm_name] = backup.to_checkpoint_id
    update_checkpoints(chk)

    return backup

# Get disks for backup
def get_disks(connection, vm_id):

    system_service = connection.system_service()
    vm_service = system_service.vms_service().vm_service(id=vm_id)
    disk_attachments = vm_service.disk_attachments_service().list()

    disks = []
    for disk_attachment in disk_attachments:
        disk_id = disk_attachment.disk.id
        disk = system_service.disks_service().disk_service(disk_id).get()
        if not specified_disks:
            disks.append(disk)
            print(disk.name)
        else:
            if disk.name in specified_disks:
                disks.append(disk)
    return disks

# Take the VM out of backup mode
def finalize_backup(connection, vm_id):
    bkp = get_backup(connection, vm_id)
    system_service = connection.system_service()
    vms_service = system_service.vms_service()
    backups_service = vms_service.vm_service(id=vm_id).backups_service()
    backups_service.backup_service(id=bkp.id).finalize()

# Download the backup
def download_backup(connection, backup, incremental=False):
    if modo_bkp and modo_bkp.lower() == 'incremental':
        incremental = True
    system_service = connection.system_service()
    vms_service = system_service.vms_service()
    backups_service = vms_service.vm_service(id=vm.id).backups_service()
    backup_service = backups_service.backup_service(id=backup.id)

    backup_disks = backup_service.disks_service().list()

    for disk in backup_disks:
        
        # Check if there is any incremental mode in the disks
        has_incremental = disk.backup_mode == types.DiskBackupMode.INCREMENTAL

        # If incremental mode is not available for the disk
        if incremental and not has_incremental:
            progress("Incremental not available for disk: %r" % disk.id)

        final_dir = f'{backup_dir}/{vm_name}'

        # Create the backup directory for the VM if it doesn't exist yet
        os.system(f'mkdir -p {final_dir}')
        if has_incremental:
            level = disk_chain_level(final_dir, disk.id)
        else:
            level = str(1)
            
        
        # File name = <vm_name>_<date>_<checkpoint>_<disk_id>_<backup_mode>_<chain_level>
        file_name = "{}_{}_{}_{}_{}_{}.qcow2".format(
            vm.name, hoje, backup.to_checkpoint_id, disk.id, disk.backup_mode, level)
        disk_path = os.path.join(final_dir, file_name)

        # When incremental, look for the last full to increment
        if has_incremental:
            backing_file = find_backing_file(
                backup.from_checkpoint_id, disk.id)
        else:
            backing_file = None

        # Start download
        download_disk(
            connection, backup.id, disk, disk_path,
            incremental=has_incremental,
            backing_file=backing_file)
        
# Starts downloading disks
def download_disk(connection, backup_id, disk, disk_path, incremental=True, backing_file=None):
    progress(f"Downloading {'incremental' if incremental else 'full'} backup DISK: {disk.name}")
    progress(f"Creating backup file: {disk_path}")
    if backing_file:
        progress(f"Using backup file: {backing_file}")

    transfer = imagetransfer.create_transfer(
        connection,
        disk,
        types.ImageTransferDirection.DOWNLOAD,
        backup=types.Backup(id=backup_id))
    try:
        progress(f"Image transfer {transfer.id} is ready")
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
        progress("Finalizing image transfer")
        imagetransfer.finalize_transfer(connection, transfer, disk)

    progress("Download completed successfully")    

# Find previous backup to increment with the new backup file
def find_backing_file(checkpoint_uuid, disk_uuid):
    pattern = os.path.join(backup_dir, f"*_{checkpoint_uuid}_{disk_uuid}_*")
    matches = glob.glob(pattern)
    if not matches:
        return None

    return os.path.relpath(matches[0], backup_dir)

# Update the checkpoints file with the last created checkpoint
def update_checkpoints(final_dict):
    f = open(checkpoints_location, "w")
    f.write('chk_list = ' + str(final_dict))
    f.close()


# Determine the position in the disk backup chain
# full = 1
# first incremental = 2
# second incremental = 3
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


def clear_backups(path):
    files = os.listdir(path)
    for file in files:
        os.remove(os.path.join(path, file))

# Parameter validation and input
try:
    vm_name = sys.argv[1]
    print('VM Name: ' + vm_name)
except:
    raise ValueError('VM name must be specified')
try:
    lista_full = sys.argv[2]
    print('Full list: OK')
except:
    raise ValueError('Full/incremental policy must be specified')

if len(lista_full) < 7:
    raise KeyError('Full/incremental policy must be specified for all days of the week')

# 0 = Monday, 6 = Sunday
today = date.today().weekday()

if lista_full[today] == '1':
    modo_bkp = 'full'
    checkpoint = None
    #clear_backups(f'{backup_dir}/{vm_name}')

else:
    modo_bkp = 'incremental'
    checkpoint = checkpoints.chk_list[vm_name]

print('Backup mode: ' + modo_bkp)

try:
    specified_disks = sys.argv[3]
    specified_disks = specified_disks.split(',')
except:
    specified_disks = None

print('Initial validations OK')

#####################################
############ PROGRAM START ##########
#####################################

# CREATE CONNECTION
print('Creating connection')
conn = create_connection()

# FIND VM BY NAME
vm = get_vm(conn, vm_name)

# BACKUP DATE
hoje = date.today().strftime("%Y%m%d")

# DATETIME FOR BACKUP
uniq_bkp = str(datetime.now())

# TAKE BACKUP
bkp = take_backup(conn, vm.id, checkpoint)
# DOWNLOAD BACKUP
download_backup(conn, bkp)
# FINALIZE VM BACKUP MODE
finalize_backup(conn, vm.id)

# CLOSE API CONNECTION AND FREE RESOURCES
conn.close()
