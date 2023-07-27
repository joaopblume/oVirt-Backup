import typer
import ovirtsdk4 as sdk
import logging
import ovirtsdk4.types as types
from os import listdir
from config import *
from time import sleep
import subprocess
from helpers import imagetransfer
from ovirt_imageio import client

app = typer.Typer()

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


@app.command()
def restore(vm_name: str, os : str = typer.Option(default='other', prompt=False), cpu : str = typer.Option(..., prompt=True), memory : str = typer.Option(..., prompt=True), directory : str = typer.Option(..., prompt=True), storage_domain : str = typer.Option(default='OLVMREPO4', prompt=True)):
    
    print("Disks are located in: " + directory)

    connection = create_connection()
    disks_service = connection.system_service().disks_service()

    sucess = 0
    restored_disks = []
    disk_ids = []

    print("Restoring disks")

    for disk in  listdir(directory):
        if disk.endswith(".qcow2"):
            restored_disks.append(disk)
            out = subprocess.check_output(['qemu-img', 'info', '--output', 'json', disk]).decode('utf-8')
            size = int(out.split('\"virtual-size\": ')[1].split(',')[0])
            initial_size = int(out.split('\"actual-size\": ')[1].split(',')[0])
            print()
            upload = disks_service.add(
                types.Disk(
                    name=disk,
                    format=types.DiskFormat.COW,
                    initial_size=initial_size,
                    provisioned_size=size,
                    backup=types.DiskBackup.INCREMENTAL,
                    content_type=types.DiskContentType.DATA,
                    storage_domains=[
                        types.StorageDomain(
                            name=storage_domain
                        )
                    ]
                )
            )

            uploading_status = disks_service.disk_service(upload.id)
            while True:
                sleep(1)
                disk = uploading_status.get()
                if disk.status == types.DiskStatus.OK:
                    print("Disk upload completed")
                    break
                else:
                    print("Disk upload status: %s" % disk.status)

            host = imagetransfer.find_host(connection, storage_domain)

            transfer = imagetransfer.create_transfer(
                connection,
                upload,
                types.ImageTransferDirection.UPLOAD,
                host=host,
                inactivity_timeout=240,
            )

            try:

                print("Initiating image transfer")
                upload_url = transfer.transfer_url
                
                with client.ProgressBar() as pb:
                    client.upload(
                    upload.name,
                    upload_url,
                    certificate,
                    buffer_size=client.BUFFER_SIZE,
                    progress=pb,
                    )
            
                disk_ids.append(upload.id)
            
            except:
        
                print("Error uploading disk")
                imagetransfer.cancel_transfer(connection, transfer)
                raise
            
            imagetransfer.finalize_transfer(connection, transfer, disk)
            sucess += 1

    total_disks = restored_disks.__len__()

    if sucess == total_disks:
        print("All disks restored successfully")

        print(f"Initiating VM {vm_name} creation")

        vms_service = connection.system_service().vms_service()

        vm = vms_service.add(
            types.Vm(
                name=vm_name + "-restored",
                
                cluster=types.Cluster(
                    name='Cluster',
                ),

                template=types.Template(
                    name='Blank',
                ),

                os=types.OperatingSystem(
                    type=os,
                ),

                type=types.VmType.SERVER,

                cpu=types.Cpu(
                    topology=types.CpuTopology(
                        cores=int(cpu),
                        sockets=1,
                    ),
                ),

                description="Restored VM",

                memory=int(memory) * 1024 * 1024 * 1024,

                memory_policy=types.MemoryPolicy(
                    guaranteed=int(memory) * 1024 * 1024 * 1024,
                    max=int(memory) * 1024 * 1024 * 1024,
                ),

                host=types.Host(
                    name=host,
                ),
            ),
        )

        print("VM created successfully")

        print("Attaching disks to VM")
        disk_attachments_service = vms_service.vm_service(vm.id).disk_attachments_service()

        for disk_id in disk_ids:
            disk_attachments_service.add(
                types.DiskAttachment(
                    disk=types.Disk(
                        id=disk_id,
                    ),
                    interface=types.DiskInterface.SATA,
                    bootable=True,
                    active=True,
                ),
            )
        
        print("Disks attached successfully")

    else:
        print("Only " + str(sucess) + " of " + str(total_disks) + " disks were restored")
        raise Exception("Error restoring disks")

if __name__ == "__main__": 
    app()
