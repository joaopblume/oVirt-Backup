# oVirt-Backup
Create and Download disk backups (full and incremental) from the specified virtual machine on a KVM/OLVM cluster. Also merge incremental disks to full and restore it.


# Installation process

yum -y install \
gcc \
libxml2-devel \
python3-devel

yum -y install qemu-img
yum -y install libcurl-devel
yum -y install -y openssl-devel

pip3 install ovirt-engine-sdk-python
pip3 install ovirt_imageio
