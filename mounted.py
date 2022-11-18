import os
os.system('blkid > blkid.txt')

first_number_disk = 1 # С какого по счету /var/dump/disk[i] будут создаваться каталоги

file_disks= open('disks.txt','r', encoding='utf-8')
file_blkid= open('blkid.txt','r', encoding='utf-8')
mkdirs = ''
uuid = []
mkfs = ''
disks = []
while True:
    line = file_disks.readline()
    if not line:
        break
    disks.append(line)
disks = [line.rstrip() for line in disks]

for i in range(len(disks)):
    mkfs += 'mkfs.xfs /dev/' + disks[i] + ' -f && '
    
mkdirs = 'mkdir /var/dump/disk{' + str(first_number_disk) + '..' + str(len(disks)+first_number_disk-1) + '}'

while True:
    line = file_blkid.readline()
    if not line:
        break
    if line.split()[0][5:-1] in disks:
        fstab_line = line.split()[1].replace('"','') + ' /var/dump/disk' + str(len(uuid) + first_number_disk) + ' xfs defailts 0 0'
        uuid.append(fstab_line)

file_result = open('result.txt','w', encoding='utf-8')

file_result.write(str(mkdirs + '\n'))
file_result.write('\n')
file_result.write(str(mkfs[:-3] + '\n'))
file_result.write('\n')

for i in range(len(uuid)):
    file_result.write(str(uuid[i] + '\n'))
