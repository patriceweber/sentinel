
import os, re
from re import RegexFlag
import zipfile


inputdir = r'D:\_RSDATA_\Sentinel-2'

dirname = 'T52JFS_20181130T011721_B01.jp2'
#dirname = 'S2A_MSIL1C_20181130T011721_N0207_R088_T52JFS_20181130T024335'
#dirname = 'S2A_S2MSI2Ap_20181130T011721_N0207_R088_T52JFS_20181130T024335_20181130T011721_N0207_R088_T52JFS_20181130T024335'


pattern = r'T([A-Z0-9]{5})_(\d{8})T(\d{6})_([A-Z0-9]{3})\.JP2$'
#pattern = r'^([A-Z0-9]{3})_([A-Z0-9_]{6,8})_([A-Z0-9]{15})_N(\d{4})_R(\d{3})_T([A-Za-z0-9]{5})_(\d{8})T(\d{6})'
#pattern = r'^([A-Za-z0-9]{3})_([A-Za-z0-9]{6})_([A-Za-z0-9]{15})_N(\d{4})_R(\d{3})_T([A-Za-z0-9]{5})_(\d{8})T(\d{6})'

re_compile = re.match(pattern, dirname, RegexFlag.IGNORECASE)

if re_compile is not None:

    groups = re_compile.groups()
    imgName = groups[0] + '_' + groups[1] + '_' + groups[3] + '.jp2'
    
    ind = 1
    for group in groups:
        print('Group%d: %s' % (ind, group))
        ind = ind+1


fzip = os.path.join(inputdir,'S2A_MSIL1C_20181130T011721_N0207_R088_T52JFS_20181130T024335.zip')

zf = zipfile.ZipFile(fzip)

filelist = zf. namelist()

#S2_bands = [x for x in filelist if re.search(r'_B(\d+)\.jp2$', x, flags=RegexFlag.IGNORECASE)]
S2_bands = [x for x in filelist if re.search(r'_[A-Za-z0-9_]{3}\.jp2$', x, flags=RegexFlag.IGNORECASE)]

for fband in S2_bands:
    
    try:        
        ind = fband.rfind("/")
        fout = fband[ind+1:]
        outputfilename = os.path.join(inputdir, fout)
    
        data = zf.read(fband)
        
        with open(outputfilename, 'wb') as output:
            output.write(data)
            
        print('Output image: %s' % fout)
        
    except KeyError:
        print('ERROR: Did not find %s in zip file' % fband)


zf.close()

exit(0)
