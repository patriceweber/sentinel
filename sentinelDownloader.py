
import os, sys, re
from re import RegexFlag
import zipfile

import argparse

from collections import OrderedDict
from sentinelsat import SentinelAPI

import subprocess
import logging
import shutil

from sentinel.utils import Globals
from sentinel.utils import LogEngine

from sentinel.utils import readConfig
#from sentinel.utils import sanityCheck


class workflowException(Exception):
    """ Workflow exception class. When such an exception is raised,
        the workflow execution is stopped
    """

    def __init__(self, *args, **kwargs):
        super(workflowException, self).__init__(*args, **kwargs)
        return

    def __repr__(self):

        return 'workflowException{0}'.format(self.args)
# ========================================


def natural_keys(filename):
    """ Natural sort order helper function """

    def atoi(stext):
        return int(stext) if stext.isdigit() else stext

    return [atoi(c) for c in re.split(r'(\d+)', filename)]
# ========================================


def initLogger(name='', level=logging.DEBUG):

    # Init logging engine
    log = logging.getLogger(name)
    log.setLevel(level)
    logformat = logging.Formatter('%(asctime)s: %(threadName)s: [%(levelname)s]: %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

    # Add console ouput handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logformat)
    log.addHandler(console)

    log.info('Logger [%s] initialized', name)

    return log
# ========================================


def doCleanup(working_dir, exclusions=[]):

    if allowCleanup:

        # Exclude file types, we want to keep from deletion
        extensions = ['sgrd', 'xml', 'mgrd', 'sdat', 'prj', 'pgw', 'tgz']

        for ext in exclusions:
            [extensions.remove(x) for x in extensions if x == ext]

        for root, dirs, files in os.walk(working_dir):

            for ext in extensions:
                pattern = '.{0}$'.format(ext)
                [os.remove(os.path.join(root, x)) for x in files if re.search(pattern, x, flags=RegexFlag.IGNORECASE)]

            [os.remove(os.path.join(root, x)) for x in files if re.search(r'_B(\d+)\.TIF$', x, flags=RegexFlag.IGNORECASE)]


        _logger.info('Clean up done.')
# ========================================            

                 
def extractSentinel2Bands(outpath, scenes):
    
    procQueue = []
    
    for uid in scenes:
    
        archive = scenes[uid]
        zipname = archive['title'] + '.zip'
        fzip = os.path.join(outpath, zipname)
        _logger.info('Extracting bands from archive: %s' % zipname)

        
        if os.path.exists(fzip) and zipfile.is_zipfile(fzip):
        
            # create output folder
            pattern = r'^([A-Z0-9]{3})_([A-Z0-9_]{6,8})_([A-Z0-9]{15})_N(\d{4})_R(\d{3})_T([A-Za-z0-9]{5})_(\d{8})T(\d{6})'
            re_compile = re.match(pattern, archive['title'], RegexFlag.IGNORECASE)
    
            if re_compile is not None:
                
                groups = re_compile.groups()
                # Groups index start at zero. Get tile number (group 5) and date (group 6) and time (group 7)
                prod_discriminator = groups[6] + "T" + groups[7]
                outputdir = os.path.join(outpath, groups[5], prod_discriminator)
                
            else:
                # regular expression match failed, used product default name
                outputdir = os.path.join(outpath, archive['title'])
                
            os.makedirs(name=outputdir, exist_ok=True)
                
            # -- Process archive. Open zip file in read mode
            fzip = os.path.join(outpath, zipname)
            zf = zipfile.ZipFile(fzip)
            
            # get the name (with relative paths) of all the files in the archive
            filelist = zf. namelist()
            
            # get only the JPEG2000 images (jp2 extension)
            S2_bands = [x for x in filelist if re.search(r'_[A-Z0-9_]{3}\.jp2$', x, flags=RegexFlag.IGNORECASE)]
        
            for fband in S2_bands:
                
                try:        
                    # get the image file name without archive relative paths
                    ind = fband.rfind("/")
                    imgName = fband[ind + 1:]
                    
                    pattern = r'T([A-Z0-9]{5})_(\d{8})T(\d{6})_([A-Z0-9]{3})\.JP2$'
                    re_compile = re.match(pattern, imgName, RegexFlag.IGNORECASE)
                    
                    if re_compile is not None:
                        # when regular expression matches, rename the image file: tile_date_xxx.jp2
                        groups = re_compile.groups()
                        imgName = groups[0] + '_' + groups[1] + '_' + groups[3] + '.jp2'
                        
                    # set new image filename (full path)
                    outputfilename = os.path.join(outputdir, imgName)
                
                    # read image file from archive into byte array
                    data = zf.read(fband)
                    
                    # save byte array to file
                    with open(outputfilename, 'wb') as output:
                        output.write(data)
                        
                    #print('Output image: %s' % imgName)
                    _logger.debug('Extracting image file: [%s]' % (imgName) )
                    
                except KeyError:
                    _logger.info('Could not find %s in zip file' % fband)

        
            zf.close()
            
            procQueue.append(outputdir)

    return procQueue;
# ========================================


def executeSAGACommand(tool_cmd, f_out, desc=None):
     
    subprocess.call(saga_cmd + tool_cmd)

    # if output file exist log workflow step
    if os.path.isfile(f_out):
        if desc is None:
            desc = 'done with SAGA processing step'
    else:
        raise workflowException('SAGA process \'{0}\' output file is missing: {1}'.format(desc, f_out))

    _logger.info(desc + ': done.')
# ========================================

     
def doSAGAProcessingSteps(srcdir):


#  STEP 1    Create a lookup table
#       ========================================

    # Lookup table of (band # --> 'Reprojected band filename'). We will need it!
    f_Bands = os.listdir(srcdir)
    S2_bands_WGS = [x for x in f_Bands if re.search(r'_B[A-Z0-9_]{2}\_WGS\.TIF$', x, flags=RegexFlag.IGNORECASE)]
    S2_bands_WGS.sort(key=natural_keys)


    _logger.info('Building projected bands lookup table')

    Band_lookup = {}
    for fb in S2_bands_WGS:
        
        pattern = r'([A-Z0-9_]{5})_(\d{8})_B([A-Z0-9_]{2})_WGS\.TIF$'
        re_compile = re.match(pattern, fb, RegexFlag.IGNORECASE)
        
        if re_compile is not None:
            
            groups = re_compile.groups()
            
            tileID = groups[0]
            tileDate = groups[1]
            
            if groups[2][0] == '0':
                bnum = groups[2][1:]
            else:
                bnum = groups[2]
                            
            Band_lookup[bnum] = fb

        
#  STEP 2    Create a 3 bands composite image
#       ==========================================

    _logger.info('Creating RGB composite image')

    B3_sgrd = os.path.join(srcdir, '{0}_{1}_{2}{3}{4}_RGB_WGS.sgrd'.format(tileID, tileDate, '12', '8A', '05'))
    B3_tiff = os.path.splitext(B3_sgrd)[0] + '.tif'

    #  RGB composite from bands [7, 6, 3]
    tool_cmd = ' grid_visualisation 3 -R_GRID={0} -R_METHOD=4 -R_STDDEV=2.000000'\
                ' -G_GRID={1} -G_METHOD=4 -G_STDDEV=2.000000 -B_GRID={2} -B_METHOD=4 -B_STDDEV=2.000000 -A_GRID=NULL -RGB={3}'\
                .format(os.path.join(srcdir, Band_lookup['12']), os.path.join(srcdir, Band_lookup['8A']), os.path.join(srcdir, Band_lookup['5']), B3_sgrd)

    
    executeSAGACommand(tool_cmd, B3_sgrd, 'Create RGB composite image')
        
    tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=5 -TYPE=0 -SET_NODATA=1 -NODATA=2.000000'.format(B3_sgrd, B3_tiff)
        
    executeSAGACommand(tool_cmd, B3_tiff, 'Calculate NBR Image (tif)')
    
    
#  STEP 4,5  Create Normalized Burn Ration (NBR)
#            and Mid Infrared Burn index (MIRB) files
#       ==========================================================

    _logger.info('Calculating NBR')

    # NBR
    t_nbr = os.path.join(srcdir, '{0}_{1}_NBR.tif'.format(tileID, tileDate))
    sg_nbr = os.path.splitext(t_nbr)[0] + '.sgrd'

    B12_WGS = os.path.join(srcdir, Band_lookup['12'])
    B8A_WGS = os.path.join(srcdir, Band_lookup['8A'])

    tool_cmd = ' grid_calculus 1 -GRIDS={0};{1} -RESULT={2} -FORMULA=(g1-g2)/(g1+g2) -NAME=NBR -TYPE=7'.format(B12_WGS, B8A_WGS, sg_nbr)
    executeSAGACommand(tool_cmd, sg_nbr, 'Calculate NBR Image (sgrd)')
    
    #tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=7 -TYPE=0 -SET_NODATA=1 -NODATA=2.000000 -OPTIONS=COMPRESS=LZW'.format(sg_nbr, t_nbr)
    tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=5 -TYPE=0 -SET_NODATA=1 -NODATA=2.000000'.format(sg_nbr, t_nbr)
            
    executeSAGACommand(tool_cmd, t_nbr, 'Calculate NBR Image (tif)')


    _logger.info('Calculating MIBR')

    # MIRB
    t_mibr = os.path.join(srcdir, '{0}_{1}_MIBR.tif'.format(tileID, tileDate))
    sg_mibr = os.path.splitext(t_mibr)[0] + '.sgrd'

    B11_WGS = os.path.join(srcdir, Band_lookup['11'])

    tool_cmd = ' grid_calculus 1 -GRIDS={0};{1} -RESULT={2} -FORMULA=(10*g1)-(9.8*g2)+2 -NAME=MIBR -TYPE=7'.format(B12_WGS, B11_WGS, sg_mibr)
    executeSAGACommand(tool_cmd, sg_mibr, 'Calculate MIBR Image (sgrd)')


    #tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=7 -TYPE=0 -SET_NODATA=1 -NODATA=2.000000 -OPTIONS=COMPRESS=LZW'.format(sg_mibr, t_mibr)
    tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=5 -TYPE=0 -SET_NODATA=1 -NODATA=2.000000'.format(sg_mibr, t_mibr)
            
    executeSAGACommand(tool_cmd, t_mibr, 'Calculate MIBR Image (tif)')
    
    _logger.info('Done processing tile {0} acquired on {1}'.format(tileID, tileDate))
# ========================================   
    
                       
def processTiles(processdir, dirtiles):


    for inputdir in dirtiles:
        
        subdir = inputdir[len(tilesdir)+1:]
        outputdir = os.path.join(processdir, subdir)
        os.makedirs(name=outputdir, exist_ok=True)
        
        # list all file in input directory
        filelist = os.listdir(inputdir)
        
        # get the band images
        All_bands = [x for x in filelist if re.search(r'_B[A-Z0-9_]{2}\.jp2$', x, flags=RegexFlag.IGNORECASE)]
        
        # keep on the band declare in 'usefulBands' list
        pattern = r'([A-Z0-9_]{5})_(\d{8})_B([A-Z0-9_]{2})\.jp2$'
        
        # filter out the bands we do not want
        S2_bands = []
        for band in All_bands:
            re_compile = re.match(pattern, band, RegexFlag.IGNORECASE)
            
            if re_compile is not None:
                
                groups = re_compile.groups()
                if groups[2][0] == '0':
                    bn = groups[2][1:]
                else:
                    bn = groups[2]
                     
                if bn in usefulBands:
                    S2_bands.append(band)

        
        S2_bands.sort(key=natural_keys)
        
        for band in S2_bands:

            inputfile = band
            
            pattern = r'([A-Z0-9_]{5})_(\d{8})_B([A-Z0-9_]{2})\.jp2$'
            re_compile = re.match(pattern, inputfile, RegexFlag.IGNORECASE)
            
            if re_compile is not None:
                
                groups = re_compile.groups()
                
                if groups[2][0] == '0':
                    bnum = groups[2][1:]
                else:
                    bnum = groups[2]
                    
                    
#== Reproject selected bands and save as TIFF file    ===========================================================================
               
            f_source = os.path.join(inputdir, inputfile)
            outputfile = band[0:band.rfind(".")] + '_WGS.sgrd'
            f_target = os.path.join(outputdir, outputfile)
   
            _logger.info('Reprojecting Band %s: [%s] --> [%s]' % (bnum, inputfile, outputfile) )
   
            #=============================================================================== 
            if saga_verbose is False:
                #tool_cmd = ' -f=p{0} pj_proj4 4 -CRS_METHOD=1 -CRS_EPSG=4326 -SOURCE={1} -RESAMPLING=0 -TARGET_GRID={2}'.format('s', f_source, f_target)
                tool_cmd = ' -f=p{0} pj_proj4 4 -CRS_METHOD=1 -CRS_EPSG=4326 -SOURCE={1} -RESAMPLING=0 -GRID={2}'.format('s', f_source, f_target)
            else:
                #tool_cmd = ' -f=p pj_proj4 4 -CRS_METHOD=1 -CRS_EPSG=4326 -SOURCE={0} -RESAMPLING=0 -TARGET_GRID={1}'.format(f_source, f_target)
                tool_cmd = ' -f=p pj_proj4 4 -CRS_METHOD=1 -CRS_EPSG=4326 -SOURCE={0} -RESAMPLING=0 -GRID={1}'.format(f_source, f_target)
    
            executeSAGACommand(tool_cmd, f_target, 'Reprojecting Band {0} [sgrd]'.format(bnum))
    
            # Export reprojected band to tif file format
            f_tiff = os.path.splitext(f_target)[0] + '.tif'
    
            # tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=7 -TYPE=0 -SET_NODATA=0 -NODATA=255.000000 -OPTIONS=COMPRESS=LZW'.format(f_target, f_tiff)
            tool_cmd = ' io_gdal 1 -GRIDS={0} -FILE={1} -FORMAT=5 -TYPE=0 -SET_NODATA=0 -NODATA=255.000000'.format(f_target, f_tiff)
  
            executeSAGACommand(tool_cmd, f_tiff, 'Reproject Band {0} [tif]'.format(bnum))


        # add TCI and PVI to list  
        #TCI_PVI = [x for x in filelist if re.search(r'_[A-Z]{3}\.jp2$', x, flags=RegexFlag.IGNORECASE)]

            
        doSAGAProcessingSteps(outputdir)
        
        doCleanup(outputdir)
        #shutil.rmtree(inputdir)
        
# ========================================       
        
def downloadTiles(data):
    
    user = data['username']
    passwd = data['password']
    portal = data['portal_url']
    platform = data['platform']
    
    api = SentinelAPI(user, passwd, portal)  # CHANGE LOGIN DETAILS HERE
    
    tiles = data['tiles']
    outputdir = data['working_d']    # CHANGE DOWNLOAD LOCATION HERE
    
    #
    #query_kwargs = {
    #        'platformname': platform,
    #        'producttype': 'S2MSI1C',
    #        'date': ('NOW-4DAYS', 'NOW')}  # CHANGE DATE RANGE HERE
    #
    
    #'date': ('NOW-794DAYS', 'NOW-629DAYS'),
    
    query_kwargs = {
            'platformname': platform,
            'producttype': 'S2MSI1C',
            'date': ('NOW-4DAYS', 'NOW')}
    #        'cloudcoverpercentage': (0)}  
    
    products = OrderedDict()
    
    if not os.path.exists(outputdir):
        print('Download directory doesn\'t exist. Aborting data download.')
        exit(1)
    
    for tile in tiles:  # Iterate through the tiles list
        kw = query_kwargs.copy()
        kw['tileid'] = tile  # 'products' after 2017-03-31
        pp = api.query(**kw)
        products.update(pp)
    
    # all the requests have been queued in the OrderedDict 'products', 
    # start the bulk download of the tiles.
    
    print('Proceeding to download:')
    
    for uid in products:
        
        archive = products[uid]
        tileid = archive['tileid']
        acqdate = archive['datatakesensingstart'].strftime("%Y-%m-%d")
        
        _logger.info('- Tile %s acquired on %s' % (tileid, acqdate))
        
        
    if sw_downloads:
        api.download_all(products, tilesdir)
        
    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument('-c', '--config', nargs=1, metavar='filename', default='params.conf', help='Configuration file')
    parser.add_argument('-r', '--revision', help='Displays script version/revision', default=False, action='store_true')
    parser.add_argument('-v', '--validate', help='Validates configuration file parameters', default=False, action='store_true')
    
    args = parser.parse_args()
    
    print(' ')
    # ====================================================== args.revision ====
    # Print Script version/revision
    if args.revision:
        print('\n\tScript \'{0}\', version: {1}\n'.format(os.path.basename(__file__), Globals.VERSION))
        exit(0)
        
    # Init logging engine
    logname = Globals.LOGNAME
    engine = LogEngine()
    engine.initLogger(name=logname)
    
    # Get logger instance
    logger = engine.logger
    logger.info('\n')    

    # =======================================================  args.config ====
    
    # Verify the configuration file exists (default or from command line)
    if args.config.__class__.__name__ == 'list':
        fconfig = args.config[0]
    else:
        fconfig = args.config
    
    basename = os.path.basename(fconfig)
    
    if basename == fconfig:
        # try relative path
        if not os.path.isfile(os.path.join(os.getcwd(), fconfig)):
            logger.critical('Config file not found: %s', os.path.join(os.getcwd(), fconfig))
            exit(1)
    else:
        if not os.path.isfile(fconfig):
            logger.critical('Config file not found: %s', fconfig)
            exit(1)

    # ======================================================  Config file  =====
    # Read configuration file into a dictionary
    config = readConfig(fconfig)
    if config is None:
        exit(1)

    
    downloadTiles(config)
    
    #==============================================================================
    #=====================  Script main branch starts here  =======================
    #==============================================================================
    
    #=====================   Script input data ==================================== 
    
    tiles = ['52LBJ','52LBH','52LCH','52LCJ']   # ADD TILES AS A PYTHON LIST
    tilesdir = r'D:\_RSDATA_\Sentinel-2'        # CHANGE DOWNLOAD LOCATION HERE
    processdir = r'D:\_RSDATA_\Sentinel-2\Processing'  # CHANGE PROCESSING LOCATION HERE
    
    usefulBands = ['5', '8A', '11', '12' ]  # Band required by SAGA computations
    
    saga_cmd = r'C:\Program Files\SAGA-GIS\saga-7.0.0_x64\saga_cmd.exe'  # SAGA cmd full path
     
    saga_cores = 2  # Number of CPU cores allocated to SAGA cmd
    saga_verbose = False  # SAGA cmd verbose mode
    
    sw_downloads = True  # Enable/Disable tile downloads
    
    allowCleanup = True
    
    # initialize console logger
    _logger = initLogger(name='Sentinel Processing', level=logging.INFO)
    
    #==============================================================================
    
    
    api = SentinelAPI('aidanfnqspatial', 'FNQsp4tial')  # CHANGE LOGIN DETAILS HERE
    
    platform = 'Sentinel-2'
    
    #
    #query_kwargs = {
    #        'platformname': platform,
    #        'producttype': 'S2MSI1C',
    #        'date': ('NOW-4DAYS', 'NOW')}  # CHANGE DATE RANGE HERE
    #
    
    #'date': ('NOW-794DAYS', 'NOW-629DAYS'),
    
    query_kwargs = {
            'platformname': platform,
            'producttype': 'S2MSI1C',
            'date': ('NOW-4DAYS', 'NOW')}
    #        'cloudcoverpercentage': (0)}  
    
    products = OrderedDict()
    
    if not os.path.exists(tilesdir):
        print('Download directory doesn\'t exist. Aborting data download.')
        exit(1)
    
    for tile in tiles:  # Iterate through the tiles list
        kw = query_kwargs.copy()
        kw['tileid'] = tile  # 'products' after 2017-03-31
        pp = api.query(**kw)
        products.update(pp)
    
    # all the requests have been queued in the OrderedDict 'products', 
    # start the bulk download of the tiles.
    
    print('Proceeding to download:')
    
    for uid in products:
        
        archive = products[uid]
        tileid = archive['tileid']
        acqdate = archive['datatakesensingstart'].strftime("%Y-%m-%d")
        
        _logger.info('- Tile %s acquired on %s' % (tileid, acqdate))
        
        
    if sw_downloads:
        api.download_all(products, tilesdir)
     
    # all the tiles matching the search parameters are saved in the 
    # products data structure. 'products' is a dictionary of dictionaries.
    # See the function 'extractSentinel2Bands' for more details
     
     
    # Extract all band images from archive
     
    if platform == 'Sentinel-2':
        procQueue = extractSentinel2Bands(tilesdir, products)
    else:
        _logger.info('Platform not supported. Unzip archive manually.')
    
    #procQueue = []
    #procQueue.append(r'D:\_RSDATA_\Sentinel-2\54LYJ\20181208')
    
    if len(procQueue) > 0:
        processTiles(processdir, procQueue);
    
    
    
    exit(0)
