
import os, re
from re import RegexFlag
import logging, argparse

from threading import Thread
from time import sleep

from sentinel.downloader import sent2Downloader
from sentinel.workflow import workflowSentinel2

from sentinel.utils import Globals
from sentinel.utils import LogEngine

from sentinel.utils import readConfig
from sentinel.utils import sanityCheck
from sentinel.utils import displayRunConfiguration


_logger = None


def processProduct(config_lk, queue):
    """ Workflow processing main loop
    """

    try:
        
        wf = workflowSentinel2(config_lk)
        
        while True:

            tile = queue.get()

            if tile is not None:
                # Test if we ran into end of scenes marker sensor='XXX'
                if tile['title'] == 'XXXX':
                    return
                else:
                    try:
                        wf.processTile(tile)
                    except ValueError as error:
                        _logger.info('Error processing tile : %s', repr(error))
                    except KeyError as error:
                        _logger.info('Error processing tile : %s', repr(error))
                        

                    queue.task_done()

            _logger.debug('Listening for new scene to process...')

    except NameError as error:
        _logger.critical('Error workflow class name: %s', repr(error))

    except TypeError as error:
        _logger.critical('Error creating workflow: %s', repr(error))        

    return
# ========================================


def natural_keys(filename):
    """ Natural sort order helper function """

    def atoi(stext):
        return int(stext) if stext.isdigit() else stext

    return [atoi(c) for c in re.split(r'(\d+)', filename)]
# ========================================


def doCleanup(allowCleanup, base_dir, exclusions=[]):

    
    if allowCleanup:

        # Exclude file types, we want to keep from deletion
        extensions = ['sgrd', 'xml', 'mgrd', 'sdat', 'prj', 'pgw', 'tgz']

        for ext in exclusions:
            [extensions.remove(x) for x in extensions if x == ext]

        for root, dirs, files in os.walk(base_dir):

            for ext in extensions:
                pattern = '.{0}$'.format(ext)
                [os.remove(os.path.join(root, x)) for x in files if re.search(pattern, x, flags=RegexFlag.IGNORECASE)]

            [os.remove(os.path.join(root, x)) for x in files if re.search(r'_B(\d+)\.TIF$', x, flags=RegexFlag.IGNORECASE)]


        _logger.info('Clean up done.')
# ========================================            



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
    engine = LogEngine()
    engine.initLogger(name=Globals.LOGNAME, level=logging.DEBUG) 
    
    # Get the initialized logger instance 
    _logger = engine.logger

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
            _logger.critical('Config file not found: %s', os.path.join(os.getcwd(), fconfig))
            exit(1)
    else:
        if not os.path.isfile(fconfig):
            _logger.critical('Config file not found: %s', fconfig)
            exit(1)

    # ======================================================  Config file  =====
    # Read configuration file into a dictionary
    config = readConfig(fconfig)
    if config is None:
        exit(1)
    
    # Finalize initialization
    basedir = config['base_d']
    config['downloads_d'] = os.path.join(basedir,'downloads')
    config['tiles_d'] = os.path.join(basedir,'tiles')
    
    if not os.path.exists(basedir):
        os.makedirs(name=basedir)
        
    if not os.path.exists(config['downloads_d']):
        os.makedirs(name=config['downloads_d'])
        
    if not os.path.exists(config['tiles_d']):
        os.makedirs(name=config['tiles_d'])

    # ======================================================= args.validate ====
    # Do we need to validate the config file?
    if args.validate is True:
    
        if config['verbose']:
            _logger.setLevel(logging.DEBUG)
        else:
            _logger.setLevel(logging.INFO)
    
        _status = sanityCheck(config)
        displayRunConfiguration(config, _status)

        exit(0)

    # ==========================================================================
    
    log_basename = 'sentinel_downloads'
    
    if config['verbose']:
        _logger.setLevel(logging.DEBUG)
    else:
        _logger.setLevel(logging.INFO)                
    
    engine.setLogsRootdir(basedir)
    engine.addFilelogHandler(basename=log_basename, rotations=config['rotations'], timestamp=config['timestamp'], identifier=config['identifier'])  
    
    if config['platform'] == 'Sentinel-2':
        
        downloader = sent2Downloader(config)
        producer = Thread(target=downloader.startDownloads, name='Downloader')
        producer.start()
        sleep(2)

        q_fifo = downloader.getTasksQueue()
        processProduct(config, q_fifo)
        
    else:
        _logger.info('Platform not supported. Unzip archive manually.')
        
    
    _logger.info('Done processing tile list ...')    
    
    exit(0)
