
import os, sys, string
import ctypes, platform, itertools

import configparser
from configparser import NoOptionError

import time, datetime, logging

import re
from re import RegexFlag

import tarfile
from tabulate import tabulate


class Globals():
    """ Class holding global constants used and shared throughout the scripts
    """

    LOGNAME = r'SENTINEL_DOWNLOADS'
    VERSION = r'1.0.1'

    # The '~' means home directory. In the case of Windows operating systems
    # it will be C:\Users\USER_HOMEDIR

    METADATA_LC8_BASEDIR = r'~\Documents\nafi\metadata\LC8'
    METADATA_LC8_LOG_BASEDIR = r'~\Documents\nafi\metadata\LC8\logs'

    DOWNLOADER_BASEDIR = r'~\Documents\nafi\downloader'

    WORKFLOWS_BASEDIR = r'~\Documents\nafi\workflows'
    WORKFLOWS_LOG_BASEDIR = r'~\Documents\nafi\workflows\logs'

    # Enable 'benchmark' decorator function
    ALLOW_BENCHMARK = True

    # Constants
    MBYTES = 1024 * 1024


#===============================================================================
# Initialized the script logging engine with two logging handlers: stdout and
# logfile. The script keeps 10 consecutive log files. When the maximum number
# is reached (as defined by 'MAX_ROTATIONS'), the old log files are archived in
# a timestamped tar.gz file.
#===============================================================================

class LogEngine:
    """ Wrapper class for standard output and file logging. The class implements the singleton pattern
        with the inner class '__logger'
    """
    
    class __logger:

        def __init__(self):

            self.loggername = None
            self.level = None
            self.rootdir = ''
            self.max_rotations = 0

            return

        def initLogger(self, name='', location='', level=logging.DEBUG):

            # Init member variables
            self.loggername = name
            self.rootdir = location
            self.level = level

            # Init logging engine
            _log = logging.getLogger(self.loggername)
            _log.setLevel(self.level)
            logformat = logging.Formatter('%(asctime)s: %(threadName)s: [%(levelname)s]: %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

            # Add console ouput handler
            console = logging.StreamHandler(sys.stdout)
            console.setFormatter(logformat)
            _log.addHandler(console)

            # register logging instance as an inner class attribute
            self.__dict__['logger'] = _log

            _log.info('Logger [%s] initialized', self.loggername)

            return

        def setLogsRootdir(self, location):
            """ Set the location of the logs folder
            """            
            self.rootdir = location
            return
            
        def getLogsRootdir(self):
            """ Return the location of the logs folder
            """             
            return self.rootdir

        def setLogLevel(self, level):
            """ Set the logging level (DEBUG, INFO, WARNING etc..)
            """

            self.level = level
            self.__dict__['logger'].setLevel(self.level)

            return

        def getLogLevel(self):
            """ get the current logging level (DEBUG, INFO, WARNING etc..)
            """
            return self.level
        
        def getLevelName(self, level):
            """ Set the logging level (DEBUG, INFO, WARNING etc..)
            """
            _log.getLevelName(level)

            return
        
        def addFilelogHandler(self, basename='L8_Script', timestamp=False, rotations=0, identifier=''):

            # Create the directory where log files are saved
            if not self.rootdir:
                logpath = os.path.join(os.getcwd(), 'logs')
            else:
                # Put all the logfiles into a 'rootdir' directory
                if self.rootdir == '~':
                    logpath = os.path.expanduser(self.rootdir)
                else:
                    logpath = os.path.join(self.rootdir, 'logs')
         
            
            self.rootdir = logpath
            if os.path.exists(logpath) is False:
                os.makedirs(logpath)

            # Set rotating logfile max index
            self.max_rotations = rotations

            # Calculate increment
            index = self.indexIncrement(basename)

            # Add logfile handler
            if  self.max_rotations > 0:

                if timestamp is True:
                    today = datetime.datetime.now().strftime('%Y%m%d-%H%M')
                    if not identifier:
                        logfilename = os.path.join(logpath, '{0}_{1}_{2}.log'.format(basename, today, index))
                    else:
                        logfilename = os.path.join(logpath, '{0}_{1}_{2}_{3}.log'.format(basename, identifier, today, index))
                else:
                    if not identifier:
                        logfilename = os.path.join(logpath, '{0}_{1}.log'.format(basename, index))
                    else:
                        logfilename = os.path.join(logpath, '{0}_{1}_{2}.log'.format(basename, identifier, index))

            else:
                if timestamp is True:
                    today = datetime.datetime.now().strftime('%Y%m%d-%H%M')
                    if not identifier:
                        logfilename = os.path.join(logpath, '{0}_{1}.log'.format(basename, today))
                    else:
                        logfilename = os.path.join(logpath, '{0}_{1}_{2}.log'.format(basename, identifier, today))
                else:
                    if not identifier:
                        logfilename = os.path.join(logpath, '{0}.log'.format(basename))
                    else:
                        logfilename = os.path.join(logpath, '{0}_{1}.log'.format(basename, identifier))


            logfile = logging.FileHandler(logfilename, 'a')
            logformat = logging.Formatter('%(asctime)s: %(threadName)s: [%(levelname)s]: %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
            logfile.setFormatter(logformat)

            _log = logging.getLogger(self.loggername)
            _log.addHandler(logfile)

            return

        def indexIncrement(self, basename):

            # Create incremental log filename, MAX_ROTATIONS files
            index = 1
            pattern = r'^{0}_(.+)_([0-9]+)\.log'.format(basename)
            filelogs = [f for f in os.listdir(self.rootdir) if re.match(pattern, f, RegexFlag.IGNORECASE)]
            fnumbers = [re.match(pattern, f, RegexFlag.IGNORECASE).group(2) for f in filelogs]

            try:
                if len(fnumbers) > 0:
                    filelogs.sort(key=natural_keys)
                    fnumbers.sort(key=natural_keys, reverse=True)
                    index = (1 + int(fnumbers[0])) % (1 + self.max_rotations)

                    if index == 0:  # Let start over

                        # Compress old log files before deleted them
                        today = datetime.datetime.now().strftime('%Y%m%d')
                        archive = os.path.join(self.rootdir, '{0}_logs_{1}.tar.gz'.format(self.loggername, today))

                        with tarfile.open(archive, 'w:gz') as tarlogs:
                            for flog in filelogs:
                                tarlogs.add(os.path.join(self.rootdir, flog), flog)

                        # Delete log files
                        [os.remove(os.path.join(self.rootdir, x)) for x in filelogs]
                        index += 1  # no zero index 'cy_process0.log'

            except ValueError as error:
                print('Error in initLogger(): %s', error.args)
                exit(1)

            return index
        
        
        def repr(self):

            name = _log.loggername
            level = _log.getLevelName(self.level)
            return 'Logger instance {0}, log level {1} in folder {2}'.format(name, level, _log.rootdir)


    # storage for the instance reference
    __instance = None

    def __init__(self):
        """ Create __logger singleton instance """
        if LogEngine.__instance is None:
            LogEngine.__instance = LogEngine.__logger()

    def __setattr__(self, attr, value):
        """ Delegate attribute access to __logger implementation """
        return setattr(self.__instance, attr, value)

    def __getattr__(self, attr):
        """ Delegate attribute access to __logger implementation """
        return getattr(self.__instance, attr)


#################################################################################
#################################################################################

def natural_keys(filename):
    """ Natural sort order helper function """

    def atoi(stext):
        return int(stext) if stext.isdigit() else stext

    return [atoi(c) for c in re.split(r'(\d+)', filename)]


#===============================================================================
#  Read and parse the configuration file. As of now, the file
#  name is hard coded to 'params.conf'. It should be moved to
#  a command line option a la -conf=file_absolute_path
#===============================================================================

def readConfig(f_config):
    """ Configuration helper function. The configuration file key/value
        pairs are read and stored in the 'config' dictionary """

    _key = ''
    logger = LogEngine().logger

    try:
        _config = configparser.SafeConfigParser()
        f_conf = os.path.join(os.getcwd(), f_config)
        _config.read_file(open(f_conf))

        config_lk = {}

        # Load [COPERNICUS] section parameters
        _key = '[COPERNICUS]: platform'
        config_lk['platform'] = _config.get('COPERNICUS', 'platform')
    
        _key = '[COPERNICUS]: username'
        config_lk['username'] = _config.get('COPERNICUS', 'username')

        _key = '[COPERNICUS]: password'
        config_lk['password'] = _config.get('COPERNICUS', 'password')

        _key = '[COPERNICUS]: portal_url'
        config_lk['portal_url'] = _config.get('COPERNICUS', 'portal_url')

        _key = '[COPERNICUS]: login_timer'
        config_lk['login_timer'] = 60 * _config.getint('COPERNICUS', 'login_timer')

        _key = '[COPERNICUS]: tiles'
        strtiles = _config.get('COPERNICUS', 'tiles')
        config_lk['tiles'] = strtiles.split(",")
        
        _key = '[COPERNICUS]: startdate'
        config_lk['startdate'] = _config.get('COPERNICUS', 'startdate')              
        
        _key = '[COPERNICUS]: enddate'
        config_lk['enddate'] = _config.get('COPERNICUS', 'enddate')    
        
        config_lk['cc_land'] = _config.get('COPERNICUS', 'cc_land')
        if not config_lk['cc_land']:
            config_lk['cc_land'] = 100.0
        else:
            config_lk['cc_land'] = float(config_lk['cc_land'])

        _key = '[COPERNICUS]: bands'
        strbands = _config.get('COPERNICUS', 'bands')
        strbands = strbands + ',PVI'
        config_lk['bands'] = strbands.split(",")


        # Load [ENV] section parameters
        _key = '[ENV]: base_d'
        config_lk['base_d'] = _config.get('ENV', 'base_d')

        _key = '[ENV]: verbose'
        config_lk['verbose'] = _config.getboolean('ENV', 'verbose')

        _key = '[ENV]: online'
        config_lk['online'] = _config.getboolean('ENV', 'online')

        _key = '[ENV]: cleanup'
        config_lk['cleanup'] = _config.getboolean('ENV', 'cleanup')

        _key = '[ENV]: cleanup-exclude'
        exclusions = _config.get('ENV', 'cleanup-exclude').replace(' ', '')
        if not exclusions:
            config_lk['cleanup-exclude'] = []
        else:
            config_lk['cleanup-exclude'] = exclusions.split(',')


        # Load [LOGGER] section parameters
        _key = '[LOGGER]: timestamp'
        config_lk['timestamp'] = _config.getboolean('LOGGER', 'timestamp')

        _key = '[LOGGER]: rotations'
        if not _config.get('LOGGER', 'rotations'):
            config_lk['rotations'] = 0
        else:
            config_lk['rotations'] = _config.getint('LOGGER', 'rotations')

        _key = '[LOGGER]: identifier'
        config_lk['identifier'] = _config.get('LOGGER', 'identifier')


        if config_lk['verbose']:
            logger = logging.getLogger(Globals.LOGNAME)
            logger.info('Configuration parsed correctly.')


    except  IOError as error:
        config_lk = None
        logger.critical('Error reading config file --> %s', error.args)

    except  NoOptionError as error:
        config_lk = None
        logger.critical('Error reading config file --> %s', error.args)

    except ValueError as error:
        config_lk = None
        logger.critical('Error reading config file: %s --> %s', _key, error.args)

    return config_lk


#===============================================================================
# Display the run configuration, parsed from the configuration file.
# This function is called when the _DEBUG_ switch is set to 'True'
# or with the script '-h' option
#===============================================================================

def displayRunConfiguration(config_lk, _status=None):
    """ Summarize the configuration environment and highlight invalid
        settings after a call to the 'sanityChecks' helper function """

    if _status is None:
        _status = dict()

    data_matrix = []

    print('\n\n')
    try:
        # [COPERNICUS]
        trow = []
        trow.append('[COPERNICUS]')
        trow.append('              ')
        data_matrix.append(trow)

        # username
        trow = []
        trow.append('COPERNICUS Username')
        trow.append(config_lk['username'])
        if 'username' in _status: trow.append(_status['username'])
        data_matrix.append(trow)

        # password
        trow = []
        trow.append('COPERNICUS Password')
        trow.append(config_lk['password'])
        if 'password' in _status: trow.append(_status['password'])
        data_matrix.append(trow)

        # portal_url
        trow = []
        trow.append('COPERNICUS Portal')
        trow.append(config_lk['portal_url'])
        if 'portal_url' in _status: trow.append(_status['portal_url'])
        data_matrix.append(trow)

        # tiles
        trow = []
        trow.append('COPERNICUS tiles')
        trow.append(config_lk['tiles'])
        if 'tiles' in _status: trow.append(_status['tiles'])
        data_matrix.append(trow)
        
        # startdate
        trow = []
        trow.append('COPERNICUS startdate')
        trow.append(config_lk['startdate'])
        if 'startdate' in _status: trow.append(_status['startdate'])
        data_matrix.append(trow)
        
        # enddate
        trow = []
        trow.append('COPERNICUS enddate')
        trow.append(config_lk['enddate'])
        if 'enddate' in _status: trow.append(_status['enddate'])
        data_matrix.append(trow)
        
        # cc_land
        trow = []
        trow.append('COPERNICUS cc_land')
        trow.append(config_lk['cc_land'])
        if 'cc_land' in _status: trow.append(_status['cc_land'])
        data_matrix.append(trow)
        
        # bands
        trow = []
        trow.append('COPERNICUS bands')
        trow.append(config_lk['bands'])
        if 'bands' in _status: trow.append(_status['bands'])
        data_matrix.append(trow)

        # [ENV]
        trow = []
        trow.append('[ENV]')
        trow.append('              ')
        data_matrix.append(trow)

        # base_d
        trow = []
        trow.append('Script Working directory')
        trow.append(config_lk['base_d'])
        if 'base_d' in _status: trow.append(_status['base_d'])
        data_matrix.append(trow)

        # verbose
        trow = []
        trow.append('Verbose mode (on/off)')
        trow.append(config_lk['verbose'])
        data_matrix.append(trow)

        # online
        trow = []
        trow.append('Online mode (on/off)')
        trow.append(config_lk['online'])
        data_matrix.append(trow)

        # cleanup
        trow = []
        trow.append('Delete intermediate files (on/off)')
        trow.append(config_lk['cleanup'])
        data_matrix.append(trow)

        # cleanup-exclude
        trow = []
        trow.append('Exclude from deletion')
        if len(config_lk['cleanup-exclude']) == 0:
            trow.append('None')
        else:
            trow.append(config_lk['cleanup-exclude'])
        data_matrix.append(trow)


        # [LOGGER]
        trow = []
        trow.append('[LOGGER]')
        trow.append('              ')
        data_matrix.append(trow)

        # timestamp
        trow = []
        trow.append('Add timestamp to logfile name')
        trow.append(config_lk['timestamp'])
        data_matrix.append(trow)

        # rotations
        trow = []
        trow.append('Maximum number of logfiles before rotation')
        trow.append(config_lk['rotations'])
        data_matrix.append(trow)

        # identifier
        trow = []
        trow.append('Indentifier string')
        trow.append(config_lk['identifier'])
        data_matrix.append(trow)

        print(tabulate(data_matrix, headers=['   Parameter   ', '   Value   ', 'Status'], tablefmt='grid'))
        print(' ')

    except ValueError as error:
        logger = LogEngine().logger
        logger.warning('Error creating file naming convention table: %s', error.args)

    return

def importClassByName(name):
    """ Dynamic module and class loader. Used to instanciate
        the workflow class defined in the configuration file
        or by the command line option (-wf)"""

    logger = LogEngine().logger

    try:
        components = name.split('.')
        mod = __import__(components[0])

        for comp in components[1:]:
            mod = getattr(mod, comp)

    except AttributeError as error:
        logger.critical('Error loading class %s: ', name, repr(error))
        mod = None

    except ModuleNotFoundError as error:
        logger.critical('Error loading class %s: ', name, repr(error))
        mod = None

    return mod

#===============================================================================
# Just to make sure that most configuration parameters are OK
# and the script is good to go. This function could be improved
# with more checks.
#===============================================================================

def sanityCheck(config_lk):
    """ Assert the validity and format of most of the parameters
        defined in the configuration file."""

    _status = dict()

#   if on Windows OS, check if working directory drive letter exists
    if 'Windows' in platform.system():
        drive_bitmask = ctypes.cdll.kernel32.GetLogicalDrives()
        drives = list(itertools.compress(string.ascii_uppercase, [ord(x) - ord('0') for x in bin(drive_bitmask)[:1:-1]]))

        _base_dir = config_lk['base_d']

        if _base_dir.split(':')[0] in drives:

            # if working directory doesn't exist, create it
            if os.path.isdir(_base_dir) is False:
                os.makedirs(_base_dir)

        else:
            _status['base_d'] = 'Drive does not exist'


    logger = LogEngine().logger
    logger.debug('Sanity checks done.')

    return _status


def benchmark(func):
    """ Decorator function which can be toggled [on/off] with the switch Globals.ALLOW_BENCHMARK
        The function reports workflow processing steps infomation like:

            - Benchmarked function name, processing step UID, processing step name and
              processing step running time
    """

    if not Globals.ALLOW_BENCHMARK:
        return func

    def report(*args):

        summary = ''
        result = None

        st = time.time()

        if func.__name__ == 'executeSAGATool':

            # Get object reference and important values before command execution
            self = args[0]
            description = args[3]
            puid = self.p_uid

            result = func(*args)
            elapsed = time.time() -st

            # Fill in report
            summary = 'Function name: {0}, '.format(func.__name__)
            summary += 'Process uid: \'{0}, Process name: \'{1}\', '.format(puid, description)
            summary += ('Running time: %.1fs' % elapsed)

        elif func.__name__ == 'createExtractedBandList':

            # Get object reference and important values before command execution
            self = args[0]
            puid = self.p_uid

            result = func(*args)
            elapsed = time.time() -st

            # Fill in report
            summary = 'Function name: {0}, '.format(func.__name__)
            summary += 'Process uid: \'{0}, '.format(puid)
            summary += ('Running time: %.1fs' % elapsed)

        else:

            # Get object reference and important values before command execution
            self = args[0]
            result = func(*args)
            elapsed = time.time() -st

            # Fill in report
            summary = 'Function name: {0}, '.format(func.__name__)
            summary += ('Running time: %.1fs' % elapsed)


        logger = LogEngine().logger
        logger.debug(summary)
        logger.debug(' ')

        return result

    return report
