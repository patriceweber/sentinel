
import os, logging
from queue import Queue

from sentinel.utils import LogEngine

from collections import OrderedDict
from sentinelsat import SentinelAPI, SentinelAPIError

from requests.models import InvalidURL
from requests.adapters import ConnectionError
from requests.exceptions import Timeout


class sent2Downloader:

    def __init__(self, config=None):

        self.config_lk = config

        self.tasks = Queue()
        self.logger = LogEngine().logger

        return
    
    
    def getTasksQueue(self):
        return self.tasks    
    
    
    def startDownloads(self):
        
        try:
            self.logger.debug('Starting download thread')
            
            if not os.path.exists(self.config_lk['downloads_d']):
                print('Download directory doesn\'t exist. Aborting data download.')
                exit(1)
            
            # log into Copernicus portal
    
            SentinelAPI.logger.setLevel(logging.DEBUG)
            #logfile = logging.FileHandler(logfilename, 'a')
    
            self.logger.debug('Connecting to portal [%s] with credentials \'%s\'/\'%s\'',self.config_lk['portal_url'],\
                              self.config_lk['username'],self.config_lk['password'])
            
            api = SentinelAPI(self.config_lk['username'], self.config_lk['password'], self.config_lk['portal_url'],\
                              show_progressbars=self.config_lk['verbose'], timeout=10)
            
            products = OrderedDict()
            query_arguments = {
                'platformname': self.config_lk['platform'],
                'producttype': 'S2MSI1C',
                'date': (self.config_lk['startdate'], self.config_lk['enddate'])}
                
            # Iterate through the tiles list and add the tile entry
            for tile in self.config_lk['tiles']:
                
                kw = query_arguments.copy()
                kw['tileid'] = tile
                
                # Submit the query to the Copernicus hub
                self.logger.debug('Submitting query for tile \'%s\'', tile)
                pp = api.query(**kw)
                
                # Add the retrieved tile data to the product list
                products.update(pp)
                self.logger.debug('Added tile \'%s\' metadata to product list', tile)
            
            
            # all the requests have been queued in the OrderedDict 'products', 
            # start the bulk download of the tiles.
            
            for uid in products:
                
                archive = products[uid]
                tileid = archive['tileid']
                acqdate = archive['datatakesensingstart'].strftime("%Y-%m-%d")
                
                self.logger.info('+ Tile %s acquired on %s : download started' % (tileid, acqdate))
                api.download(uid, self.config_lk['downloads_d'])
                
                self.logger.info('- Tile %s acquired on %s has been downloaded' % (tileid, acqdate))
                self.tasks.put(archive)


        except Timeout as error:
            self.logger.critical('Connection to Copernicus portal timed out: %s', repr(error))

        except InvalidURL as error:
            self.logger.critical('Error connecting to Copernicus portal: %s', repr(error))

        except SentinelAPIError as error:
            self.logger.critical('Error authenticating to Copernicus portal: %s', repr(error))
            
        except ConnectionError as error:
            self.logger.critical('Error connecting to Copernicus portal: %s', repr(error))
            
        finally:
            # finished download of sentinel products
            self.logger.info('No more tiles to download, exiting [\'sent2Downloader\'] ')
            end_marker = {'title':'XXXX'}
            self.tasks.put(end_marker)
            
