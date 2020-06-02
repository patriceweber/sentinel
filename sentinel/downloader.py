
import os
from queue import Queue

from sentinel.utils import LogEngine

from collections import OrderedDict
from sentinelsat import SentinelAPI


class sent2Downloader:

    def __init__(self, config=None):

        self.config_lk = config

        self.tasks = Queue()
        self.logger = LogEngine().logger

        return
    
    
    def getTasksQueue(self):
        return self.tasks    
    
    
    def startDownloads(self):
        
        if not os.path.exists(self.config_lk['downloads_d']):
            print('Download directory doesn\'t exist. Aborting data download.')
            exit(1)
        
        # log into Copernicus portal
        api = SentinelAPI(self.config_lk['username'], self.config_lk['password'], self.config_lk['portal_url'])
        
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
            pp = api.query(**kw)
            
            # Add the retrieved tile data to the product list
            products.update(pp)
        
        
        # all the requests have been queued in the OrderedDict 'products', 
        # start the bulk download of the tiles.
        
        print('Proceeding to download:')
        
        for uid in products:
            
            archive = products[uid]
            tileid = archive['tileid']
            acqdate = archive['datatakesensingstart'].strftime("%Y-%m-%d")
            api.download(uid, self.config_lk['downloads_d'])
            
            self.logger.info('- Tile %s acquired on %s has been downloaded' % (tileid, acqdate))
            self.tasks.put(archive)
            
        # finish downloaded Sentinel products
        end_marker = {'title':'XXXX'}
        self.tasks.put(end_marker)

