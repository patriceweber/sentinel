
from re import RegexFlag
import os, re, zipfile

from PIL import Image

from sentinel.utils import LogEngine


class workflowSentinel2:


    def __init__(self, config_lk):

        # Initialize logger
        self.logger = LogEngine().logger

        self.config = config_lk
        self.tile = None

        return


    def processTile(self, tile):
        """ This function is responsible for the initialization, the execution 
        of the main workflow and clean up tasks. """

        if tile is None:
            raise ValueError('Sentinel-2 tile is not valid')
        
        title = tile['title']
        
        self.tile = tile
        
        try:
            date = tile['ingestiondate']
            self.logger.debug('Processing tile \'%s\' acquired on %s', tile['title'], date)
        except KeyError as error:
            self.logger.debug('Processing tile \'%s\'', tile['title'])
            
        self.extractSentinel2Bands()

        return     


    # ========================================

    def extractSentinel2Bands(self):

        tiles_d = self.config['tiles_d']
        downloads_d = self.config['downloads_d']

        zipname = self.tile['title'] + '.zip'
        fzip = os.path.join(downloads_d, zipname)
        self.logger.info('Extracting bands from archive: %s' % zipname)

        if os.path.exists(fzip) and zipfile.is_zipfile(fzip):

            # create output folder
            pattern = r'^([A-Z0-9]{3})_([A-Z0-9_]{6,8})_([A-Z0-9]{15})_N(\d{4})_R(\d{3})_T([A-Za-z0-9]{5})_(\d{8})T(\d{6})'
            re_compile = re.match(pattern, self.tile['title'], RegexFlag.IGNORECASE)

            if re_compile is not None:

                tilegroups = re_compile.groups()
                # Groups index start at zero. Get tile number (group 5) and date (group 6) and time (group 7)
                tempdir = os.path.join(tiles_d, tilegroups[5], tilegroups[6])
                outputdir = os.path.join(tiles_d, tempdir, "T" + tilegroups[7])

            else:
                # regular expression match failed, used product default name
                outputdir = os.path.join(tiles_d, self.tile['title'])

            os.makedirs(name=outputdir, exist_ok=True)

            # -- Process archive. Open zip file in read mode
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

                        if not (groups[3] in self.config['bands']):
                            continue

                        if groups[3] == 'PVI':
                            imgName = groups[0] + '_' + groups[1] + '_T' + tilegroups[7] + '.jp2'
                        else:
                            imgName = groups[0] + '_' + groups[1] + '_T' + tilegroups[7] + '_' + groups[3] + '.jp2'
                        
                        # set new image filename (full path)
                        outputfilename = os.path.join(outputdir, imgName)
                        outputmetadata = os.path.join(outputdir, 'metadata.txt')

                        # read image file from archive into byte array
                        data = zf.read(fband)

                        # save byte array to file
                        with open(outputfilename, 'wb') as output:
                            output.write(data)

                        # save tile metadata in image folder
                        with open(outputmetadata, 'w') as metadata:
                            for key in self.tile.keys():
                                metadata.write('%s : %s\n' % (key, self.tile[key]))

                        #print('Output image: %s' % imgName)
                        self.logger.debug('Extracting image file: [%s]' % (imgName) )


                        if groups[3] == 'PVI':

                            img = Image.open(outputfilename)
                            
                            # form the png image filename
                            fname = os.path.basename(outputfilename)
                            filename, extension = os.path.splitext(fname);

                            thumbsdir = os.path.join(self.config['thumbs_d'], groups[0])
                            if not os.path.exists(thumbsdir):
                                os.makedirs(name=thumbsdir)
                                
                            thumbnail = os.path.join(thumbsdir,fname)
                            img.save(thumbnail + ".png")
                            
                            # delete PVI.jp2 image
                            os.remove(outputfilename)


                except KeyError:
                    self.logger.info('Could not find %s in zip file' % fband)


            zf.close()

        return
    # ========================================    