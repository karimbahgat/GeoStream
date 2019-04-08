
import numpy as np

from .load import file_reader

from wkb_raster import write_wkb_raster

class Band(object):
    def __init__(self, rast, data=None, dtype=None, width=None, height=None, initialvalue=0, nodataval=None):
        self.rast = rast
        # data is either None or np.array
        self._data = data
        self.dtype = dtype
        self.width = width
        self.height = height
        self.initialvalue = initialvalue
        self.nodataval = nodataval

    def __repr__(self):
        return "<Band object: dtype={dtype} size={size} nodataval={nodataval}>".format(dtype=self.dtype,
                                                                                       size=(self.width,self.height),
                                                                                       nodataval=self.nodataval)
    

    def data(self, bbox=None):        
        # if file source, use the reader to return the data, but do not store the data in memory
        if self.rast and self.rast.filepath:
            bandnum = self.rast.bands.index(self)
            data = self.rast.reader.data(bandnum, bbox)

        else:
            # create empty data if not exists
            data = self._data
            if data is None:
                data = np.full((self.width,self.height), initialvalue, dtype=dtype)
                self._data = data

            # crop to bbox
            if bbox:
                x1,y1,x2,y2 = bbox
                x2, y2 = min(x12, self.width), min(y2, self.height)
                w,h = x2-x1, y2-y1
                data = data[y1:y2, x1:x2]

        return data

    def crop(self, bbox):
        data = self.data(bbox)
        w, h = data.shape[1], data.shape[0]
        band = Band(None, data, data.dtype, w, h, self.nodataval)
        return band

    def wkb_dict(self):
        data = self.data() # force loading the data
        dtypes = ['bool', None, None, 'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'float32', 'float64']
        pixtype = dtypes.index(str(self.dtype))
                               
        dct = {'isOffline': self.rast and bool(self.rast.filepath),
               'hasNodataValue': self.nodataval is not None,
               'isNodataValue': np.all(self.data == self.nodataval),
               'ndarray': data,
               'pixtype': pixtype,
               }

        dct['nodata'] = self.nodataval if dct['hasNodataValue'] else 0
        
        if dct['isOffline']:
            bandnum = self.rast.bands.index(self)
            dct['bandNumber'] = bandnum
            dct['path'] = self.rast.filepath
            
        return dct

class Raster(object):
    def __init__(self, filepath=None, width=None, height=None, affine=None, **kwargs):
        self.filepath = filepath
        self.bands = []
        
        if self.filepath:
            self.reader = file_reader(filepath, **kwargs)
            width = self.reader.width
            height = self.reader.height
            affine = affine or self.reader.affine
            for i in range(self.reader.bandcount):
                nodataval = self.reader.nodata(i)
                self.add_band(nodataval=nodataval)
        else:
            self.reader = None
        
        self.width = width
        self.height = height
        self.affine = affine
        self.kwargs = kwargs

    def __repr__(self):
        return "<Raster data: dtype={dtype} bands={bands} size={size} bbox={bbox}>".format(dtype=None, #self.dtype,
                                                                                           bands=len(self.bands),
                                                                                           size=(self.width,self.height),
                                                                                           bbox=self.bbox)

    @property
    def bbox(self):
        xoff,xscale,xskew,yoff,yscale,yskew = self.affine
        x1,y1 = xoff,yoff
        x2,y2 = x1 + self.width * xscale, y1 + self.height * yscale
        return [x1,y1,x2,y2]

    def add_band(self, *args, **kwargs):
        if args and isinstance(args[0], Band):
            band = args[0]
        else:
            band = Band(self, *args, **kwargs)
        self.bands.append(band)

    def tiled(self, tilesize=None, tiles=None):
        # create iterable tiler class, to allow also checking length
        
        class Tiler:
            def __init__(self, rast, tilesize=None, tiles=None):
                self.rast = rast
                
                # determine tile sizes
                if not (tilesize or tiles):
                    tilesize = (200,200)
                if tiles:
                    xtiles,ytiles = list(map(float, (xtiles,ytiles)))
                    tilesize = int(self.rast.width/xtiles)+1, int(self.rast.height/ytiles)+1

                self.tilesize = tilesize
                
            def __iter__(self):
                tw,th = self.tilesize
                for y in range(0, self.rast.height, th):
                    for x in range(0, self.rast.width, tw):
                        tile = self.rast.crop([x, y, x+tw, y+th])
                        yield tile
                        
            def __len__(self):
                return self.tilenum()

            def tilenum(self):
                tw,th = list(map(float, self.tilesize))
                tiles = (int(self.rast.width/tw)+1, int(self.rast.height/th))
                tilenum = tiles[0] * tiles[1]
                return tilenum

        return Tiler(self, tilesize=tilesize, tiles=tiles)

    def crop(self, bbox):
        px1,py1,px2,py2 = bbox
        pw,ph = px2-px1, py2-py1

        xoff,xscale,xskew,yoff,yscale,yskew = list(self.affine)
        xoff += px1 * xscale
        yoff += py1 * yscale

        px2, py2 = min(px1+pw, self.width), min(py1+ph, self.height)
        pw, ph = px2-px1, py2-py1
        
        rast = Raster(None, pw, ph, [xoff,xscale,xskew,yoff,yscale,yskew])
        for band in self.bands:
            cropped_band = band.crop([px1, py1, px2, py2])
            rast.add_band(cropped_band)
        return rast 

    @property
    def wkb(self):
        band_dicts = [b.wkb_dict() for b in self.bands]
        wkb = write_wkb_raster(band_dicts,
                               self.width,
                               self.height,
                               self.affine)
        return wkb
