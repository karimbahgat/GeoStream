
from .layers import LayerGroup


class Map:
    def __init__(self, width=None, height=None, background=None, layers=None, title="", titleoptions=None, *args, **kwargs):

        # remember and be remembered by the layergroup
        if not layers:
            layers = LayerGroup()
        self.layers = layers
        layers.connected_maps.append(self)

        # background decorations
        self.backgroundgroup = BackgroundLayerGroup()
        if background:
            obj = Background(self)
            self.backgroundgroup.add_layer(obj)
        self.background = rgb(background)

        # create the drawer with a default unprojected lat-long coordinate system
        # setting width and height locks the ratio, otherwise map size will adjust to the coordspace
        self.width = width or None
        self.height = height or None
        self.drawer = None

        # foreground layergroup for non-map decorations
        self.foregroundgroup = ForegroundLayerGroup()

        # title (these properties affect the actual rendered title after init)
        self.title = title
        self.titleoptions = dict(textsize="6%w")
        if titleoptions: self.titleoptions.update(titleoptions)
        self.foregroundgroup.add_layer(Title(self))

        self.dimensions = dict()
            
        self.img = None
        self.changed = True

    def _create_drawer(self):
        # get coordspace bbox aspect ratio of all layers
        autosize = not self.width or not self.height
        if self.width and self.height:
            pass
        elif self.layers.is_empty():
            self.height = 500 # default min height
            self.width = 1000 # default min width
        else:
            bbox = self.layers.bbox
            w,h = abs(bbox[0]-bbox[2]), abs(bbox[1]-bbox[3])
            aspect = w/float(h)
            if not self.width and not self.height:
                # largest side gets set to default minimum requirement
                if aspect < 1:
                    self.height = 500 # default min height
                else:
                    self.width = 1000 # default min width
                
            if self.width:
                self.height = int(self.width / float(aspect))
            elif self.height:
                self.width = int(self.height * aspect)
            
        # factor in zooms (zoombbx should somehow be crop, so alters overall img dims...)
        self.drawer = pyagg.Canvas(self.width, self.height, None)
        self.drawer.geographic_space()

    def copy(self):
        dupl = Map(self.width, self.height, background=self.background, layers=self.layers.copy())
        dupl.backgroundgroup = self.backgroundgroup.copy()
        if self.drawer: dupl.drawer = self.drawer.copy()
        dupl.foregroundgroup = self.foregroundgroup.copy()
        return dupl

    def pixel2coord(self, x, y):
        if not self.drawer: self._create_drawer() 
        return self.drawer.pixel2coord(x, y)

    # Map canvas alterations

    def offset(self, xmove, ymove):
        if not self.drawer: self._create_drawer()
        self.drawer.move(xmove, ymove)
        self.changed = True
        self.img = self.drawer.get_image()

    def resize(self, width, height):
        self.width = width
        self.height = height
        if not self.drawer: self._create_drawer()
        self.changed = True
        self.drawer.resize(width, height, lock_ratio=True)
        self.img = self.drawer.get_image()

    def crop(self, xmin, ymin, xmax, ymax):
        if not self.drawer: self._create_drawer()
        self.changed = True
        self.drawer.crop(xmin,ymin,xmax,ymax)
        self.width = self.drawer.width
        self.height = self.drawer.height
        self.img = self.drawer.get_image()

    # Zooming

    def zoom_auto(self):
        if not self.drawer: self._create_drawer()
        bbox = self.layers.bbox
        self.zoom_bbox(*bbox)
        self.changed = True
        self.img = self.drawer.get_image()

    def zoom_bbox(self, xmin, ymin, xmax, ymax):
        if not self.drawer: self._create_drawer()
        if self.width and self.height:
            # predetermined map size will honor the aspect ratio
            self.drawer.zoom_bbox(xmin, ymin, xmax, ymax, lock_ratio=True)
        else:
            # otherwise snap zoom to edges so can determine map size from coordspace
            self.drawer.zoom_bbox(xmin, ymin, xmax, ymax, lock_ratio=False)
        self.changed = True
        self.img = self.drawer.get_image()

    def zoom_in(self, factor, center=None):
        if not self.drawer: self._create_drawer()
        self.drawer.zoom_in(factor, center=center)
        self.changed = True
        self.img = self.drawer.get_image()

    def zoom_out(self, factor, center=None):
        if not self.drawer: self._create_drawer()
        self.drawer.zoom_out(factor, center=center)
        self.changed = True
        self.img = self.drawer.get_image()

    def zoom_units(self, units, center=None, geodetic=False):
        if not self.drawer: self._create_drawer()
        if geodetic:
            from .vector._helpers import _vincenty_distance
            desired_km = units
            unit_width = self.drawer.coordspace_width
            x1,y1,x2,y2 = self.drawer.coordspace_bbox
            cur_km = _vincenty_distance((y1,x1), (y1,x1+unit_width))
            ratio = desired_km/cur_km
            if ratio < 1: ratio = -1/ratio
            ratio = -ratio
        self.drawer.zoom_factor(ratio)
        self.changed = True
        self.img = self.drawer.get_image()

    # Layers

    def __iter__(self):
        for layer in self.layers:
            yield layer

    def add_layer(self, layer, **options):
        return self.layers.add_layer(layer, **options)

    def move_layer(self, from_pos, to_pos):
        self.layers.move_layer(from_pos, to_pos)

    def remove_layer(self, position):
        self.layers.remove_layer(position)

    def get_position(self, layer):
        return self.layers.get_position(layer)
        
##    def add_decoration(self, funcname, *args, **kwargs):
##        # draws directly on an image the size of the map canvas, so no pasteoptions needed
##        self.changed = True
##        decor = Decoration(self, funcname, *args, **kwargs)
##        decor.pasteoptions = dict() #xy=(0,0), anchor="nw")
##        self.foreground.add_layer(decor)

##    def add_grid(self, xinterval, yinterval, **kwargs):
##        self.drawer.draw_grid(xinterval, yinterval, **kwargs)
##
##    def add_axis(self, axis, minval, maxval, intercept,
##                  tickpos=None,
##                  tickinterval=None, ticknum=5,
##                  ticktype="tick", tickoptions={},
##                  ticklabelformat=None, ticklabeloptions={},
##                  noticks=False, noticklabels=False,
##                  **kwargs):
##        self.drawer.draw_axis(axis, minval, maxval, intercept,
##                              tickpos=tickpos, tickinterval=tickinterval, ticknum=ticknum,
##                              ticktype=ticktype, tickoptions=tickoptions,
##                              ticklabelformat=ticklabelformat, ticklabeloptions=ticklabeloptions,
##                              noticks=noticks, noticklabels=noticklabels,
##                              **kwargs)

    def get_legend(self, **legendoptions):
        legendoptions = legendoptions or dict()
        legend = Legend(self, **legendoptions)
        return legend

    def add_legend(self, legendoptions=None, **pasteoptions):
        self.changed = True
        legendoptions = legendoptions or {}
        legend = self.get_legend(**legendoptions)
        legend.pasteoptions.update(pasteoptions)
        self.foregroundgroup.add_layer(legend)

    # Drawing

    def render_one(self, layer, antialias=False):
        if not self.drawer: self._create_drawer()
        
        if layer.visible:
            layer.render(width=self.drawer.width,
                         height=self.drawer.height,
                         bbox=self.drawer.coordspace_bbox,
                         antialias=antialias)
            self.update_draworder()

    def render_all(self, antialias=False):
        #import time
        #t=time.time()
        if not self.drawer: self._create_drawer()
        #print "# createdraw",time.time()-t

        #import time
        #t=time.time()
        
        for layer in self.backgroundgroup:
            layer.render()
        
        for layer in self.layers:
            if layer.visible:
                layer.render(width=self.drawer.width,
                             height=self.drawer.height,
                             bbox=self.drawer.coordspace_bbox,
                             antialias=antialias)
                
                layer.render_text(width=self.drawer.width,
                                 height=self.drawer.height,
                                 bbox=self.drawer.coordspace_bbox)

        for layer in self.foregroundgroup:
            layer.render()
        #print "# rendall",time.time()-t
            
        self.changed = False
        import time
        t=time.time()
        self.update_draworder()
        print "# draword",time.time()-t

    def update_draworder(self):
        if self.drawer: self.drawer.clear()
        else: self.drawer = self._create_drawer()

        # paste the background decorations
        for layer in self.backgroundgroup:
            if layer.img:
                self.drawer.paste(layer.img, **layer.pasteoptions)

        # paste the map layers
        for layer in self.layers:
            if layer.visible and layer.img:
                self.drawer.paste(layer.img)

        # paste the map text/label layers
        for layer in self.layers:
            if layer.visible and layer.img_text:
                self.drawer.paste(layer.img_text)

        # paste the foreground decorations
        for layer in self.foregroundgroup:
            if layer.img:
                pasteoptions = layer.pasteoptions.copy()
                if isinstance(layer, Title):
                    # since title is rendered on separate img then pasted,
                    # some titleoptions needs to be passed to pasteoptions
                    # instead of the rendering method
                    extraargs = dict([(k,self.titleoptions[k]) for k in ["xy","anchor"] if k in self.titleoptions])
                    pasteoptions.update(extraargs)
                self.drawer.paste(layer.img, **pasteoptions)

        self.layers.changed = False
        self.img = self.drawer.get_image()

    def get_tkimage(self):
        # Special image format needed by Tkinter to display it in the GUI
        import PIL, PIL.ImageTk
        return PIL.ImageTk.PhotoImage(image=self.img)

    def view(self):
        mapp = self.copy() # ???
        # make gui
        from . import app
        win = app.builder.MultiLayerGUI(mapp)
        win.mainloop()

    def save(self, savepath):
        self.render_all(antialias=True) # antialias
        self.drawer.save(savepath)


        
