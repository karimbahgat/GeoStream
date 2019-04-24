

class LayerGroup:
    def __init__(self):
        self._layers = list()
        self.connected_maps = list()
        self.dimensions = dict()
        self.changed = False

    def __iter__(self):
        for layer in self._layers:
            yield layer

    def __len__(self):
        return len(self._layers)

    def __getitem__(self, i):
        return self._layers[i]

    def __setitem__(self, i, value):
        self.changed = True
        self._layers[i] = value

    def is_empty(self):
        return all((lyr.is_empty() for lyr in self))

    @property
    def bbox(self):
        if not self.is_empty():
            xmins,ymins,xmaxs,ymaxs = zip(*(lyr.bbox for lyr in self._layers if not lyr.is_empty() ))
            bbox = min(xmins),min(ymins),max(xmaxs),max(ymaxs)
            return bbox

        else:
            raise Exception("Cannot get bbox since there are no layers with geometries")

##    def add_dimension(self, dimtag, dimvalues):
##        # used by parent map to batch render all varieties of this layer
##        self.dimensions[dimtag] = dimvalues # list of dimval-dimfunc pairs

    def copy(self):
        layergroup = LayerGroup()
        layergroup._layers = list(self._layers)
        return layergroup

    def add_layer(self, layer, **options):
        self.changed = True
        
        if not isinstance(layer, (VectorLayer,RasterLayer)):
            # if data instance
            if isinstance(layer, VectorData):
                layer = VectorLayer(layer, **options)
            elif isinstance(layer, RasterData):
                layer = RasterLayer(layer, **options)
                
            # or if path string to data
            elif isinstance(layer, basestring):
                if vector_filetype(layer):
                    layer = VectorLayer(layer, **options)
                elif raster_filetype(layer):
                    layer = RasterLayer(layer, **options)
                else:
                    raise Exception("Filetype not supported")

            # invalid input
            else:
                raise Exception("Adding a layer requires either an existing layer instance, a data instance, or a filepath.")
            
        self._layers.append(layer)

        return layer

    def move_layer(self, from_pos, to_pos):
        self.changed = True
        layer = self._layers.pop(from_pos)
        self._layers.insert(to_pos, layer)

    def remove_layer(self, position):
        self.changed = True
        self._layers.pop(position)

    def get_position(self, layer):
        return self._layers.index(layer)





class BackgroundLayerGroup(LayerGroup):
    def add_layer(self, layer, **options):
        self._layers.append(layer, **options)

    def copy(self):
        background = BackgroundLayerGroup()
        background._layers = list(self._layers)
        return background





class ForegroundLayerGroup(LayerGroup):
    def add_layer(self, layer, **options):
        self._layers.append(layer, **options)

    def copy(self):
        foreground = ForegroundLayerGroup()
        foreground._layers = list(self._layers)
        return foreground



