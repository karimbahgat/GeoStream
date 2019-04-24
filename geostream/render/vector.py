


class VectorLayer:
    def __init__(self, data, legendoptions=None, legend=True, datafilter=None, transparency=0, flipy=True, **options):
        """
        UNFINISHED docstring...
        
            flipy (optional): If True, flips the direction of the y-coordinate axis so that it increases towards the top of the screen
                instead of the bottom of the screen. This is useful for rendering unprojected long/lat coordinates and is the default. 
                Should only be changed to False when rendering projected x/y coordinates, whose y-axis typically increases towards the bottom.
        """

        if not isinstance(data, VectorData):
            # assume data is filepath
            dtoptions = options.get("dataoptions", dict())
            data = VectorData(data, **dtoptions)
        
        self.data = data
        self.visible = True
        self.transparency = transparency
        self.flipy = flipy
        self.img = None
        self.img_text = None

        self.effects = []

        self.legendoptions = legendoptions or dict()
        self.legend = legend
        self.datafilter = datafilter
        
        # by default, set random style color
        randomcolor = rgb("random")
        self.styleoptions = {"fillcolor": randomcolor,
                             "sortorder": "incr"}
        if 'Line' in self.data.type:
            self.styleoptions['outlinecolor'] = None
            
        # override default if any manually specified styleoptions
        self.styleoptions.update(options)

        # make sure has geom
        if not self.data.has_geometry():
            return

        # classify
        self.update()

    def update(self):
        # reset spatial index
        self.data.create_spatial_index()
        
        # set up symbol classifiers
        features = list(self.data) # classifications should be based on all features and not be affected by datafilter, thus enabling keeping the same classification across subsamples
        for key,val in self.styleoptions.copy().items():
            if key in "fillcolor fillsize outlinecolor outlinewidth".split():
                if isinstance(val, dict):
                    # random colors if not specified
                    if "color" in key and "colors" not in val:
                        if val["breaks"] == "unique":
                            val["colors"] = [rgb("random") for _ in range(20)]
                        else:
                            val["colors"] = [rgb("random"),rgb("random")]

                    # remove args that are not part of classypie
                    val = dict(val)
                    if isinstance(val.get("key"), basestring):
                        fieldname = val["key"]
                        val["key"] = lambda f,fn=fieldname: f[fn] # turn field name into callable
                    if "color" in key:
                        val["classvalues"] = val.pop("colors")
                    else:
                        val["classvalues"] = val.pop("sizes")

                    notclassified = val.pop("notclassified", None if "color" in key else 0) # this means symbol defaults to None ie transparent for colors and 0 for sizes if feature had a missing/null value, which should be correct
                    if "color" in key and notclassified != None:
                        notclassified = rgb(notclassified)

                    # convert any color names to pure numeric so can be handled by classypie
                    if "color" in key:
                        if isinstance(val["classvalues"], dict):
                            # value color dict mapping for unique breaks
                            val["classvalues"] = dict([(k,rgb(v)) for k,v in val["classvalues"].items()])
                        else:
                            # color gradient
                            val["classvalues"] = [rgb(col) for col in val["classvalues"]]
                    else:
                        pass #val["classvalues"] = [Unit(col) for col in val["classvalues"]]

                    # cache precalculated values in id dict
                    # more memory friendly alternative is to only calculate breakpoints
                    # and then find classvalue for feature when rendering,
                    # which is likely slower
                    classifier = cp.Classifier(features, **val)
                    self.styleoptions[key] = dict(classifier=classifier,
                                                   symbols=dict((id(f),classval) for f,classval in classifier),
                                                   notclassified=notclassified
                                                   )

                    # convert from area to radius for more correct visual comparisons
                    # first interpolation was done between areas, now convert to radius sizes
                    # TODO: now only circles, test and calc for other shapes too
                    if 'size' in key and 'Point' in self.data.type:
                        shp = self.styleoptions.get('shape')
                        if shp is None or shp == 'circle':
                            #val['classvalues'] = [math.sqrt(v/math.pi) for v in val['classvalues']]
                            classifier.classvalues_interp = [math.sqrt(v/math.pi) for v in classifier.classvalues_interp]
                            self.styleoptions[key]['symbols'] = dict((id(f),classval) for f,classval in classifier)
                    
                elif hasattr(val, "__call__"):
                    pass
                
                else:
                    # convert any color names to pure numeric so can be handled by classypie
                    if "color" in key:
                        val = rgb(val)
                    else:
                        #pass #val = Unit(val)
                        # convert from area to radius for more correct visual comparisons
                        # TODO: now only circles, test and calc for other shapes too
                        if 'Point' in self.data.type:
                            shp = self.styleoptions.get('shape')
                            if shp is None or shp == 'circle':
                                val = math.sqrt(val/math.pi)

                    self.styleoptions[key] = val

        # set up text classifiers
        if "text" in self.styleoptions and "textoptions" in self.styleoptions:
            for key,val in self.styleoptions["textoptions"].copy().items():
                if isinstance(val, dict):
                    # random colors if not specified in unique algo
                    if "color" in key and "colors" not in val:
                        if val["breaks"] == "unique":
                            val["colors"] = [rgb("random") for _ in range(20)]
                        else:
                            val["colors"] = [rgb("random"),rgb("random")]

                    # remove args that are not part of classypie
                    val = dict(val)
                    if isinstance(val.get("key"), basestring):
                        fieldname = val["key"]
                        val["key"] = lambda f,fn=fieldname: f[fn] # turn field name into callable
                    if "color" in key:
                        val["classvalues"] = val.pop("colors")
                    else:
                        val["classvalues"] = val.pop("sizes")
                    notclassified = val.pop("notclassified", None if "color" in key else 0) # this means symbol defaults to None ie transparent for colors and 0 for sizes if feature had a missing/null value, which should be correct

                    # cache precalculated values in id dict
                    # more memory friendly alternative is to only calculate breakpoints
                    # and then find classvalue for feature when rendering,
                    # which is likely slower
                    classifier = cp.Classifier(features, **val)
                    self.styleoptions["textoptions"][key] = dict(classifier=classifier,
                                                               symbols=dict((id(f),classval) for f,classval in classifier),
                                                               notclassified=notclassified
                                                               )
                    
                elif hasattr(val, "__call__"):
                    pass
                
                else:
                    # convert any color names to pure numeric so can be handled by classypie
                    if "color" in key:
                        val = rgb(val)
                    else:
                        pass #val = Unit(val)
                    self.styleoptions["textoptions"][key] = val

    def is_empty(self):
        """Used for external callers unaware of the vector or raster nature of the layer"""
        return not self.has_geometry()

    def has_geometry(self):
        return any((feat.geometry for feat in self.features()))

    def copy(self):
        new = VectorLayer(self.data)
        new.visible = self.visible
        new.img = self.img.copy() if self.img else None
        new.img_text = self.img_text.copy() if self.img_text else None

        new.legendoptions = self.legendoptions
        new.legend = self.legend
        new.datafilter = self.datafilter
        
        new.styleoptions = self.styleoptions.copy()

        return new
    
    @property
    def bbox(self):
        if self.has_geometry():
            xmins, ymins, xmaxs, ymaxs = itertools.izip(*(feat.bbox for feat in self.features() if feat.geometry))
            bbox = min(xmins),min(ymins),max(xmaxs),max(ymaxs)
            return bbox
        else:
            raise Exception("Cannot get bbox since there are no selected features with geometries")

    def features(self, bbox=None):
        # get features based on spatial index, for better speeds when zooming
        if bbox:
            if not hasattr(self.data, "spindex"):
                self.data.create_spatial_index()
            features = self.data.quick_overlap(bbox)
        else:
            features = self.data
        
        if self.datafilter:
            for feat in features:
                if self.datafilter(feat):
                    yield feat
        else:
            for feat in features:
                yield feat

    def add_effect(self, effect, **kwargs):
        
        if isinstance(effect, basestring):

            if effect == "shadow":
                def effect(lyr):
                    _,a = lyr.img.convert('LA').split()
                    opacity = kwargs.get('opacity', 0.777) # dark/strong shadow
                    a = a.point(lambda v: v*opacity) 
                    binary = PIL.Image.new('L', lyr.img.size, 0) # black
                    binary.putalpha(a)
                    drawer = pyagg.canvas.from_image(binary)
                    #binary = lyr.img.point(lambda v: 255 if v > 0 else 0)
                    #drawer = pyagg.canvas.from_image(binary)
                    #drawer.replace_color((255,255,255,255), kwargs.get("color", (115,115,115,155)))
                    drawer.move(kwargs.get("xdist"), kwargs.get("ydist"))
                    drawer.paste(lyr.img)
                    img = drawer.get_image()
                    return img

            elif effect == "glow":

                def effect(lyr):
                    import PIL, PIL.ImageMorph
                    
                    binary = lyr.img.point(lambda v: 255 if v > 0 else 0).convert("L")
                    
                    color = kwargs.get("color")
                    if isinstance(color, list):
                        # use gradient to set range of colors via incremental grow/shrink
                        newimg = PIL.Image.new("RGBA", lyr.img.size, (0,0,0,0))
                        grad = pyagg.canvas.Gradient(color)
                        for col in grad.interp(kwargs.get("size")):
                            col = tuple(col)
                            _,binary = PIL.ImageMorph.MorphOp(op_name="dilation8").apply(binary)
                            _,edge = PIL.ImageMorph.MorphOp(op_name="edge").apply(binary)
                            if len(col) == 4:
                                edge = edge.point(lambda v: col[3] if v == 255 else 0)
                            newimg.paste(col[:3], (0,0), mask=edge)
                        newimg.paste(lyr.img, (0,0), lyr.img)
                    else:
                        # entire area same color
                        for _ in range(kwargs.get("size")):
                            _,binary = PIL.ImageMorph.MorphOp(op_name="dilation8").apply(binary)
                        newimg = PIL.Image.new("RGBA", lyr.img.size, (0,0,0,0))
                        newimg.paste(color, (0,0), lyr.img)
                        newimg.paste(lyr.img, (0,0), binary)
                        
                    return newimg
                
            elif effect == "inner":
                # TODO: gets affected by previous effects, somehow only get original rendered image
                # TODO: should do inner type on each feature, not the entire layer.
                # OR: effect for entire layer, and separate for each feature via styleoptions...?
                # TODO: canvas edge should not be counted...
                # TODO: transp gradient not working, sees through even original layer...
                    
                def effect(lyr):
                    import PIL, PIL.ImageMorph
                    
                    binary = lyr.img.point(lambda v: 255 if v > 0 else 0).convert("L")
                    
                    color = kwargs.get("color")
                    if isinstance(color, list):
                        # use gradient to set range of colors via incremental grow/shrink
                        newimg = lyr.img.copy()
                        grad = pyagg.canvas.Gradient(color)
                        for col in grad.interp(kwargs.get("size")):
                            col = tuple(col)
                            _,binary = PIL.ImageMorph.MorphOp(op_name="erosion8").apply(binary)
                            _,edge = PIL.ImageMorph.MorphOp(op_name="edge").apply(binary)
                            if len(col) == 4:
                                edge = edge.point(lambda v: col[3] if v == 255 else 0)
                            newimg.paste(col[:3], (0,0), mask=edge)
                    else:
                        # entire area same color
                        for _ in range(kwargs.get("size")):
                            _,binary = PIL.ImageMorph.MorphOp(op_name="erosion8").apply(binary)
                        newimg = PIL.Image.new("RGBA", lyr.img.size, (0,0,0,0))
                        newimg.paste(color, (0,0), lyr.img)
                        newimg.paste(lyr.img, (0,0), binary)
                        
                    return newimg
                
            else:
                raise Exception("Not a valid effect")
        
        self.effects.append(effect)

    def render(self, width, height, bbox=None, antialias=False):

        # normal way
        if self.has_geometry():
            import time
            t=time.time()

            if not bbox:
                bbox = self.bbox
            
            drawer = pyagg.Canvas(width, height, background=None)
            drawer.custom_space(*bbox, lock_ratio=True)
            
            features = self.features(bbox=bbox)

            # custom draworder (sortorder is only used with sortkey)
            if "sortkey" in self.styleoptions:
                features = sorted(features, key=self.styleoptions["sortkey"],
                                  reverse=self.styleoptions["sortorder"].lower() == "decr")

            # prep PIL if non-antialias polygon
            if not antialias and "Polygon" in self.data.type:
                #print "preint",time.time()-t
                import time
                t=time.time()
                img = PIL.Image.new("RGBA", (width,height), None)
                PIL_drawer = PIL.ImageDraw.Draw(img)   #self.PIL_drawer

            # for each
            for feat in features:
                
                # get symbols
                rendict = dict()
                if "shape" in self.styleoptions: rendict["shape"] = self.styleoptions["shape"]
                for key in "fillcolor fillsize outlinecolor outlinewidth".split():
                    if key in self.styleoptions:
                        val = self.styleoptions[key]
                        if isinstance(val, dict):
                            # lookup self in precomputed symboldict
                            fid = id(feat)
                            if fid in val["symbols"]:
                                rendict[key] = val["symbols"][fid]
                            else:
                                rendict[key] = val["notclassified"]
                        elif hasattr(val, "__call__"):
                            rendict[key] = val(feat)
                        else:
                            rendict[key] = val

                # draw

                # fast PIL Approach for non-antialias polygons
                if not antialias and "Polygon" in feat.geometry["type"]:

                    if "Multi" in feat.geometry["type"]:
                        geoms = feat.geometry["coordinates"]
                    else:
                        geoms = [feat.geometry["coordinates"]]

                    fill = tuple((int(c) for c in rendict["fillcolor"])) if rendict.get("fillcolor") else None
                    outline = tuple((int(c) for c in rendict["outlinecolor"])) if rendict.get("outlinecolor") else None
                    
                    for poly in geoms:
                        coords = poly[0]
                        if len(poly) > 1:
                            holes = poly[1:0]
                        else:
                            holes = []

                        # first exterior
                        path = PIL.ImagePath.Path([tuple(p) for p in coords])
                        path.transform(drawer.coordspace_transform)
                        #print "draw",str(path.tolist())[:300]
                        path.compact(1)
                        #print "draw",str(path.tolist())[:100]
                        if len(path) > 1:
                            PIL_drawer.polygon(path, fill, None)
                            PIL_drawer.line(path, outline, 1)

                        # then holes
                        for hole in holes:
                            path = PIL.ImagePath.Path([tuple(p) for p in hole])
                            path.transform(drawer.coordspace_transform)
                            path.compact(1)
                            if len(path) > 1:
                                PIL_drawer.polygon(path, (0,0,0,0), None)
                                PIL_drawer.line(path, outline, 1)

                else:
                    # high qual geojson
                    drawer.draw_geojson(feat.geometry, **rendict)

            # flush
            print "internal",time.time()-t
            if not antialias and "Polygon" in self.data.type:
                self.img = img
            else:
                self.img = drawer.get_image()

            # transparency
            if self.transparency:
                #opac = 256 - int(256*(self.transparency/100.0))
                opac = 1 - self.transparency
                
                r,g,b,a = self.img.split()
                #a = PIL.ImageMath.eval('min(alpha,opac)', alpha=a, opac=opac).convert('L') # putalpha img must be 0 to make it transparent, so the nodata mask must be inverted
                a = PIL.ImageMath.eval('float(alpha) * opac', alpha=a, opac=opac).convert('L') # putalpha img must be 0 to make it transparent, so the nodata mask must be inverted
                self.img.putalpha(a)
                
            # effects
            for eff in self.effects:
                self.img = eff(self)

        else:
            self.img = None

    def render_text(self, width, height, bbox=None):
        if self.has_geometry() and self.styleoptions.get("text"):

            textkey = self.styleoptions["text"]
            
            if not bbox:
                bbox = self.bbox
            
            drawer = pyagg.Canvas(width, height, background=None)
            drawer.custom_space(*bbox, lock_ratio=True)

            
            features = self.features(bbox=bbox)

            # custom draworder (sortorder is only used with sortkey)
            if "sortkey" in self.styleoptions:
                features = sorted(features, key=self.styleoptions["sortkey"],
                                  reverse=self.styleoptions["sortorder"].lower() == "decr")

            # draw each as text
            for feat in features:
                text = textkey(feat)
                
                if text is not None:
                
                    # get symbols
                    rendict = dict()
                    if "textoptions" in self.styleoptions:
                        for key,val in self.styleoptions["textoptions"].copy().items():
                            if isinstance(val, dict):
                                # lookup self in precomputed symboldict
                                fid = id(feat)
                                if fid in val["symbols"]:
                                    rendict[key] = val["symbols"][fid]
                                else:
                                    rendict[key] = val["notclassified"]
                            elif hasattr(val, "__call__"):
                                rendict[key] = val(feat)
                            else:
                                rendict[key] = val

                    # draw
                    # either bbox or xy can be set for positioning
                    if "bbox" not in rendict:
                        # also allow custom key for any of the options
                        for k,v in rendict.items():
                            if hasattr(v, "__call__"):
                                rendict[k] = v(feat)
                        # default to xy being centroid
                        rendict["xy"] = rendict.get("xy", "centroid")
                        if rendict["xy"] == "centroid":
                            rendict["xy"] = feat.get_shapely().centroid.coords[0]
                    drawer.draw_text(text, **rendict)
                
            self.img_text = drawer.get_image()

            # transparency
            if self.transparency:
                #opac = 256 - int(256*(self.transparency/100.0))
                opac = 1 - self.transparency
                
                r,g,b,a = self.img_text.split()
                #a = PIL.ImageMath.eval('min(alpha,opac)', alpha=a, opac=opac).convert('L') # putalpha img must be 0 to make it transparent, so the nodata mask must be inverted
                a = PIL.ImageMath.eval('float(alpha) * opac', alpha=a, opac=opac).convert('L') # putalpha img must be 0 to make it transparent, so the nodata mask must be inverted
                self.img_text.putalpha(a)

        else:
            self.img_text = None
