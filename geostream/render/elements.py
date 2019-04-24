

class Background:
    def __init__(self, map):
        self.map = map
        self.img = None
        self.pasteoptions = dict()

    def render(self):
        canv = pyagg.Canvas(self.map.drawer.width, self.map.drawer.height, self.map.background)
        self.img = canv.get_image()



class Title:
    def __init__(self, layout):
        self.layout = layout
        self.img = None
        self.pasteoptions = dict(xy=("50%w","1%h"), anchor="n")

    def render(self):
        if self.layout.title:
            # since title is rendered on separate img then pasted,
            # some titleoptions needs to be passed to pasteoptions
            # instead of the rendering method
            titleoptions = self.layout.titleoptions.copy()
            titleoptions.pop("xy", None)
            titleoptions.pop("anchor", None)
            rendered = pyagg.legend.Label(self.layout.title, refcanvas=self.layout.drawer, **titleoptions).render() # pyagg label indeed implements a render method()
            self.img = rendered.get_image()


        
