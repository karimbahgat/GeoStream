


DEFAULTSTYLE = None

COLORSTYLES = dict([("strong", dict( [("intensity",1), ("brightness",0.5)]) ),
                    ("dark", dict( [("intensity",0.8), ("brightness",0.3)]) ),
                    ("matte", dict( [("intensity",0.4), ("brightness",0.5)]) ),
                    ("bright", dict( [("intensity",0.8), ("brightness",0.7)] ) ),
                    ("weak", dict( [("intensity",0.3), ("brightness",0.5)] ) ),
                    ("pastelle", dict( [("intensity",0.5), ("brightness",0.6)] ) )
                    ])

def rgb(basecolor, intensity=None, brightness=None, opacity=None, style=None):
    """
    Returns an rgba color tuple of the color options specified.

    - basecolor: the human-like name of a color. Always required, but can also be set to 'random'. | string
    - *intensity: how strong the color should be. Must be a float between 0 and 1, or set to 'random' (by default uses the 'strong' style values, see 'style' below). | float between 0 and 1
    - *brightness: how light or dark the color should be. Must be a float between 0 and 1 , or set to 'random' (by default uses the 'strong' style values, see 'style' below). | float between 0 and 1
    - *style: a named style that overrides the brightness and intensity options (optional). | For valid style names, see below.

    Valid style names are:
    - 'strong'
    - 'dark'
    - 'matte'
    - 'bright'
    - 'pastelle'
    """
    # test if none
    if basecolor is None:
        return None
    
    # if already rgb tuple just return
    if isinstance(basecolor, (tuple,list)):
        rgb = [v / 255.0 for v in basecolor[:3]]
        if len(basecolor) == 3:
            rgba = list(colour.Color(rgb=rgb, saturation=intensity, luminance=brightness).rgb) + [opacity or 255]
        elif len(basecolor) == 4:
            rgba = list(colour.Color(rgb=rgb, saturation=intensity, luminance=brightness).rgb) + [opacity or basecolor[3]]
        rgba = [int(round(v * 255)) for v in rgba[:3]] + [rgba[3]]
        return tuple(rgba)
    
    #first check on intens/bright
    if not style and DEFAULTSTYLE:
        style = DEFAULTSTYLE
    if style and basecolor not in ("black","white","gray"):
        #style overrides manual intensity and brightness options
        intensity = COLORSTYLES[style]["intensity"]
        brightness = COLORSTYLES[style]["brightness"]
    else:
        #special black,white,gray mode, bc random intens/bright starts creating colors, so have to be ignored
        if basecolor in ("black","white","gray"):
            if brightness == "random":
                brightness = random.randrange(20,80)/100.0
        #or normal
        else:
            if intensity == "random":
                intensity = random.randrange(20,80)/100.0
            elif intensity is None:
                intensity = 0.7
            if brightness == "random":
                brightness = random.randrange(20,80)/100.0
            elif brightness is None:
                brightness = 0.5
    #then assign colors
    if basecolor in ("black","white","gray"):
        #graymode
        if brightness is None:
            rgb = colour.Color(color=basecolor).rgb
        else:
            #only listen to gray brightness if was specified by user or randomized
            col = colour.Color(color=basecolor)
            col.luminance = brightness
            rgb = col.rgb
    elif basecolor == "random":
        #random colormode
        basecolor = tuple([random.uniform(0,1), random.uniform(0,1), random.uniform(0,1)])
        col = colour.Color(rgb=basecolor)
        col.saturation = intensity
        col.luminance = brightness
        rgb = col.rgb
    elif isinstance(basecolor, (str,unicode)):
        #color text name
        col = colour.Color(basecolor)
        col.saturation = intensity
        col.luminance = brightness
        rgb = col.rgb
    else:
        #custom made color
        col = colour.Color(rgb=basecolor)
        col.saturation = intensity
        col.luminance = brightness
        rgb = col.rgb

    rgba = [int(round(v * 255)) for v in rgb] + [opacity or 255]
    return tuple(rgba)
