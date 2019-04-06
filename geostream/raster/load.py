
from . import fileformats


file_extensions = {".tif": "GeoTIFF",
                   }

def detect_filetype(filepath):
    for ext in file_extensions.keys():
        if filepath.lower().endswith(ext):
            return file_extensions[ext]
    else:
        return None


def from_file(filepath, **kwargs):
    filetype = detect_filetype(filepath)
    
    if filetype in ('GeoTIFF'):
        reader = fileformats.GeoTIFF(filepath, **kwargs)

    else:
        raise Exception("Could not import data from the given filepath: the filetype extension is either missing or not supported")

    return reader

def from_wkb(wkb):
    pass
