
import geostream as gs

def viewrast(results):
    import pythongis as pg
    from PIL import Image
    for (rast,) in results:
        print rast
        data = rast.bands[0].data()
##        img = Image.fromarray(data)
##        img.show()
##        print data.shape, img
##        print rast.affine
##        r = pg.RasterData(image=img,
##                          width=rast.width,
##                          height=rast.height,
##                          affine=rast.affine)
##        print r
##        r.view()

TESTFILE = 'rasttest.db'

workspace = gs.Workspace(TESTFILE, 'w')

origrast = gs.raster.data.Raster(r"P:\Freelance\Projects\Henry City Data\Work Files\Sources\GlobCover\GLOBCOVER_L4_200901_200912_V2.3.tif")
print origrast
print origrast.affine

##workspace.clear(1)
##workspace.import_table('countries', r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", replace=True)
##workspace.import_raster('globcover', r"P:\Freelance\Projects\Henry City Data\Work Files\Sources\GlobCover\GLOBCOVER_L4_200901_200912_V2.3.tif",
##                        tilesize=(1000,1000), replace=True)

print workspace

countries = workspace.table('countries')
glob = workspace.table('globcover')

##glob.create_spatial_index('rast')

for co in countries.select(['name','geom']):
    print co[0]
    isec = glob.intersection('rast', co[-1].bounds)
    #viewrast(isec)
    for (rast,) in isec:
        data = rast.bands[0].data()
        #print data.mean()
        
