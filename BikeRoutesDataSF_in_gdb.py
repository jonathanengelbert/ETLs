#This script copies and processes Bike Routes data from DATASF. Data is staged and processed before being fed to BikeRoutes.gdb
#Last modified: 11/20/2017 by Jonathan Engelbert
#
### No Known Issues
### WARNING: This script points to and depends on  "working.gdb" and a "working" folder in C:\ETLs\TIM\TIMUpdates\
###################################################################################

import arcpy, os, urllib, struct, sys, shutil
import zipfile, zlib

def explode(out, zip, name):
    # Given a 'zip' instance, copy data from the 'name' to the 'out' stream.
    zinfo = zip.getinfo(name)
 
    if zinfo.compress_type == zipfile.ZIP_STORED:
        decoder = None
    elif zinfo.compress_type == zipfile.ZIP_DEFLATED:
        decoder = zlib.decompressobj(-zlib.MAX_WBITS)
    else:
        raise zipfile.BadZipFile("unsupported compression method")
 
    # Navigate to the file header and skip over it
    zip.fp.seek(zinfo.header_offset)
    fheader = zip.fp.read(30)
    if fheader[0:4] != zipfile.stringFileHeader:
        raise zipfile.BadZipfile, "Bad magic number for file header"
 
    fheader = struct.unpack(zipfile.structFileHeader, fheader)
    fname = zip.fp.read(fheader[zipfile._FH_FILENAME_LENGTH])
    if fheader[zipfile._FH_EXTRA_FIELD_LENGTH]:
        zip.fp.read(fheader[zipfile._FH_EXTRA_FIELD_LENGTH])
 
    if fname != zinfo.orig_filename:
        raise zipfile.BadZipfile, \
            'File name in directory "%s" and header "%s" differ.' % (
                zinfo.orig_filename, fname)
 
    size = zinfo.compress_size
 
    while 1:
        data = zip.fp.read(min(size, 8192))
        if not data:
            break
        size -= len(data)
        if decoder:
            data = decoder.decompress(data)
        out.write(data)
 
    if decoder:
        out.write(decoder.decompress('Z'))
        out.write(decoder.flush())
        
def unzipFile(zippedPath, extractDir): 
 
    # Make sure extractDir exists
    if not os.path.exists(extractDir):
        os.makedirs(extractDir)
 
    # Open the zip file for read
    zip = zipfile.ZipFile(zippedPath,'r') 
 
    for entry in zip.namelist(): 
        # If folder, create directory - otherwise write file directly 
        if entry.endswith("/"): 
            fullPath = os.path.join(extractDir, entry)
            if not os.path.exists(fullPath): 
                os.makedirs(fullPath) 
        else: 
            # Get the relative directory for this file
            (relativeDir, fileName) = os.path.split(entry)
 
            # Concatenate with extractDir and create if missing
            fullPath = os.path.join(extractDir, relativeDir)
            if not os.path.exists(fullPath):
                os.makedirs(fullPath)
 
            # Create the file at this location
            fileLocation = os.path.join(fullPath, fileName)
            outfile = open(fileLocation,'wb') 
            explode(outfile, zip, entry)
            outfile.flush()
            outfile.close() 

#VARIABLES: Dependent on "working" folder and "working.gdb. Feed to TIMReady/BikeRoutesDataSF.gdb

arcpy.env.overwriteOutput=True


BikeRoutesDataSF_shp = "C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp"
GISDATA_BikeRoutes_Current__4_ = BikeRoutesDataSF_shp
BikeRoutesDataSF_shp__2_ = BikeRoutesDataSF_shp
GISDATA_BikeRoutes_Current = "Database Connections\\Planning Dept SDE Service w GISData.sde\\GISDATA.BikeRoutes_Current"
GISDATA_BikeRoutes_Current__2_ = GISDATA_BikeRoutes_Current
TIMReady = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\\"
BikeRoutesDataSF_250FtBuffer_shp = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\\BikeRoutesDataSF_250FtBuffer"
BikeRoutesDataSF_HalfMileBuffer_shp = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\\BikeRoutesDataSF_HalfMileBuffer"


myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print theStartTime

myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
file = open("C:/ETLs/TIM/TIMUpdates/Logs/"+ myStartDate +"BikeRoutesDataSF.txt", "w")
file.write(theStartTime + "\n")
when =datetime.date.today()
theDate = when.strftime("%d")
theDay=when.strftime("%A") 


try:
    zippedPath = r"C:/ETLs/TIM/TIMUpdates/working/BikeRoutesDataSF/BikeRoutesDataSF.zip"
    extractDir = r"C:/ETLs/TIM/TIMUpdates/working/BikeRoutesDataSF"
    urllib.urlretrieve ("https://data.sfgov.org/api/geospatial/x3cv-qums?method=export&format=Shapefile", zippedPath)
    print "downloaded"
    file.write("Downloaded from DataSF" + "\n")

    unzipFile(zippedPath, extractDir)
    print "unzipped"
    file.write("unzipped" + "\n")

    os.system(r"C:/ETLs/TIM/TIMUpdates/working/BikeRoutesDataSF/RenameShapefile.bat")
    file.write("Renamed shapefile" + "\n")
    
    # Process: Truncate Table
    arcpy.TruncateTable_management(GISDATA_BikeRoutes_Current)
    print "truncated cityplan03SDE FC"
    file.write("Truncated cityplan03SDE FC" + "\n")

    # Process: Append
    arcpy.Append_management("C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp", GISDATA_BikeRoutes_Current__2_, "NO_TEST", "FROM_STREE \"FROM_STREE\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,from_stree,-1,-1;DATE_LAST_ \"DATE_LAST_\" true true false 36 Date 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,date_last_,-1,-1;TIME_LAST_ \"TIME_LAST_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,time_last_,-1,-1;CNN_ID \"CNN_ID\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,cnn_id,-1,-1;YEAR_INSTA \"YEAR_INSTA\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,year_insta,-1,-1;CONTRAFLOW \"CONTRAFLOW\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,contraflow,-1,-1;INSTALL_YE \"INSTALL_YE\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_ye,-1,-1;FULL_STREE \"FULL_STREE\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,full_stree,-1,-1;DOUBLE \"DOUBLE\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,double,-1,-1;DIRECTION \"DIRECTION\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,direction,-1,-1;FISCAL_QUA \"FISCAL_QUA\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_qua,-1,-1;FISCAL_YEA \"FISCAL_YEA\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_yea,-1,-1;BIKE_ROUTE \"BIKE_ROUTE\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,bike_route,-1,-1;OBJECT_ID \"OBJECT_ID\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,object_id,-1,-1;SHARROW \"SHARROW\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,sharrow,-1,-1;BARRIER_TY \"BARRIER_TY\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,barrier_ty,-1,-1;NUMBER_OF_ \"NUMBER_OF_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,number_of_,-1,-1;LENGTH_FEE \"LENGTH_FEE\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_fee,-1,-1;LENGTH_MIL \"LENGTH_MIL\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_mil,-1,-1;TO_STREET \"TO_STREET\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,to_street,-1,-1;STREET_TYP \"STREET_TYP\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_typ,-1,-1;SMALL_STRE \"SMALL_STRE\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,small_stre,-1,-1;SURFACE_TR \"SURFACE_TR\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,surface_tr,-1,-1;INSTALL_MO \"INSTALL_MO\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_mo,-1,-1;FACILITY_T \"FACILITY_T\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,facility_t,-1,-1;INNOVATIVE \"INNOVATIVE\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,innovative,-1,-1;LAST_UPGRA \"LAST_UPGRA\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,last_upgra,-1,-1;STREET_NAM \"STREET_NAM\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_nam,-1,-1;NOTES \"NOTES\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,notes,-1,-1;UPDATE_MON \"UPDATE_MON\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,update_mon,-1,-1;SHAPE.LEN \"SHAPE.LEN\" false false true 0 Double 0 0 ,First,#", "")
    print "Appended new FC into cityplan03sde"
    file.write("Appended new FC into cityplan03sde" + "\n")

    # Process: Feature Class to Feature Class
    arcpy.FeatureClassToFeatureClass_conversion(BikeRoutesDataSF_shp, TIMReady, "BikeRoutesDataSF", "", "from_stree \"from_stree\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,from_stree,-1,-1;date_last_ \"date_last_\" true true false 8 Date 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,date_last_,-1,-1;time_last_ \"time_last_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,time_last_,-1,-1;cnn_id \"cnn_id\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,cnn_id,-1,-1;year_insta \"year_insta\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,year_insta,-1,-1;contraflow \"contraflow\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,contraflow,-1,-1;install_ye \"install_ye\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_ye,-1,-1;full_stree \"full_stree\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,full_stree,-1,-1;double \"double\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,double,-1,-1;direction \"direction\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,direction,-1,-1;fiscal_qua \"fiscal_qua\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_qua,-1,-1;fiscal_yea \"fiscal_yea\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_yea,-1,-1;bike_route \"bike_route\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,bike_route,-1,-1;object_id \"object_id\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,object_id,-1,-1;sharrow \"sharrow\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,sharrow,-1,-1;barrier_ty \"barrier_ty\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,barrier_ty,-1,-1;number_of_ \"number_of_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,number_of_,-1,-1;length_fee \"length_fee\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_fee,-1,-1;length_mil \"length_mil\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_mil,-1,-1;to_street \"to_street\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,to_street,-1,-1;street_typ \"street_typ\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_typ,-1,-1;small_stre \"small_stre\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,small_stre,-1,-1;surface_tr \"surface_tr\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,surface_tr,-1,-1;install_mo \"install_mo\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_mo,-1,-1;facility_t \"facility_t\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,facility_t,-1,-1;innovative \"innovative\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,innovative,-1,-1;last_upgra \"last_upgra\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,last_upgra,-1,-1;street_nam \"street_nam\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_nam,-1,-1;notes \"notes\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,notes,-1,-1;update_mon \"update_mon\" true true false 33 Double 31 32 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,update_mon,-1,-1", "")
    print "Copied to TIMReady"
    file.write("Copied routes to TIMReady" + "\n")

    # Process: Buffer
    arcpy.Buffer_analysis(BikeRoutesDataSF_shp, BikeRoutesDataSF_250FtBuffer_shp, "250 Feet", "FULL", "ROUND", "LIST", "street_typ;facility_t;street_nam", "PLANAR")
    print "ran 250Ft Buffer"
    file.write("Ran 250ft Buffer" + "\n")

    # Process: Buffer (2)
    arcpy.Buffer_analysis(BikeRoutesDataSF_shp, BikeRoutesDataSF_HalfMileBuffer_shp, "0.5 Miles", "FULL", "ROUND", "LIST", "street_typ;facility_t;street_nam", "PLANAR")
    print "ran 0.5 mile buffer"
    file.write("Ran 1/2 mile buffer" + "\n")

    #Refresh spatial indexes
    BikeRoutesDataSF_shp = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\\BikeRoutesDataSF"
    BikeRoutesDataSF_shp__2_ = BikeRoutesDataSF_shp
    BikeRoutesDataSF_250FtBuffer_shp = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\\BikeRoutesDataSF_250FtBuffer"
    BikeRoutesDataSF_250FtBuffer_shp__2_ = BikeRoutesDataSF_250FtBuffer_shp
    BikeRoutesDataSF_HalfMileBuffer_shp = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeRoutesDataSF.gdb\BikeRoutesDataSF_HalfMileBuffer"
    BikeRoutesDataSF_HalfMileBuffer_shp__2_ = BikeRoutesDataSF_HalfMileBuffer_shp

    try:
        arcpy.AddSpatialIndex_management(BikeRoutesDataSF_shp, "0", "0", "0")
        print "Updated spatial indexes for Bike Routes"
        file.write("Updated spatial indexes for Bike Routes" + "\n")
    except:
        print "FAILED to updated spatial indexes for Bike Routes"
        file.write("FAILED to update spatial indexes for Bike Routes" + "\n")
        
    try:
        arcpy.AddSpatialIndex_management(BikeRoutesDataSF_250FtBuffer_shp, "0", "0", "0")
        print "Updated spatial indexes for Bike Routes 250ft buffer"
        file.write("Updated spatial indexes for Bike Routes 250ft buffer" + "\n")
    except:
        print "FAILED to update spatial indexes for Bike Routes 250ft buffer"
        file.write("FAILED to update spatial indexes for Bike Routes 250ft buffer" + "\n")
    try:
        arcpy.AddSpatialIndex_management(BikeRoutesDataSF_HalfMileBuffer_shp, "0", "0", "0")
        print "Updated spatial indexes for Bike Routes 1/2 mile buffer"
        file.write("Updated spatial indexes 1/2 mile buffer" + "\n")
    except:
        print "FAILED to update spatial indexes 1/2 mile buffer"
        file.write("FAILED to update spatial indexes 1/2 mile buffer" + "\n")




    #Update Bike Routes in CPC-POSTGIS-1
    gis_db_gisdata_Transport_Bike_Routes = "Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Transport_Bike_Routes" 
    BikeRoutesDataSF_shp = "C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp"
    try:
        arcpy.TruncateTable_management(gis_db_gisdata_Transport_Bike_Routes)
        print "Truncated bike routes in cpcpostgis1"
        file.write("Truncated bike routes in cpcpostgis1" + "\n")
        arcpy.Append_management("C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp", gis_db_gisdata_Transport_Bike_Routes, "NO_TEST", "from_stree \"from_stree\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,from_stree,-1,-1;date_last_ \"date_last_\" true true false 36 Date 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,date_last_,-1,-1;time_last_ \"time_last_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,time_last_,-1,-1;cnn_id \"cnn_id\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,cnn_id,-1,-1;year_insta \"year_insta\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,year_insta,-1,-1;contraflow \"contraflow\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,contraflow,-1,-1;install_ye \"install_ye\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_ye,-1,-1;full_stree \"full_stree\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,full_stree,-1,-1;double_ \"double_\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,double,-1,-1;direction \"direction\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,direction,-1,-1;fiscal_qua \"fiscal_qua\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_qua,-1,-1;fiscal_yea \"fiscal_yea\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,fiscal_yea,-1,-1;bike_route \"bike_route\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,bike_route,-1,-1;object_id \"object_id\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,object_id,-1,-1;sharrow \"sharrow\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,sharrow,-1,-1;barrier_ty \"barrier_ty\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,barrier_ty,-1,-1;number_of_ \"number_of_\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,number_of_,-1,-1;length_fee \"length_fee\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_fee,-1,-1;length_mil \"length_mil\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,length_mil,-1,-1;to_street \"to_street\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,to_street,-1,-1;street_typ \"street_typ\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_typ,-1,-1;small_stre \"small_stre\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,small_stre,-1,-1;surface_tr \"surface_tr\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,surface_tr,-1,-1;install_mo \"install_mo\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,install_mo,-1,-1;facility_t \"facility_t\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,facility_t,-1,-1;innovative \"innovative\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,innovative,-1,-1;last_upgra \"last_upgra\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,last_upgra,-1,-1;street_nam \"street_nam\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,street_nam,-1,-1;notes \"notes\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,notes,-1,-1;update_mon \"update_mon\" true true false 8 Double 8 38 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working\\BikeRoutesDataSF\\BikeRoutesDataSF.shp,update_mon,-1,-1;st_length(shape) \"st_length(shape)\" false false true 0 Double 0 0 ,First,#", "")
        print "Appended bike routes in cpcpostgis1"
        file.write("Appended bike routes in cpcpostgis1" + "\n")
    except:
        print "FAILED to append bike routes in cpcpostgis1"
        file.write("FAILED to append bike routes in cpcpostgis1" + "\n")

    


    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    file.write(str(time.ctime()) +"/nFINISHED SUCCESSFULLY")
    file.close()

except Exception,e:
  print "Ended badly"
  file.write(str(time.ctime()) +": Ended badly\n")
  
  print str(e)
  file.write(str(e))
  myEndTime = time.clock()
  theTime = myEndTime - myStartTime
  theEndTime = time.ctime()
  theMinutes = theTime / 60
  print theEndTime 
  print "Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)"
  file.write(str(time.ctime()) +": Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)")
  file.close()

