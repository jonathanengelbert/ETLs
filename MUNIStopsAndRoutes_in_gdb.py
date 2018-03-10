# This script copies and processes MUNI stops and routes data from DataSF.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
###WARNING: Depends on "working" folder and "working.gdb" located at C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\
################################################################################################


import arcpy, time, datetime, os
arcpy.env.overwriteOutput=True


# Local variables:
Stops = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops"
Routes = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Routes"

MUNIStops_shp_Staging = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops"
MUNIRoutes_shp_Staging = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes"
MUNIStops_Buffer_shp_Staging = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops_Buffer"
MUNIRoutes_Buffer_shp_Staging = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes_Buffer"
working_gdb = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb"

myStartDate = str(datetime.date.today())
myStartTime = time.clock()
logfile = open("C:/ETLs/TIM/TIMUpdates/Logs/"+ myStartDate + "MUNIStopsAndRoutes.txt", "w")
logfile.write(str(time.ctime()) +": Started" + "\n")

################################################################################################

#STEP ONE 
#QUERIES, COPIES AND MODIFYING PATHS

try:
    
    # Modify the following variables:
    # URL to your service, where clause, fields and token if applicable
    stopsURL= "http://services.sfmta.com/arcgis/rest/services/Transit/transit/FeatureServer/2/query"
    routesURL= "http://services.sfmta.com/arcgis/rest/services/Transit/transit/FeatureServer/9/query"
    where = '1=1'
    token = ''

    stopsFields ='STOPNAME,TRAPEZESTOPABBR,RUCUSSTOPABBR,STOPID,LATITUDE,LONGITUDE,ACCESSIBILITYMASK,ATSTREET,ONSTREET,POSITION,ORIENTATION,SERVICEPLANNINGSTOPTYPE,SHELTER'
    routesFields ='PATTERNID,ROUTE_NAME,DIRECTION,PATTERN_TY,SUB_TYPE,PATTERN_VE,LINEABBR,SIGNID,SERVICE_CA'
 
    #The above variables construct the query
    where='STOPID>7000'
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, stopsFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fsURL = stopsURL + query
    print fsURL
    fs = arcpy.FeatureSet()
    fs.load(fsURL)
    arcpy.CopyFeatures_management(fs, r"c:\ETLS\TIM\TIMUpdates\working.gdb\Stops1")
    print "Copied stops1"
    
    where='STOPID>6000+and+STOPID<=7000'
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, stopsFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fsURL = stopsURL + query
    fs = arcpy.FeatureSet()
    fs.load(fsURL)
    arcpy.CopyFeatures_management(fs, r"c:\ETLS\TIM\TIMUpdates\working.gdb\Stops2")
    print "Copied stops2"

    where='STOPID>5000+and+STOPID<=6000'
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, stopsFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fsURL = stopsURL + query
    fs = arcpy.FeatureSet()
    fs.load(fsURL)
    
    arcpy.CopyFeatures_management(fs, r"c:\ETLS\TIM\TIMUpdates\working.gdb\Stops3")
    print "Copied stops3"

    
    where='STOPID>4000+and+STOPID<=5000'
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, stopsFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fsURL = stopsURL + query
    fs = arcpy.FeatureSet()
    fs.load(fsURL)
    
    arcpy.CopyFeatures_management(fs, r"c:\ETLS\TIM\TIMUpdates\working.gdb\Stops4")
    print "Copied stops4"

    where='STOPID<=4000'
    query = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, stopsFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fsURL = stopsURL + query
    fs = arcpy.FeatureSet()
    fs.load(fsURL)
    
    logfile.write(str(time.ctime()) +": Copied Stops to Local" + "\n")
    arcpy.CopyFeatures_management(fs, r"c:\ETLS\TIM\TIMUpdates\working.gdb\Stops5")
    print "Copied stops5"

   ################################################################################################

    #STEP TWO 
	#GEOPROCESSING   

    # Local variables:
    Stops = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops"
    Stops1 = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1"
    Stops2 = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2"
    Stops3 = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3"
    Stops4 = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4"
    Stops5 = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5"

    # Process: Merge
    arcpy.Merge_management("C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1;C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2;C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3;C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4;C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5", Stops, "STOPNAME \"STOPNAME\" true true false 100 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,STOPNAME,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,STOPNAME,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,STOPNAME,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,STOPNAME,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,STOPNAME,-1,-1;TRAPEZESTOPABBR \"TRAPEZESTOPABBR\" true true false 8 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,TRAPEZESTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,TRAPEZESTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,TRAPEZESTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,TRAPEZESTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,TRAPEZESTOPABBR,-1,-1;RUCUSSTOPABBR \"RUCUSSTOPABBR\" true true false 30 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,RUCUSSTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,RUCUSSTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,RUCUSSTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,RUCUSSTOPABBR,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,RUCUSSTOPABBR,-1,-1;STOPID \"STOPID\" true true false 8 Double 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,STOPID,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,STOPID,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,STOPID,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,STOPID,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,STOPID,-1,-1;LATITUDE \"LATITUDE\" true true false 8 Double 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,LATITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,LATITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,LATITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,LATITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,LATITUDE,-1,-1;LONGITUDE \"LONGITUDE\" true true false 8 Double 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,LONGITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,LONGITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,LONGITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,LONGITUDE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,LONGITUDE,-1,-1;ACCESSIBILITYMASK \"ACCESSIBILITYMASK\" true true false 8 Double 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,ACCESSIBILITYMASK,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,ACCESSIBILITYMASK,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,ACCESSIBILITYMASK,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,ACCESSIBILITYMASK,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,ACCESSIBILITYMASK,-1,-1;ATSTREET \"ATSTREET\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,ATSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,ATSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,ATSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,ATSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,ATSTREET,-1,-1;ONSTREET \"ONSTREET\" true true false 254 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,ONSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,ONSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,ONSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,ONSTREET,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,ONSTREET,-1,-1;POSITION \"POSITION\" true true false 30 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,POSITION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,POSITION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,POSITION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,POSITION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,POSITION,-1,-1;ORIENTATION \"ORIENTATION\" true true false 30 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,ORIENTATION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,ORIENTATION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,ORIENTATION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,ORIENTATION,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,ORIENTATION,-1,-1;SERVICEPLANNINGSTOPTYPE \"SERVICEPLANNINGSTOPTYPE\" true true false 30 Text 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,SERVICEPLANNINGSTOPTYPE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,SERVICEPLANNINGSTOPTYPE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,SERVICEPLANNINGSTOPTYPE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,SERVICEPLANNINGSTOPTYPE,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,SERVICEPLANNINGSTOPTYPE,-1,-1;SHELTER \"SHELTER\" true true false 8 Double 0 0 ,First,#,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops1,SHELTER,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops2,SHELTER,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops3,SHELTER,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops4,SHELTER,-1,-1,C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Stops5,SHELTER,-1,-1")
    logfile.write(str(time.ctime()) +": Merged Stops to Local" + "\n")
    print "Merged Stops to Local"



    #The above variables construct the query
    where='1=1'
    query2 = "?where={}&outFields={}&returnGeometry=true&f=json&token={}".format(where, routesFields, token)
    # See http://services1.arcgis.com/help/index.html?fsQuery.html for more info on FS-Query
    fs2URL = routesURL + query2
    fs2 = arcpy.FeatureSet()
    fs2.load(fs2URL)

    arcpy.CopyFeatures_management(fs2, r"c:\ETLS\TIM\TIMUpdates\working.gdb\RoutesTemp")
    print "Copied routes temp"
    logfile.write(str(time.ctime()) +": Copied RoutesTemp to Local" + "\n")

    RoutesTemp = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\RoutesTemp"
    Routes = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Routes"

    # Process: Dissolve
    arcpy.Dissolve_management(RoutesTemp, Routes, "ROUTE_NAME;SERVICE_CA", "", "MULTI_PART", "DISSOLVE_LINES")

    print "Dissolved Routes"
    logfile.write(str(time.ctime()) +": Dissolved Routes" + "\n")

    

    arcpy.Compact_management(working_gdb)
    print "Compacted fgdb"
    logfile.write(str(time.ctime()) +": Compacted fgdb" + "\n")

    # Process: Project
    arcpy.Project_management(Stops, MUNIStops_shp_Staging, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "WGS_1984_(ITRF08)_To_NAD_1983_2011", "PROJCS['NAD_1983_2011_StatePlane_California_III_FIPS_0403_Ft_US',GEOGCS['GCS_NAD_1983_2011',DATUM['D_NAD_1983_2011',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',6561666.666666666],PARAMETER['False_Northing',1640416.666666667],PARAMETER['Central_Meridian',-120.5],PARAMETER['Standard_Parallel_1',37.06666666666667],PARAMETER['Standard_Parallel_2',38.43333333333333],PARAMETER['Latitude_Of_Origin',36.5],UNIT['Foot_US',0.3048006096012192]]", "NO_PRESERVE_SHAPE", "")
    print "projected Stops"
    logfile.write(str(time.ctime()) +": Projected Stops" + "\n")


    # Process: Project (2)
    arcpy.Project_management(Routes, MUNIRoutes_shp_Staging, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "WGS_1984_(ITRF08)_To_NAD_1983_2011", "PROJCS['NAD_1983_2011_StatePlane_California_III_FIPS_0403_Ft_US',GEOGCS['GCS_NAD_1983_2011',DATUM['D_NAD_1983_2011',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',6561666.666666666],PARAMETER['False_Northing',1640416.666666667],PARAMETER['Central_Meridian',-120.5],PARAMETER['Standard_Parallel_1',37.06666666666667],PARAMETER['Standard_Parallel_2',38.43333333333333],PARAMETER['Latitude_Of_Origin',36.5],UNIT['Foot_US',0.3048006096012192]]", "NO_PRESERVE_SHAPE", "")
    print "projected routes"
    logfile.write(str(time.ctime()) +": Projected Routes" + "\n")

    # Process: Buffer
    arcpy.Buffer_analysis(MUNIStops_shp_Staging, MUNIStops_Buffer_shp_Staging, "0.25 Miles", "FULL", "ROUND", "NONE", "", "PLANAR")
    print "Buffered stops"
    logfile.write(str(time.ctime()) +": Buffered Stops" + "\n")

    # Process: Buffer (2)
    arcpy.Buffer_analysis(MUNIRoutes_shp_Staging, MUNIRoutes_Buffer_shp_Staging, "0.25 Miles", "FULL", "ROUND", "NONE", "", "PLANAR")
    print "Buffered routes"
    logfile.write(str(time.ctime()) +": Buffered Routes" + "\n")

    #Now refresh all indexes
    # Local variables:
    MUNIStops_shp = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops"
    MUNIStops_shp__2_ = MUNIStops_shp
    MUNIStops_Buffer_shp = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops_Buffer"
    MUNIStops_Buffer_shp__2_ = MUNIStops_Buffer_shp
    MUNIRoutes_shp = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes"
    MUNIRoutes_shp__2_ = MUNIRoutes_shp
    MUNIRoutes_Buffer_shp = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes_Buffer"
    MUNIRoutes_Buffer_shp__2_ = MUNIRoutes_Buffer_shp
    MUNIStops_shp__5_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops"
    MUNIStops_shp__6_ = MUNIStops_shp__5_
    MUNIStops_shp__4_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops"
    MUNIStops_Buffer_shp__5_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops_Buffer"
    MUNIStops_Buffer_shp__6_ = MUNIStops_Buffer_shp__5_
    MUNIRoutes_shp__5_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes"
    MUNIRoutes_shp__6_ = MUNIRoutes_shp__5_
    MUNIStops_Buffer_shp__4_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIStops.gdb\\MUNIStops_Buffer"
    MUNIRoutes_shp__4_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes"
    MUNIRoutes_Buffer_shp__5_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes_Buffer"
    MUNIRoutes_Buffer_shp__6_ = MUNIRoutes_Buffer_shp__5_
    MUNIRoutes_Buffer_shp__4_ = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\MUNIRoutes.gdb\\MUNIRoutes_Buffer"

    try:
        arcpy.AddSpatialIndex_management(MUNIStops_shp, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMReady MUNI Stops"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMReady MUNI Stops" + "\n")

    try:
       arcpy.AddSpatialIndex_management(MUNIStops_Buffer_shp, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMReady MUNI Stops buffer"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMReady MUNI Stops buffer" + "\n")

    try:
        arcpy.AddSpatialIndex_management(MUNIRoutes_shp, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMReady MUNI Routes"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMReady MUNI Routes" + "\n")
        
    try:
        arcpy.AddSpatialIndex_management(MUNIRoutes_Buffer_shp, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMReady MUNI Routes buffer"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMReady MUNI Routes buffer" + "\n")

    try:
        arcpy.AddSpatialIndex_management(MUNIStops_shp__5_, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMStaging MUNI Stops"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMStaging MUNI Stops" + "\n")

    try:
        arcpy.AddSpatialIndex_management(MUNIStops_Buffer_shp__5_, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMStaging MUNI Stops buffer"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMStaging MUNI Stops buffer" + "\n")

    try:
        arcpy.AddSpatialIndex_management(MUNIRoutes_shp__5_, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMStaging MUNI Routes buffer"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMStaging MUNI Routes buffer" + "\n")
    try:
        arcpy.AddSpatialIndex_management(MUNIRoutes_Buffer_shp__5_, "0", "0", "0")
    except:
        print "FAILED to refresh spatial index for TIMStaging MUNI Routes buffer"
        logfile.write(str(time.ctime()) +": FAILED to refresh spatial index for TIMStaging MUNI Routes buffer" + "\n")
        
	

    logfile.write(str(time.ctime()) +": FINISHED SUCCESSFULLY" + "\n")
    print("FINISHED SUCCESSFULLY")
    
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    print theEndTime 
    print "Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)"
    logfile.write(str(time.ctime()) +" Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)")
    logfile.close()
    
################################################################################################
	
except Exception,e:
    print "Ended badly"
    logfile.write(str(time.ctime()) +": Finished with ERROR! \n\n")
    print str(e)
    logfile.write(str(e)+ "\n")
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    print theEndTime 
    print "Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)"
    logfile.write(str(time.ctime()) +" Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)")
    logfile.close()
    







