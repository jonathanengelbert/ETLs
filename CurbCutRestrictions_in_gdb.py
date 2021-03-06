#This script processes Curb Cut Restrictions data from SDE. Data is staged and processed before being fed to Curbcuts.gdb
#Last modified: 12/01/2017 by Jonathan Engelbert
#
### No Known Issues
### WARNING: This script points to and depends on  "working.gdb" and a "working" folder in C:\ETLs\TIM\TIMUpdates\
###################################################################################

# Import arcpy module
import arcpy
arcpy.env.overwriteOutput=True


# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print theStartTime


# thisfile = os.path.realpath(__file__)
file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "CurbCuts" + ".txt", "w")
file.write(theStartTime + "\n")

try:
    
    # Local variables:
    gis_db_gisdata_Street_Curb_Cut_Restrictions = "Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions"
    Curb_Cut_Restrictions = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb\\Curb_Cut_Restrictions"
    Curb_Cut_Restrictions_working_gdb = "C:\\ETLs\\TIM\\TIMUpdates\\working.gdb"
    CurbCuts_gdb = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\CurbCuts.gdb"
    CurbCuts_buffer = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\CurbCuts.gdb/CurbCutRestrictions_Buffer"
    CurbCuts_filepath = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\CurbCuts.gdb/CurbCutRestrictions"
    CurbCuts_gdb__2_ = CurbCuts_gdb

    # Process: Project
    arcpy.Project_management(gis_db_gisdata_Street_Curb_Cut_Restrictions, Curb_Cut_Restrictions, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "NAD_1983_To_WGS_1984_1", "PROJCS['NAD_1983_StatePlane_California_III_FIPS_0403_Feet',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',6561666.666666666],PARAMETER['False_Northing',1640416.666666667],PARAMETER['Central_Meridian',-120.5],PARAMETER['Standard_Parallel_1',37.06666666666667],PARAMETER['Standard_Parallel_2',38.43333333333333],PARAMETER['Latitude_Of_Origin',36.5],UNIT['Foot_US',0.3048006096012192]]", "NO_PRESERVE_SHAPE", "")

    try:
        print "Projected"
        file.write(str(time.ctime()) +": Projected"+ "\n")

        # Process: Feature Class to Feature Class
        arcpy.FeatureClassToFeatureClass_conversion(Curb_Cut_Restrictions, CurbCuts_gdb, "CurbCutRestrictions", "", "cnn \"cnn\" true true false 8 Double 8 38 ,First,#,Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions1,cnn,-1,-1;curbcutcode \"curbcutcode\" true true false 50 Text 0 0 ,First,#,Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions1,curbcutcode,-1,-1;code_note \"code_note\" true true false 255 Text 0 0 ,First,#,Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions1,code_note,-1,-1;st_length_shape_ \"st_length_shape_\" true false true 0 Double 0 0 ,First,#,Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions1,st_length_shape_,-1,-1;Shape_length \"Shape_length\" true true false 0 Double 0 0 ,First,#,Database Connections\\cpc-postgis-1_GISData.sde\\gis_db.gisdata.Street_Curb_Cut_Restrictions1,Shape_length,-1,-1", "")
        print "FC to FC"
        file.write(str(time.ctime()) +": FC to FC" + "\n")
    except Exception as e:
        print(e)
    
    # Buffer (in order to include Curb Cut Restrictions in Bike and Ped tab)
    print("\n")
    print "Buffering "
    arcpy.Buffer_analysis(CurbCuts_filepath, CurbCuts_buffer, "250 Feet", "FULL", "ROUND", "NONE", "", "PLANAR")
    print "Projected measures"
    file.write("Buffered Measures" + "\n")

    # Process: Compact
    try:
        arcpy.Compact_management(CurbCuts_gdb)
        print "Compacted Fgdb in TIMReady"
        file.write(str(time.ctime()) +": Compacted Fgdb in TIMReady" + "\n")
    except:
        print "FAILED to Compact Fgdb in TIMReady"
        file.write(str(time.ctime()) +": FAILED to Compact Fgdb in TIMReady" + "\n")

    # Process: Compact
    try:
        arcpy.Compact_management(Curb_Cut_Restrictions_working_gdb)
        print "Compacted working Fgdb"
        file.write(str(time.ctime()) +": Compacted working Fgdb" + "\n")
    except:
        print "FAILED to Compact working Fgdb"
        file.write(str(time.ctime()) +": FAILED to Compact working Fgdb" + "\n")
	

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    myEndTime = time.clock()
    print "Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)"
    file.write(str(time.ctime()) +": Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)")
    
    file.close()

except Exception,e:
    print "Ended badly"
    file.write(str(time.ctime()) +": Ended badly"+ "\n" )
    print str(e)
    file.write(str(e))
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    myEndTime = time.clock()
    print "Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)"
    file.write(str(time.ctime()) +": Ran for a total of " + str(theTime) + " seconds (" + str(theMinutes) + " minutes)")
    file.close()
