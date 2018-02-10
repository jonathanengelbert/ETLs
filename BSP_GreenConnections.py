# this script pulls Better Streets plan and Green Connections from the Planning SDE into TIM.

import arcpy
from arcpy import env
import sys, string, os, time, datetime

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print theStartTime

try:
    myStartDate = str(datetime.date.today())
    myStartTime = time.clock()
    theStartTime = time.ctime()
    # thisfile = os.path.realpath(__file__)
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "BSP_GreenConnections" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay


    # STEP ONE
    # COPYING FROM SDE TO LOCAL STAGING FOLDER: SET NAMES AND PATHS AND COPY

    # filepath for all copied files:
    stagingfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMStaging\\"
    staging_gdb = "C:\\arcgisserver\\DataAndMXDs\\TIMStaging\\GreenConnections.gdb\\"
    
    # better streets plan
    try:
        bsp_sde = "Database Connections\\CITYPLAN-03SDE_Service_GISData.sde\\GISDATA.BetterStreetsPlan"
        bsp_local_1 = "bsp_1"
        arcpy.CopyFeatures_management(bsp_sde, staging_gdb + bsp_local_1)
        print "BSP copied from SDE to staging folder"
        file.write(str(time.ctime()) +": copied files - BSP"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO COPY - BSP"+ "\n")
    
    # # better streets plan
    # bsp_sde = "Database Connections\\CITYPLAN-03SDE_Service_GISData.sde\\GISDATA.BetterStreetsPlan"
    # bsp_local_1 = "bsp_1.shp"
    # print "BSP loaded from " + bsp_sde
    # # arcpy.CopyFeatures_management(bsp_sde, stagingfolder + bsp_local_1, "", "0", "0", "0")
    # arcpy.CopyFeatures_management(bsp_sde, stagingfolder + bsp_local_1)
    # print "Saved to " + bsp_local_1
    # file.write(str(time.ctime()) +": copied features - bsp"+ "\n")
    
    # green connections
    gc_sde = "Database Connections\\CITYPLAN-03SDE_Service_GISData.sde\\GISDATA.GreenConnectionsNetwork"
    print "Green Connections loaded from " + gc_sde
    
    gc_layername = "gc_layer" # make into layer to get selection
    arcpy.MakeFeatureLayer_management (gc_sde, gc_layername,""" "GC_RT_NME5" <> ' ' """, "", "")
    
    gc_local_1 = "gc_1"
    arcpy.CopyFeatures_management(gc_layername, staging_gdb + gc_local_1)
    file.write(str(time.ctime()) +": copied green connections"+ "\n")
    
    print "Saved to " + gc_local_1
    

    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting BSP"
    bsp_local_2 = "bsp_2"
    webmercator = arcpy.SpatialReference(3857) # This is WGS_1984_Web_Mercator_Auxiliary_Sphere WKID: 3857 Authority: EPSG
    arcpy.Project_management(staging_gdb + bsp_local_1, staging_gdb + bsp_local_2, webmercator)
    print "BSP reprojected to " + bsp_local_2
    file.write(str(time.ctime()) +": projected1"+ "\n")
    
    print "Reprojecting GC"
    gc_local_2 = "gc_2"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(staging_gdb + gc_local_1, staging_gdb + gc_local_2, webmercator)
    print "GC reprojected to " + gc_local_2
    file.write(str(time.ctime()) +": projected2"+ "\n")
    
    # process BSP data    
    # load table of sidewalk widths
    BSP_sidewalks = "C:\\arcgisserver\\DataAndMXDs\\TIMStaging\\BSP_sidewalks_OID.dbf"
    print "BSP table loaded"
    
    # add field for final street type
    arcpy.AddField_management(staging_gdb + bsp_local_2, "finaltype", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
    print "BSP fields added"
    file.write(str(time.ctime()) +": BSP fields added"+ "\n")
    
    # Process: Calculate Field
    arcpy.CalculateField_management(staging_gdb + bsp_local_2, "finaltype", "bsp( !BSP_Class!, !Special!)", "PYTHON", "def bsp(BSP_Class,Special):\\n	if Special != 'N':\\n		return Special\\n	else:\\n		return BSP_Class")
    print "BSP fields calculated"
    file.write(str(time.ctime()) +": BSP field calc"+ "\n")
    
    # Dissolve BSP (dissolve before join to make join faster)
    bsp_local_3 = "BetterStreetsPlan_TIM"
    arcpy.Dissolve_management(staging_gdb + bsp_local_2, staging_gdb + bsp_local_3, ["STREETNAME","BSP_Class","Special","finaltype"], "", "", "")
    print "BSP dissolved" 
    print "BSP joining fields..."
    file.write(str(time.ctime()) +": dissolved"+ "\n")
    
    # Join fields from sidewalk table
    arcpy.JoinField_management(staging_gdb + bsp_local_3, "finaltype", BSP_sidewalks, "finaltype", "side_min;side_rec")
    print "BSP fields joined"
    file.write(str(time.ctime()) +": joined"+ "\n")
    
    # process Green Connections
    # Dissolve Green Connections
    gc_local_3 = "GreenConnectionsTIM_dissolve"
    arcpy.Dissolve_management(staging_gdb + gc_local_2, staging_gdb + gc_local_3, ["STREETNAME","GC_RT_NME5","GC_RT_NUM5"], "", "", "")
    print "GC dissolved"
    file.write(str(time.ctime()) +": dissolved 2"+ "\n")
    
    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    
    arcpybuffer("betterstreetsbuffer",bsp_local_3,"250 Feet","","")
    file.write(str(time.ctime()) +": buffered 1"+ "\n")
    
    # Buffer GC
    arcpybuffer("greenconnectionsbuffer",gc_local_3,"250 Feet","","")
    file.write(str(time.ctime()) +": buffered2"+ "\n")
    
    # STEP FOUR
    # DELETE AND APPEND
    
    ready_folder = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\GreenConnections.gdb\\"

    def deleteandappend(shpname):
        print "Delete and append " + shpname
        live_file = ready_folder + shpname
        staging_file = staging_gdb + shpname
        arcpy.DeleteRows_management(live_file)
        arcpy.Append_management(staging_file, live_file, "TEST", "", "")
    
    # delete and append feature layers
    deleteandappend(bsp_local_3)
    deleteandappend(gc_local_3)
    
    # delete and append buffer layers
    deleteandappend("betterstreetsbuffer")
    file.write(str(time.ctime()) +": deleted and appended local"+ "\n")
    deleteandappend("greenconnectionsbuffer")
    file.write(str(time.ctime()) +": deleted and appended buffer"+ "\n")
    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()
    
except Exception,e:
    print "Ended badly"
    file.write(str(time.ctime()) +": Ended badly")
    file.write(arcpy.GetMessages())
    file.write(arcpy.GetMessages(2))
    file.write(arcpy.GetMessages(1))
    print str(e)
    file.write(str(e))
    file.close()
    print arcpy.GetMessages()
    print arcpy.GetMessages(2)
    print arcpy.GetMessages(1)
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
    print arcpy.GetMessages(2)
    print arcpy.GetMessages(1)
    myEndTime = time.clock()
    theTime = myEndTime - myStartTime
    theEndTime = time.ctime()
    theMinutes = theTime / 60
