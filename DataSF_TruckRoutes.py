# This script copies and processes truck routes from DataSF.

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json, zipfile
from zipfile import ZipFile
from arcpy import env

# chnage default encoding to read the decoded data from JSON
#reload(sys)
#sys.setdefaultencoding("utf-8")

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
    file = open("C:/ETLs/TIM/Logs/" + myStartDate + "truckroutes" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

    # STEP ONE
    # PULL SHAPEFILE FROM DATASF

    # filepath for all copied files:
    stagingfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\TruckRoutes.gdb\\"
    tempzipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    
    zipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    zip_temp = zipfolder + 'TC.zip'

    #dl_link= "https://data.sfgov.org/download/ce5f-8b7c/SHAPEFILE"
    dl_link= "https://extxfer.sfdph.org/gis/Transportation/SanFranciscoTruckRoutes.zip"
    response = urllib2.urlopen(dl_link)
    output = open(zip_temp,'wb')
    output.write(response.read())
    output.close()

    zf = ZipFile(zip_temp, 'r')
    print zf.namelist()
    tempfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\TruckRoutes\\"
    
    for f in os.listdir(tempfolder):
        print f
        os.remove(tempfolder + f)
    
    zf.extractall(tempfolder)
    file.write(str(time.ctime()) +": Got new files"+ "\n")

    replacestring = "SanFranciscoTruckRoutes"
    newstring = "Truck_1"

    for filename in os.listdir(tempfolder):
        os.rename(tempfolder + filename, tempfolder + filename.replace(replacestring, newstring))
        temp_shp = tempfolder + newstring + ".shp"
    
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting"
    local2 = "SanFranciscoTruckRoutes"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(temp_shp, stagingfolder + local2, webmercator)
    print "GC reprojected to " + local2
    file.write(str(time.ctime()) +": Reprojected"+ "\n")
    
  
    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = stagingfolder + original_name
        filename_buffer = stagingfolder + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "truckroutesbuffer"
    arcpybuffer(buffername,local2,"250 Feet","LIST",["STREETNAME","RouteType"])
    file.write(str(time.ctime()) +": Ran 250ft buffer"+ "\n")
    
    
    # STEP FOUR
    # DELETE AND APPEND
    
    ready_folder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\"

    def deleteandappend(shpname):
        print "Delete and append " + shpname
        live_file = ready_folder + shpname
        staging_file = stagingfolder + shpname
        arcpy.DeleteRows_management(live_file)
        arcpy.Append_management(staging_file, live_file, "TEST", "", "")
    
    # delete and append feature layers
    #deleteandappend(local2)
    #file.write(str(time.ctime()) +": Deleted and appended"+ "\n")
    
    # delete and append buffer layers
    #deleteandappend(buffername)
    #file.write(str(time.ctime()) +": Deleted and appended"+ "\n")
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
