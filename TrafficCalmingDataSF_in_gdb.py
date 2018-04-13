<<<<<<< HEAD:TrafficCalmingDataSF_in_gdb.py
# This script copies and processes traffic calming data from DataSF.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
################################################################################################

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
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "TrafficCalming" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

################################################################################################

    # STEP ONE
    # PULL SHAPEFILE FROM DATASF

    import glob, os
    test = '/path/*'
    r = glob.glob(test)
    for i in r:
       os.remove(i)

    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\TrafficCalming.gdb\\"
    tempzipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    
    zipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    zip_temp = zipfolder + 'TC.zip'

    dl_link= "https://data.sfgov.org/api/geospatial/ddye-rism?method=export&format=Shapefile"
    response = urllib2.urlopen(dl_link)

    output = open(zip_temp,'wb')
    output.write(response.read())
    output.close()
    zf = ZipFile(zip_temp, 'r')
    print zf.namelist()
    tempfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\TC\\"
    #Clear out the folder
    import glob, os
    test = tempfolder+'/*'
    r = glob.glob(test)
    for i in r:
       os.remove(i)
    #Extract new versions
    zf.extractall(tempfolder)

    newstring = "TC_1"

    for filename in os.listdir(tempfolder):
        exten = os.path.splitext(filename) # filename and extensionname (extension in [1])
        os.rename(tempfolder + filename, tempfolder + newstring + exten[1])
    
    temp_shp = tempfolder + newstring + ".shp"
    print temp_shp

################################################################################################        
    
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    try:
        local2 = "Traffic_Calming"
        print "try to reproject " + temp_shp + " to " + staging_gdb + local2
        webmercator = arcpy.SpatialReference(3857)
        arcpy.Project_management(temp_shp, staging_gdb + local2, webmercator)
        print "Reprojected to " + local2
        file.write(str(time.ctime()) +": Reprojected"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO REPROJECT"+ "\n")
        print "FAILED TO REPROJECT"
        print arcpy.GetMessages()

    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "trafficcalmingbuffer"
    arcpybuffer(buffername,local2,"250 Feet","","")
    
    
    print("FINISHED SUCCESFULLY")

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

################################################################################################    
    
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
=======
# This script copies and processes traffic calming data from DataSF.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
################################################################################################

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
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "TrafficCalming" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

################################################################################################

    # STEP ONE
    # PULL SHAPEFILE FROM DATASF

    import glob, os
    test = '/path/*'
    r = glob.glob(test)
    for i in r:
       os.remove(i)

    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\TrafficCalming.gdb\\"
    tempzipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    
    zipfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\"
    zip_temp = zipfolder + 'TC.zip'

    dl_link= "https://data.sfgov.org/api/geospatial/ddye-rism?method=export&format=Shapefile"
    response = urllib2.urlopen(dl_link)

    output = open(zip_temp,'wb')
    output.write(response.read())
    output.close()
    zf = ZipFile(zip_temp, 'r')
    print zf.namelist()
    tempfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\zip\\TC\\"
    #Clear out the folder
    import glob, os
    test = tempfolder+'/*'
    r = glob.glob(test)
    for i in r:
       os.remove(i)
    #Extract new versions
    zf.extractall(tempfolder)

    newstring = "TC_1"

    for filename in os.listdir(tempfolder):
        exten = os.path.splitext(filename) # filename and extensionname (extension in [1])
        os.rename(tempfolder + filename, tempfolder + newstring + exten[1])
    
    temp_shp = tempfolder + newstring + ".shp"
    print temp_shp

################################################################################################        
    
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    try:
        local2 = "Traffic_Calming"
        print "try to reproject " + temp_shp + " to " + staging_gdb + local2
        webmercator = arcpy.SpatialReference(3857)
        arcpy.Project_management(temp_shp, staging_gdb + local2, webmercator)
        print "Reprojected to " + local2
        file.write(str(time.ctime()) +": Reprojected"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO REPROJECT"+ "\n")
        print "FAILED TO REPROJECT"
        print arcpy.GetMessages()

    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "trafficcalmingbuffer"
    arcpybuffer(buffername,local2,"250 Feet","","")
    
    
    print("FINISHED SUCCESFULLY")

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

################################################################################################    
    
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
>>>>>>> 1f289a2542687ec384d7420d6d2e26d6ba66bc07:TrafficCalmingDataSF_in_gdb.py
