# This script copies and processes Bay Area Bike share data from the API.

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json
from urllib2 import Request, urlopen
from arcpy import env

print "Starting"

# change default encoding to read the decoded data from JSON
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
    file = open("C:/ETLs/TIM/TIMUpdates/logs/" + myStartDate + "BABS" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

    # STEP ONE
    # COPYING FROM SDE TO LOCAL STAGING FOLDER: SET NAMES AND PATHS

    # filepath for all copied files:
    stagingfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMStaging\\"
    stagingfile_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMStaging\\bikeshare.gdb\\"
    
    # change default encoding to read the decoded data from JSON
    #reload(sys)
    #sys.setdefaultencoding("utf-8")

    # SET TO OVERWRITE
    arcpy.env.overwriteOutput = True

    def file_get_contents(filename):
            with open(filename) as f:
                return f.read()

    def get_Json(jsonurl):
        req = urllib2.Request(jsonurl, params, headers)
        response = urllib2.urlopen(req)
        the_page = response.read()
        the_page=the_page.decode("ISO-8859-1")
        d = json.loads(the_page)
        return d

    contentType = 'application/json'
    headers = { 'Content-Type' : contentType }

    #values = {"module": "Planning" }
    #params=json.dumps(values)
    #Comment out the next line to switch to POST
    params=None

    txtfile = open(stagingfolder + "BABS.csv", "w")

    url='http://www.bayareabikeshare.com/stations/json'
    url='http://feeds.bayareabikeshare.com/stations/stations.json'

    print url
    d = get_Json(url)
    namelist = ["city","id","landMark","latitude","location","longitude","stAddress1","stAddress2","stationName","statusKey","statusValue","testStation","totalDocks","timestamp"]

    for f in namelist:
        if f != "timestamp":
            txtfile.write(str(f.encode('ascii','ignore')).replace("\n","")+',')
        else:
            txtfile.write(str(f.encode('ascii','ignore')).replace("\n",""))
            
    txtfile.write("\n")

    timestamp = d['executionTime']
    print timestamp

    i=0
    for record in d["stationBeanList"]:
        
        i=i+1
        
        for f in namelist:
            if f in record:
                value = str(record[f])
            else: value = ""    
            if f != "timestamp":
                txtfile.write(str(value.encode('ascii','ignore')).replace("\n","")+',')
            else:
                txtfile.write(timestamp)

        txtfile.write("\n")

    print str(i) + " records loaded"
    file.write(str(time.ctime()) + ": " + str(i) + " records loaded"+ "\n")
    txtfile.close()
  
    
    # create XY event layer
    print "Converting to XY Event Layer"
    table = stagingfolder + "BABS.csv"
    local1 = "babs_1"
    arcpy.MakeXYEventLayer_management(table, "longitude", "latitude", stagingfile_gdb + local1, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision", "")
    print("Finished with make XY")

   
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting"
    local2 = "BikeShareStations"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(stagingfile_gdb + local1, stagingfile_gdb + local2, webmercator)
    print "GC reprojected to " + local2
    file.write(str(time.ctime()) +": " + " GC reprojected to " + local2+ "\n")

  
  
    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = stagingfile_gdb + original_name
        filename_buffer = stagingfile_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "BikeShareStations_buffer"
    arcpybuffer(buffername,local2,"1000 Feet","","")
    

    
    # STEP FOUR
    # DELETE AND APPEND
    
    ready_folder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\"
    ready_folder_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\bikeshare.gdb\\"

    def deleteandappend(shpname):
        print "Delete and append " + shpname
        live_file = ready_folder_gdb + shpname
        staging_file = stagingfile_gdb + shpname
        arcpy.DeleteRows_management(live_file)
        arcpy.Append_management(staging_file, live_file, "TEST", "", "")
    
    # delete and append feature layers
    deleteandappend(local2)
    file.write(str(time.ctime()) +": deleted and appended features"+ "\n")
    
    # delete and append buffer layers
    deleteandappend(buffername)
    file.write(str(time.ctime()) +": deleted and appended buffer layers"+ "\n")
    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

    # Local variables:
    BikeShareStations_buffer = "C:\\arcgisserver\\DataAndMXDs\\TIMStaging\\bikeshare.gdb\\BikeShareStations_buffer"  
    BikeShareStations_buffer__2_ = BikeShareStations_buffer

    # Process: Add Spatial Index
    arcpy.AddSpatialIndex_management(BikeShareStations_buffer, "610", "0", "0")

    # Local variables:
    bikeshare_gdb = "C:\\arcgisserver\\DataAndMXDs\\TIMStaging\\bikeshare.gdb"
    bikeshare_gdb__2_ = bikeshare_gdb

    # Process: Compact
    arcpy.Compact_management(bikeshare_gdb)


    
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


