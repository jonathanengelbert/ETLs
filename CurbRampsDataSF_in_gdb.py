# This script copies and processes Curb Ramp data from DataSF.
#Last modified: 12/01/2017 by Jonathan Engelbert
#
### No Known Issues
################################################################################################

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json
from urllib2 import Request, urlopen
from arcpy import env

#change default encoding to read the decoded data from JSON
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
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "CurbRamps" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

################################################################################################

    # STEP ONE
    # PULL DATA FROM DATASF API IN JSON  
    
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\CurbRampRestrictions.gdb\\"

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
    params=None

    txtfile = open("\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv", "w")
    i=0
    thelimit=10000
    k=0
    hasMore=True

    while hasMore:
        if k>0:
            theoffset=k*thelimit
        else:
            theoffset=0
        url='https://data.sfgov.org/resource/ch9w-7kih.json?crexist=0&$limit=' + str(thelimit) + '&$offset='+str(theoffset)
        print url
        d = get_Json(url)
        if d==[]:
            hasMore=False
        else:

            if k == 0:
                namelist = ["locationdescription","locid","cnn","positiononreturn","curbreturnloc","latitude","longitude"]
                for f in namelist:
                    if f != "longitude":
                        txtfile.write(str(f.encode('ascii','ignore')).replace("\n","")+',')
                    else:
                        txtfile.write(str(f.encode('ascii','ignore')).replace("\n",""))
                txtfile.write("\n")
            
            for record in d:
                i=i+1
                
                for f in namelist:
                    if f in record:
                        value = str(record[f])
                    else:
                        value = ""
                    if f != "longitude":
                        txtfile.write(str(value.encode('ascii','ignore')).replace("\n","")+',')
                    else:
                        txtfile.write(str(value.encode('ascii','ignore')).replace("\n",""))

                txtfile.write("\n")
        
        k=k+1

    print str(i) + " records saved"
    file.write(str(time.ctime()) +": " + str(i) + " saved records"+ "\n")
    txtfile.close()
    
    # create XY event layer
    try:
        # Local variables:
        missingcurbramps_csv = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv"     
        Output_Location = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\"

    # Process: Table to Table
        arcpy.TableToTable_conversion(missingcurbramps_csv, Output_Location, "missingcurbramps_table", "", "locationdescription \"locationdescription\" true true false 8000 Text 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,locationdescription,-1,-1;locid \"locid\" true true false 8000 Text 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,locid,-1,-1;cnn \"cnn\" true true false 8000 Text 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,cnn,-1,-1;positiononreturn \"positiononreturn\" true true false 8000 Text 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,positiononreturn,-1,-1;curbreturnloc \"curbreturnloc\" true true false 8000 Text 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,curbreturnloc,-1,-1;latitude \"latitude\" true true false 8000 Float 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,latitude,-1,-1;longitude \"longitude\" true true false 8000 Float 0 0 ,First,#,\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps.csv,longitude,-1,-1", "")
        print "Converting to XY Event Layer..."
        
        missingcurbramps_table = "\\\\cp-gis-svr1\\arcgisserver\\DataAndMXDs\\TIMReady\\missingcurbramps_table.dbf"
        
       

# Process: Make XY Event Layer
        arcpy.MakeXYEventLayer_management(missingcurbramps_table, "longitude", "latitude", "templayer", "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;0.001;0.001;IsHighPrecision", "")                              
        print "Converted to event layer"
        file.write(str(time.ctime()) +": converted to event layer"+ "\n")
    except Exception,e:
        print "Ended badly"
        print str(e)
        file.write(str(time.ctime()) +": Ended badly")
################################################################################################
    
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting..."
    local2 = "missingcurbramps"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management("templayer", staging_gdb + local2, webmercator)
    print "Reprojected to " + local2
    file.write(str(time.ctime()) +": reprojected"+ "\n")
    
  
    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "missingcurbramps_buffer"
    arcpybuffer(buffername,local2,"250 Feet","","")
    print "Buffer created"
    file.write(str(time.ctime()) +": buffered"+ "\n")
    
    print("FINISHED SUCCESSFULLY")

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


