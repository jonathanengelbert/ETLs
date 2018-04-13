<<<<<<< HEAD:BikeparkingDataSF_in_gdb.py
# This script copies and processes bike parking data from DataSF.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
################################################################################################

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json
from arcpy import env

# change default encoding to read the decoded data from JSON
#reload(sys)
#sys.setdefaultencoding("utf-8")

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print str(theStartTime)



try:
    myStartDate = str(datetime.date.today())
    myStartTime = time.clock()
    theStartTime = time.ctime()
    # thisfile = os.path.realpath(__file__)
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "bikeparking" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay



################################################################################################

    # STEP ONE
    # PULL DATA FROM DATASF API IN JSON  

    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeParking.gdb\\"
    
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

    txtfile = open(staging_gdb + "bikeparking2.txt", "w")
    url='https://data.sfgov.org/resource/dd7x-3h4a.json$limit=1'

    i=0
    thelimit=10000
    k=0
    hasMore=True

    while hasMore:
        if k>0:
            theoffset=k*thelimit
        else:
            theoffset=0
        url='https://data.sfgov.org/resource/dd7x-3h4a.json?$limit=' + str(thelimit) + '&$offset='+str(theoffset)
        file.write(url)
        
        d = get_Json(url)
        if d==[]:
            hasMore=False
        else:
            
            namelist = ["ADDRESS","LOCATION_N","RACKS","SPACES","PLACEMENT","yr_install", "LATITUDE","LONGITUDE"]
            print "here"
            for f in namelist:
                if f != "LONGITUDE":
                    txtfile.write(str(removeNonAscii(f)+'\t'))
                else:
                    txtfile.write(str(removeNonAscii(f) ))
            txtfile.write("\n")
            
            for record in d:
                i=i+1
                theLat=""
                theLon=""
                
                if 'addr_num' in record:
                    addr_num =str(record['addr_num'])
                else:
                    addr_num=""

                if 'address' in record:
                    address =str(record['address'])
                else:
                    address=""
                    
                if 'street_name' in record:
                    street_name =  str(record['street_name'])
                else:
                    street_name=""
                    
                if 'number_of_racks' in record:
                    racks =str(record['number_of_racks'])
                else:
                    racks=""
                    
                if 'number_of_spaces' in record:
                    spaces =str(record['number_of_spaces'])
                else:
                    spaces=""

                if 'location' in record:
                    location =str(removeNonAscii(record['location']))
                else:
                    location=""
                    
                if 'placement' in record:
                    placement =str(record['placement'])
                else:
                    placement=""
                
                if 'year_installed' in record:
                    yrinstall =str(record['year_installed']) + " " 
                else:
                    yrinstall=""
                    
                if 'geom' in record:
                    if 'coordinates' in  record['geom']:
                        #print "here"
                        #theLat=99999
                        theLat = str(record['geom']['coordinates'][1])
                        theLon = str(record['geom']['coordinates'][0])
                        #print str(theLat) + " " + str(theLon)
                
                
                fieldlist = [address,location,racks,spaces,placement,yrinstall,theLat,theLon]
                
                for f in fieldlist:
                    if f != theLon:
                        #print i
                        txtfile.write(str(f)+'\t')
                        #txtfile.write(str(f.replace("\n","")+'\t'))
                    else:
                        #print i
                        txtfile.write(str(f))

                txtfile.write("\n")
        
        k=k+1

    print i
    txtfile.close()
    
    table = staging_gdb + "bikeparking2.txt"
    
    # create XY event layer
    print "Converting to XY Event Layer"
    eventlayer = "bikeparking_event"
    local1 = "bikeparking_1"
    arcpy.MakeXYEventLayer_management(table, "LONGITUDE", "LATITUDE", local1, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision", "")
    file.write(str(time.ctime()) +": made XY Event Layer"+ "\n")
    print "made XY event"

################################################################################################

    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting"
    local2 = "BikeParking"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(local1, staging_gdb + local2, webmercator)
    print "GC reprojected to " + local2
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
    buffername = "BikeParkingBuffer250ft"
    arcpybuffer(buffername,local2,"250 Feet","","")
    
    print("FINISHED SUCCESSFULLY")    

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

################################################################################################
    
except Exception,e:
    print "Ended badly"
    file.write(str(time.ctime()) +": Ended badly")
    
    print str(e)
    file.write(str(e))
    file.close()
    
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
# This script copies and processes bike parking data from DataSF.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
################################################################################################

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json
from arcpy import env

# change default encoding to read the decoded data from JSON
#reload(sys)
#sys.setdefaultencoding("utf-8")

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print str(theStartTime)



try:
    myStartDate = str(datetime.date.today())
    myStartTime = time.clock()
    theStartTime = time.ctime()
    # thisfile = os.path.realpath(__file__)
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "bikeparking" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay



################################################################################################

    # STEP ONE
    # PULL DATA FROM DATASF API IN JSON  

    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\BikeParking.gdb\\"
    
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

    txtfile = open(staging_gdb + "bikeparking2.txt", "w")
    url='https://data.sfgov.org/resource/dd7x-3h4a.json$limit=1'

    i=0
    thelimit=10000
    k=0
    hasMore=True

    while hasMore:
        if k>0:
            theoffset=k*thelimit
        else:
            theoffset=0
        url='https://data.sfgov.org/resource/dd7x-3h4a.json?$limit=' + str(thelimit) + '&$offset='+str(theoffset)
        file.write(url)
        
        d = get_Json(url)
        if d==[]:
            hasMore=False
        else:
            
            namelist = ["ADDRESS","LOCATION_N","RACKS","SPACES","PLACEMENT","yr_install", "LATITUDE","LONGITUDE"]
            print "here"
            for f in namelist:
                if f != "LONGITUDE":
                    txtfile.write(str(removeNonAscii(f)+'\t'))
                else:
                    txtfile.write(str(removeNonAscii(f) ))
            txtfile.write("\n")
            
            for record in d:
                i=i+1
                theLat=""
                theLon=""
                
                if 'addr_num' in record:
                    addr_num =str(record['addr_num'])
                else:
                    addr_num=""

                if 'address' in record:
                    address =str(record['address'])
                else:
                    address=""
                    
                if 'street_name' in record:
                    street_name =  str(record['street_name'])
                else:
                    street_name=""
                    
                if 'number_of_racks' in record:
                    racks =str(record['number_of_racks'])
                else:
                    racks=""
                    
                if 'number_of_spaces' in record:
                    spaces =str(record['number_of_spaces'])
                else:
                    spaces=""

                if 'location' in record:
                    location =str(removeNonAscii(record['location']))
                else:
                    location=""
                    
                if 'placement' in record:
                    placement =str(record['placement'])
                else:
                    placement=""
                
                if 'year_installed' in record:
                    yrinstall =str(record['year_installed']) + " " 
                else:
                    yrinstall=""
                    
                if 'geom' in record:
                    if 'coordinates' in  record['geom']:
                        #print "here"
                        #theLat=99999
                        theLat = str(record['geom']['coordinates'][1])
                        theLon = str(record['geom']['coordinates'][0])
                        #print str(theLat) + " " + str(theLon)
                
                
                fieldlist = [address,location,racks,spaces,placement,yrinstall,theLat,theLon]
                
                for f in fieldlist:
                    if f != theLon:
                        #print i
                        txtfile.write(str(f)+'\t')
                        #txtfile.write(str(f.replace("\n","")+'\t'))
                    else:
                        #print i
                        txtfile.write(str(f))

                txtfile.write("\n")
        
        k=k+1

    print i
    txtfile.close()
    
    table = staging_gdb + "bikeparking2.txt"
    
    # create XY event layer
    print "Converting to XY Event Layer"
    eventlayer = "bikeparking_event"
    local1 = "bikeparking_1"
    arcpy.MakeXYEventLayer_management(table, "LONGITUDE", "LATITUDE", local1, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision", "")
    file.write(str(time.ctime()) +": made XY Event Layer"+ "\n")
    print "made XY event"

################################################################################################

    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting"
    local2 = "BikeParking"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(local1, staging_gdb + local2, webmercator)
    print "GC reprojected to " + local2
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
    buffername = "BikeParkingBuffer250ft"
    arcpybuffer(buffername,local2,"250 Feet","","")
    
    print("FINISHED SUCCESSFULLY")    

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

################################################################################################
    
except Exception,e:
    print "Ended badly"
    file.write(str(time.ctime()) +": Ended badly")
    
    print str(e)
    file.write(str(e))
    file.close()
    
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
>>>>>>> 1f289a2542687ec384d7420d6d2e26d6ba66bc07:BikeparkingDataSF_in_gdb.py
