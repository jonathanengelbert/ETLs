# This script copies and processes bike parking data from DataSF.

import arcpy, sys, string, os, time, datetime, urllib, urllib2, json
from arcpy import env

# chnage default encoding to read the decoded data from JSON
reload(sys)
sys.setdefaultencoding("utf-8")

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

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
    file = open("C:/ETLs/log/" + myStartDate + "bikeparking" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

    # STEP ONE
    # PULL DATA FROM DATASF API IN JSON TO 

    # filepath for all copied files:
    stagingfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMStaging\\"
    
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

    txtfile = open(stagingfolder + "bikeparking.txt", "w")

    #url='https://data.sfgov.org/resource/w969-5mn4.json$limit=1'
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
        #url='https://data.sfgov.org/resource/w969-5mn4.json?$limit=' + str(thelimit) + '&$offset='+str(theoffset)
        url='https://data.sfgov.org/resource/dd7x-3h4a.json?$limit=' + str(thelimit) + '&$offset='+str(theoffset)
        file.write(url)
        
        d = get_Json(url)
        if d==[]:
            hasMore=False
        else:
            
            #namelist = ["ADDRESS","LOCATION_N","STREET_NAM","RACKS","SPACES","PLACEMENT","YR_INSTALL","LATITUDE","LONGITUDE"]
            namelist = ["address","location","street","number_of_racks","number_of_spaces","placement","year_installed","LATITUDE","LONGITUDE"]
            print "here"
            for f in namelist:
                if f != "LONGITUDE":
                    txtfile.write(str(f.encode('ascii','ignore')).replace("\n","")+'\t')
                else:
                    txtfile.write(str(f.encode('ascii','ignore')).replace("\n",""))
            txtfile.write("\n")
            
            for record in d:
                i=i+1
                
                if 'yr_inst' in record:
                    yr_inst =  str(record['yr_inst'])
                else:
                    yr_inst=""
                
                if 'addr_num' in record:
                    addr_num =str(record['addr_num'])
                else:
                    addr_num=""
                    
                if 'street_name' in record:
                    street_name =  str(record['street_name'])
                else:
                    street_name=""
                    
                if 'racks' in record:
                    racks =str(record['racks'])
                else:
                    racks=""
                    
                if 'spaces' in record:
                    spaces =str(record['spaces'])
                else:
                    spaces=""
                    
                if 'placement' in record:
                    placement =str(record['placement'])
                else:
                    placement=""
                
                if 'yr_installed' in record:
                    yr_installed =str(record['yr_installed'])
                else:
                    yr_installed=""
                    
                if 'latitude' in record:
                    theLat=""
                    theLon=""
                    if 'latitude' in  record['latitude']:
                        if 'longitude' in  record['latitude']:
                            theLat = record['latitude']['latitude']
                            theLon = record['latitude']['longitude']
                
                fieldlist = [yr_inst,addr_num,street_name,racks,spaces,placement,yr_installed,theLat,theLon]
                
                for f in fieldlist:
                    if f != theLon:
                        txtfile.write(str(f.encode('ascii','ignore')).replace("\n","")+'\t')
                    else:
                        txtfile.write(str(f.encode('ascii','ignore')).replace("\n",""))

                txtfile.write("\n")
        
        k=k+1

    print i
    txtfile.close()
    
    # convert to DBF 
    # print "Converting to DBF"
    # dbf_table = "bikeparking.dbf"
    # arcpy.TableToTable_conversion (txtfile, stagingfolder, dbf_table, "", "", "")
    
    table = stagingfolder + "bikeparking.txt"
    
    # create XY event layer
    print "Converting to XY Event Layer"
    eventlayer = "bikeparking_event"
    local1 = "bikeparking_1.shp"
    arcpy.MakeXYEventLayer_management(table, "longitude", "latitude", local1, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision", "")
    file.write(str(time.ctime()) +": made XY Event Layer"+ "\n")
    # STEP TWO
    # GEOPROCESSING

    # Project both layers
    
    print "Reprojecting"
    local2 = "BikeParking.shp"
    webmercator = arcpy.SpatialReference(3857)
    arcpy.Project_management(local1, stagingfolder + local2, webmercator)
    print "GC reprojected to " + local2
    file.write(str(time.ctime()) +": reprojected"+ "\n")
    
  
    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        # bufferlist.append(buffer_name)
        staging_name = stagingfolder + original_name
        filename_buffer = stagingfolder + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)

        
    # Buffer BSP
    buffername = "BikeParkingBuffer250ft.shp"
    arcpybuffer(buffername,local2,"250 Feet","","")
    
    
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
    deleteandappend(local2)
    file.write(str(time.ctime()) +": appended local"+ "\n")
    
    # delete and append buffer layers
    deleteandappend(buffername)
    file.write(str(time.ctime()) +": appended buffer"+ "\n")
    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()
    
except Exception,e:
    print str(e)
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
