# Import arcpy and other modules
import arcpy
from arcpy import env
import sys, string, os, time, datetime
import csv

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

#Create Directory file path
root = "C:/ETLs/TIM/TIMUpdates/"

# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print theStartTime
file = open(root + "Logs/" + myStartDate + "keywalkingstreets"+ ".txt", "w")
file.write(theStartTime + "\n")
when =datetime.date.today()
theDate = when.strftime("%d")
theDay=when.strftime("%A")
print theDay

try:
	#NOTE: raw data was downloaded via email from Paul Chasan on June 20, 2017.
	#raw_layer = root + "Raw_Data/KeyWalkingStreets.shp"
	KeyWalkingStreets_shp = "C:\\ETLs\\TIM\\TIMUpdates\\Raw_Data\\KeyWalkingStreets.shp"
	key_walking_streets_dissolve = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\Ped_Network.gdb\\key_walking_streets_dissolve"
	key_walking_streets_proj = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\Ped_Network.gdb\\key_walking_streets_proj"
	key_walking_streets_buffer = "C:\\arcgisserver\\DataAndMXDs\\TIMReady\\Ped_Network.gdb\\key_walking_streets_buffer"
	
	
	#Dissolve
	print("\n")
	print "Dissolving "
	#arcpy.Dissolve_management(raw_layer, dissolve_layer, "street_nam", "", "MULTI_PART", "DISSOLVE_LINES")
	arcpy.Dissolve_management(KeyWalkingStreets_shp, key_walking_streets_dissolve, "street_nam", "", "MULTI_PART", "DISSOLVE_LINES")
	print "Dissolved"
	file.write("Dissolved" + "\n")
	
	#Project
	print("\n")
	print "Projecting "
	arcpy.Project_management(key_walking_streets_dissolve, key_walking_streets_proj, "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", "WGS_1984_(ITRF00)_To_NAD_1983", "PROJCS['NAD_1983_StatePlane_California_III_FIPS_0403_Feet',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',6561666.666666666],PARAMETER['False_Northing',1640416.666666667],PARAMETER['Central_Meridian',-120.5],PARAMETER['Standard_Parallel_1',37.06666666666667],PARAMETER['Standard_Parallel_2',38.43333333333333],PARAMETER['Latitude_Of_Origin',36.5],UNIT['Foot_US',0.3048006096012192]]", "NO_PRESERVE_SHAPE", "")
	print "Projected"
	file.write("Projected" + "\n")
	
	#Buffer
	print("\n")
	print "Buffering "
	arcpy.Buffer_analysis(key_walking_streets_proj, key_walking_streets_buffer, "250 Feet", "FULL", "ROUND", "NONE", "", "PLANAR")
	print "Buffered"
	file.write("Buffered" + "\n")
		
	file.write(str(time.ctime()) +": Time Ended")
	file.close()
	
except Exception,e:
	print "Ended badly"
	print str(e)
	print arcpy.GetMessages()
	file.write(str(e) + "\n")
	#file.write(arcpy.GetMessages() + "\n") I don't totally understand what arcpy.GetMessages() does
	file.write(str(time.ctime()) +": Ended badly")
	file.close()
