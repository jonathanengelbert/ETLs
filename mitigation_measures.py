#This script processes mitigation measures data.
#Last modified: 11/21/2017 by Mike Wynne
#
### No Known Issues
################################################################################################

import arcpy
from arcpy import env
import sys, string, os, time, datetime
import csv

# SET TO OVERWRITE
arcpy.env.overwriteOutput = True

#NOTE: Before running the below code, I manually create a geodatabase and "Commitments" point feature class
#with the below column names. Make sure that the original fields you create have enough storage value in 
#in ArcCatalogue to handle the description strings

#Create Directory file path
root = "c:/arcgisserver/DataAndMXDs/TIMReady/"

# Logging script
myStartDate = str(datetime.date.today())
myStartTime = time.clock()
theStartTime = time.ctime()
print theStartTime
file = open("C:\\ETLs\\TIM\\TIMUpdates\\Logs\\" + myStartDate + "test"+ ".txt", "w")
file.write(theStartTime + "\n")
when =datetime.date.today()
theDate = when.strftime("%d")
theDay=when.strftime("%A")
print theDay

try:
	
	mitigation_layer = root + "sfmta_commitments.gdb/Commitments"
	mitigation_projected = root + "sfmta_commitments.gdb/Commitments_proj"
	mitigation_buffer = root + "sfmta_commitments.gdb/Commitments_buffer"
	arcpy.TruncateTable_management(mitigation_layer)
	file.write("Deleted old feature class table" + "\n")
	cursor = arcpy.da.InsertCursor(mitigation_layer, ['Title', 'Description', 'SHAPE@XY'])
	
	with open('C:\\ETLs\\TIM\\TIMUpdates\\Raw_Data\\mitigation.csv', 'r') as f:
		reader = csv.DictReader(f)
		#reader = [('Title1', 'The project is the construction of a 36-story 262', (-122.4248302, 37.7856142)),
		#('Title2', 'The proposed project would demolish the existing', (-122.4248302, 37.7856142))]

		for row in reader:
			cursor.insertRow((str(row['Title2']), str(row['Short Description']),(float(row['Longitude']), float(row['Latitude'])))) 
			
	#clean up
	del cursor
			
	theEndTime = time.ctime()
	file.write("End time" + theEndTime + "\n")
	
	#Project feature class
	webmercator = arcpy.SpatialReference(3857)
	arcpy.Project_management(mitigation_layer, mitigation_projected, webmercator)
	print "Projected measures"
	file.write("Projected Measures" + "\n")
	
	#Buffer feature class
	print("\n")
	print "Buffering "
	arcpy.Buffer_analysis(mitigation_projected, mitigation_buffer, "0.25 Miles", "FULL", "ROUND", "NONE", "", "PLANAR")
	print "Projected measures"
	file.write("Buffered Measures" + "\n")
		
	file.write(str(time.ctime()) +": Time Ended")
	file.close()
	
except Exception,e:
	print "Ended badly"
	print str(e)
	print arcpy.GetMessages()
	file.write(str(e) + "\n")
	file.write(arcpy.GetMessages() + "\n" )
	file.write(str(time.ctime()) +": Ended badly")
	file.close()
	

