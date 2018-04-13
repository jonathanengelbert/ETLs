# This script copies the basic layers from Transbase needed by TIM, with the exception of injury data.
#Last modified: 11/21/2017 by Jonathan Engelbert
#
### No Known Issues
### WARNING: #CAUTION: The field "overlap" in dataset "TB_overall_hgh_injry_network" no longer exists 
### in newer versions of this dataset. It has been deleted for processing, and might cause problems 
### once the data is loaded. See STEP THREE below for old code and notes.
################################################################################################

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

#try:
if 1==1:
    myStartDate = str(datetime.date.today())
    myStartTime = time.clock()
    theStartTime = time.ctime()
    # thisfile = os.path.realpath(__file__)
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "Transbase1" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

################################################################################################

    # STEP ONE
    # COPYING FROM SDE TO LOCAL STAGING FOLDER: SET NAMES AND PATHS
    # NOTE: NO NEED TO REPROJECT TRANSBASE LAYERS

    # lists for looping through later
    transbaselist = []
    locallist = []

    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\Transbase_1.gdb\\"

    # intersection - transpo variables
    tb_transpo = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_intrsctn_trnsprtn"
    tb_transpo_local = "TB_intersection_transpo"
    transbaselist.append(tb_transpo) # add to list created above for looping later
    locallist.append(tb_transpo_local)
    
    # pedestrian high injury corridor
    tb_hicped = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_st_sgmt_2013_ped_hgh_injry_crrdr"
    tb_hicped_local = "TB_ped_hgh_injry_crrdr"
    transbaselist.append(tb_hicped)
    locallist.append(tb_hicped_local)

    # vehicle high injury corridor 
    tb_hicveh = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_st_sgmt_2014_veh_hgh_injry_crrdr"
    tb_hicveh_local = "TB_veh_hgh_injry_crrdr"
    transbaselist.append(tb_hicveh)
    locallist.append(tb_hicveh_local)

    # vision zero capital improvements
    #tb_vz = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_intrsctn_sp_vz_capital_improvements_40_projects_aug15"
    tb_vz = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_intrsctn_sp_vz_capital_improvements_40_projects_jan16"
    tb_vz_local = "TB_VZ_capitalimprovements"
    transbaselist.append(tb_vz)
    locallist.append(tb_vz_local)

    # Bicycle high injury corridors
    tb_hiccyc = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_st_sgmt_2014_cyc_hgh_injry_crrdr"
    tb_hiccyc_local = "TB_cyc_hgh_injry_crrdr"
    transbaselist.append(tb_hiccyc)
    locallist.append(tb_hiccyc_local)

    # overall high injury network
    tb_hic = "C:\\SDE_Connections\\Transbase.sde\\transbase_public.public.vw_geo_st_sgmt_2017_vz_hgh_injry_ntwrk"
    tb_hic_local = "TB_overall_hgh_injry_network"
    transbaselist.append(tb_hic)
    locallist.append(tb_hic_local)
    
    print str(len(transbaselist)) + " layers from Transbase identified"
    print str(len(locallist)) + " destination layers set"
    file.write(str(time.ctime()) +": " +str(len(transbaselist)) + " layers from Transbase identified"+ "\n")
    file.write(str(time.ctime()) +": " +str(len(locallist)) + " destination layers set"+ "\n")

################################################################################################
	
    # STEP TWO
    # COPYING FROM SDE TO LOCAL STAGING FOLDER: DO THE ACTUAL COPYING

    # loop through layers and copy to staging folder

    for i in range(0,len(transbaselist)):
        print("\n")
        print "Copying files - iteration " + str(i) + ":"
        print "New file path: " + staging_gdb + locallist[i]
        print "From: " + transbaselist[i]
        try:
            filename = staging_gdb + locallist[i]
            arcpy.CopyFeatures_management(transbaselist[i], filename, "", "0", "0", "0")
        except:
            print "FAILED TO COPY " + filename
            file.write(str(time.ctime()) +": FAILED TO COPY"+ filename+"\n")

    file.write(str(time.ctime()) +": copied files"+ "\n")
    # STEP THREE
    # GEOPROCESSING

    # create list for looping later
    bufferlist = []

    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)
        

    # intersection - transpo variables
    # 1/4 mile buffer, no dissolve

    arcpybuffer("tb_int_transpo_buffer_quartermile",tb_transpo_local,".25 Miles","","")
    file.write(str(time.ctime()) +": 0.25 buffer"+ "\n")

    # pedestrian high injury corridor
    # create 250 ft buffer, dissolve on "street_nam" and "street_typ"

    arcpybuffer("TB_ped_hgh_injry_crrdr_buffer",tb_hicped_local,"250 Feet","LIST",["street_nam","street_type"])
    file.write(str(time.ctime()) +": 250ft buffer - ped"+ "\n")

    # vehicle high injury corridor
    # create 250 ft buffer, dissolve on "street_nam" and "street_typ"

    arcpybuffer("TB_veh_hgh_injry_crrdr_buffer",tb_hicveh_local,"250 Feet","LIST",["street_nam","street_type"])
    file.write(str(time.ctime()) +": 250ft buffer - veh"+ "\n")

    # bicycle high injury corridor
    # create 250 ft buffer, dissolve on "street_nam" and "street_typ"

    arcpybuffer("TB_cyc_hgh_injry_crrdr_buffer",tb_hiccyc_local,"250 Feet","LIST",["street_nam","street_type"])
    file.write(str(time.ctime()) +": 250ft buffer - cyc"+ "\n")

    # overall high injury network
    # create 250 ft buffer, dissolve on "street_nam", "street_typ", and "overlap"

    #CAUTION: The field "overlap" no longer exists in newer versions of this dataset. It has been deleted for processing, and might cause problems once the data is loaded. Below is the original line of code for the geoprocess:
    #arcpybuffer("TB_overall_hgh_injry_network_buffer",tb_hic_local,"250 Feet","LIST",["street_nam","street_type","overlap"])
    arcpybuffer("TB_overall_hgh_injry_network_buffer",tb_hic_local,"250 Feet","LIST",["street_nam","street_type"])
    file.write(str(time.ctime()) +": 250ft buffer - overall"+ "\n")

    # vision zero capital improvements
    # - create 500 ft buffer, no dissolve

    arcpybuffer("TB_VZ_capitalimprovements_buffer",tb_vz_local,"500 Feet","","")
    file.write(str(time.ctime()) +": 500ft buffer - cap imp"+ "\n")
	
    print str(len(bufferlist)) + " buffer layers created"

    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()

################################################################################################	
	
try:
    print "FINISHED SUCCESSFULLY"
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
