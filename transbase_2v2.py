# This script copies injury and fatality data from Transbase database.
#Last modified: 11/30/2017 by Jonathan Engelbert
#
### No Known Issues
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

try:
    myStartDate = str(datetime.date.today())
    myStartTime = time.clock()
    theStartTime = time.ctime()
    # thisfile = os.path.realpath(__file__)
    file = open("C:/ETLs/TIM/TIMUpdates/Logs/" + myStartDate + "Transbase2" + ".txt", "w")
    file.write(theStartTime + "\n")
    when =datetime.date.today()
    theDate = when.strftime("%d")
    theDay=when.strftime("%A")
    print theDay

 ################################################################################################
    #STEP ONE
    # COPYING FROM SDE TO LOCAL STAGING FOLDER
    # NOTE: NO NEED TO REPROJECT TRANSBASE LAYERS
    
    # filepath for all copied files:
    staging_gdb = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\Transbase_2.gdb\\"
    
    # pedestrian collisions

    #HAS THIS CHANGED NAME?
    tb_ped = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_switrs_all_types_ped_col_cty"    
    pedcol_1 = "pedcol_1"
    
    try:
        
        pedcolloc="\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMReady\\Transbase_2.gdb\\pedcol_1"
        #arcpy.CopyFeatures_management(tb_ped, staging_gdb + pedcol_1, "", "0", "0", "0")
        arcpy.CopyFeatures_management(tb_ped, pedcolloc, "", "0", "0", "0")
        print "Pedestrian collisions from Transbase loaded"
        print "Copied successfully to staging folder"
        file.write(str(time.ctime()) +": copied files - ped"+ "\n")
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO COPY - ped"+ "\n")
        print "FAILED TO COPY - ped"

    # bike collisions
    tb_bikecol = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_switrs_all_types_cyc_col_cty"
    bikecol_1 = "bikecol_1"
   
    

    
    try:
        arcpy.CopyFeatures_management(tb_bikecol, staging_gdb + bikecol_1, "", "0", "0", "0")
        arcpy.MultipartToSinglepart_management(staging_gdb + bikecol_1, staging_gdb + "bikecol_1_singlepart")
        print "Bicycle collisions from Transbase loaded"
        print "Copied successfully to staging folder"
        file.write(str(time.ctime()) +": copied files - bike"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO COPY - bike"+ "\n")
        print "FAILED TO COPY - bike"
    bikecol_1 = "bikecol_1_singlepart"

    # pedestrian collisions (all parties)
    tb_ped_party = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_intrsctn_switrs_all_types_ped_col_prties_all_cty"
    tb_ped_party = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_switrs_all_types_ped_col_prties_all_cty"
    pedcol_party = "pedcol_party"
    
    try:
        arcpy.CopyFeatures_management(tb_ped_party, staging_gdb + pedcol_party, "", "0", "0", "0")
        print "Pedestrian collisions (all parties) from Transbase loaded"
        print "Copied successfully to staging folder"
        file.write(str(time.ctime()) +": copied files - pedcol party"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO COPY - pedcol party"+ "\n")
        print "FAILED TO COPY - pedcol party"

    # bike collisions (all parties)
    #tb_bike_party = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_intrsctn_switrs_all_types_cyc_col_prties_all_cty"
    tb_bike_party = "Database Connections\\Transbase.sde\\transbase_public.public.vw_geo_switrs_all_types_cyc_col_prties_all_cty"
    bikecol_party = "bikecol_party"
    
    try:
        arcpy.CopyFeatures_management(tb_bike_party, staging_gdb + bikecol_party, "", "0", "0", "0")
        print "Bicycle collisions (all parties) from Transbase loaded"
        print "Copied successfully to staging folder"
        file.write(str(time.ctime()) +": copied files - bikecol party"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO COPY - bikecol party"+ "\n")
        print "FAILED TO COPY - bikecol party"

################################################################################################
    #STEP TWO
    # GEOPROCESSING: PED AND BIKE INJURY/FATALITY COUNT AT INTERSECTION

    # Ped counts at intersection
    pedcol_2 = "TB_pedcollisions_int"
    
    try:
        print "Calculating ped counts..."
        arcpy.AddField_management(staging_gdb + pedcol_1, "pedinj", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.AddField_management(staging_gdb + pedcol_1, "pedfatal", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

        arcpy.CalculateField_management(staging_gdb + pedcol_1, "pedinj", "!count_ped_injured!", "PYTHON", "")
        arcpy.CalculateField_management(staging_gdb + pedcol_1, "pedfatal", "!count_ped_injured!", "PYTHON", "")
        print "Success"
        file.write(str(time.ctime()) +": calculated ped counts"+ "\n")

        print "Dissolving ped counts..."
        arcpy.Dissolve_management(
            staging_gdb + pedcol_1,
            staging_gdb + pedcol_2,
            "cnn_intrsctn_fkey",
            "primary_rd FIRST;secondary_rd FIRST;intersection FIRST;pedinj SUM;pedfatal SUM", "MULTI_PART", "DISSOLVE_LINES"
            )
        print "Success"
    except Exception as e:
        print(e)
        print "Success"
        file.write(str(time.ctime()) +": dissolved"+ "\n")
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped counts"+ "\n")

    # Bike counts at intersection
    try:
        print "Calculating bike counts..."
        arcpy.AddField_management(staging_gdb + bikecol_1, "bikeinj", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.AddField_management(staging_gdb + bikecol_1, "bikefatal", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

        arcpy.CalculateField_management(staging_gdb + bikecol_1, "bikeinj", "!count_bicyclist_injured!", "PYTHON", "")
        arcpy.CalculateField_management(staging_gdb + bikecol_1, "bikefatal", "!count_bicyclist_killed!", "PYTHON", "")
        print "Success"
        file.write(str(time.ctime()) +": calculated bike counts"+ "\n")
 
        print "Dissolving bike counts..."
        bikecol_2 = "TB_bikecollisions_int"
        arcpy.Dissolve_management(
            staging_gdb + bikecol_1,
            staging_gdb + bikecol_2,
            "cnn_intrsctn_fkey",
            "primary_rd FIRST;secondary_rd FIRST;intersection FIRST;bikeinj SUM;bikefatal SUM", "MULTI_PART", "DISSOLVE_LINES"
            )
        print "Success"
        file.write(str(time.ctime()) +": dissolved bikes"+ "\n")
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS bike counts"+ "\n")

################################################################################################
        
    #STEP THREE
    # GEOPROCESSING: PED AND BIKE FATALITIES

    # PED 
    # Ped fatalities
    try:
        switrs_mvmt_csv = "C:\\ETLs\\TIM\\TIMUpdates\\Helper Files\\switrs_mvmt.csv"
        switrs_mvmt_codes = "C:\\ETLs\\TIM\\TIMUpdates\\Helper Files\\switrs_mvmt.dbf"
        arcpy.CopyRows_management (switrs_mvmt_csv, switrs_mvmt_codes)
        print "SWITRS codes loaded"
    except:
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped fatalities - SWITRS codes loading failed"+ "\n")
        print "SWITRS codes not loaded"

    try:
        print "Filtering for ped fatalities..."
        # Create shapefile: pedestrian level, fatalities only
        ped_f = "pedcollisions_party_ped_fatal"
        arcpy.FeatureClassToFeatureClass_conversion (staging_gdb + pedcol_party, staging_gdb, ped_f, """ "party_type" = 'Pedestrian' AND "party_number_killed" <> 0 """)
        file.write(str(time.ctime()) +": copied FC - ped fatal"+ "\n")
        print("Success")
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped fatalities - feature class to feature class conversion (ped level fatalities)"+ "\n")
        print "error - feature class to feature class conversion (ped level fatalities)"

    try:
        # Create shapefile: auto level, with only columns for join (collision ID and turning movement of car)
        ped_f_auto = "ped_f_auto"
        arcpy.FeatureClassToFeatureClass_conversion (staging_gdb + pedcol_party, staging_gdb, ped_f_auto, """ "party_type" = 'Driver (including Hit and Run)' """)
        print "Success"
        file.write(str(time.ctime()) +": copied FC - ped f auto"+ "\n")
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped fatalities - feature class to feature class conversion (auto level dataset)"+ "\n")
        print "error - feature class to feature class conversion (auto level)"
        
        # Join auto movement to ped fatality table

    try:
        print "Processing ped fatalities..."
        # rename first to avoid confusion
        arcpy.AddField_management(staging_gdb + ped_f_auto, "auto_move", "TEXT", "", "", "100", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.CalculateField_management(staging_gdb + ped_f_auto, "auto_move", "!move_pre_acc!", "PYTHON_9.3", "")
        arcpy.DeleteField_management(staging_gdb + ped_f_auto, "move_pre_acc")
        arcpy.JoinField_management(staging_gdb + ped_f, "cnn_intrsctn_fkey", staging_gdb + ped_f_auto, "cnn_intrsctn_fkey", ["auto_move"])
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped fatalities - field manipulations"+ "\n")
    
    try:
        print "Joining ped fatalities..."
        file.write(str(time.ctime()) +": joined ped fatalities"+ "\n")
        # Join codes to auto move
        arcpy.JoinField_management(staging_gdb + ped_f, "auto_move", switrs_mvmt_codes, "Code", ["auto_desc"])
        print "Success"
    except:
        file.write(str(time.ctime()) +": FAILED TO PROCESS ped fatalities - join table"+ "\n")

    # BIKE
    try:
        print "Filtering for bike fatalities..."
        # Create shapefile: bike level, fatalities only
        bike_f = "bikecollisions_party_bike_fatal"
        arcpy.FeatureClassToFeatureClass_conversion (staging_gdb + bikecol_party, staging_gdb, bike_f, """ "party_type" = 'Bicyclist' AND "number_killed" <> 0 """)
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS bike fatalities - feature class to feature class conversion (bicyclist level dataset)"+ "\n")
        
    try:
        # Create shapefile: auto level, with only columns for join (collision ID and turning movement of car)
        bike_f_auto = "ped_f_auto"
        arcpy.FeatureClassToFeatureClass_conversion (staging_gdb + bikecol_party, staging_gdb, bike_f_auto, """ "party_type" = 'Driver (including Hit and Run)' """)
        print "Success"
    except:
        file.write(str(time.ctime()) +": FAILED TO PROCESS bike fatalities - feature class to feature class conversion (auto level dataset)"+ "\n")
        # Join auto movement to bike fatality table

    try:
        print "Processing bike fatalities..."
        # rename first to avoid confusion
        arcpy.AddField_management(staging_gdb +  bike_f_auto, "auto_move", "TEXT", "", "", "100", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.CalculateField_management(staging_gdb + bike_f_auto, "auto_move", "!move_pre_acc!", "PYTHON_9.3", "")
        arcpy.DeleteField_management(staging_gdb + bike_f_auto, "move_pre_acc")
        arcpy.JoinField_management(staging_gdb + bike_f, "cnn_intrsctn_fkey", staging_gdb + bike_f_auto, "cnn_intrsctn_fkey", ["auto_move"])
    except Exception as e:
        print(e)
        file.write(str(time.ctime()) +": FAILED TO PROCESS bike fatalities - field manipulations"+ "\n")
    
    try:
        print "Joining bike fatalities..."
        # Join codes to auto move
        arcpy.JoinField_management(staging_gdb + bike_f, "auto_move", switrs_mvmt_codes, "Code", ["auto_desc"])
        print "Success"
        file.write(str(time.ctime()) +": joined bike fatalities"+ "\n")
    except:
        file.write(str(time.ctime()) +": FAILED TO PROCESS bike fatalities - join table"+ "\n")


    # function to create buffers
    def arcpybuffer(buffer_name,original_name,buffer_dist,dissolve_opt,dissolve_fld):
        print("\n")
        print "Buffering " + buffer_name
        #bufferlist.append(buffer_name)
        staging_name = staging_gdb + original_name
        filename_buffer = staging_gdb + buffer_name
        arcpy.Buffer_analysis(staging_name, filename_buffer, buffer_dist, "", "", dissolve_opt, dissolve_fld)
        print "finished buffer"
        

    # buffer all
    # 500 feet buffer, no dissolve

    arcpybuffer("TB_pedcollisions_int_buffer",pedcol_2,"500 Feet","","")
    arcpybuffer("TB_bikecollisions_int_buffer",bikecol_2,"500 Feet","","")
    arcpybuffer("pedcollisions_party_ped_fatal_buffer",ped_f,"500 Feet","","")
    arcpybuffer("bikecollisions_party_bike_fatal_buffer",bike_f,"500 Feet","","")
    file.write(str(time.ctime()) +": ran buffers"+ "\n")
    file.write(str(time.ctime()) +": FINISHED SUCCESSFULLY"+ "\n")
    file.close()
    print "FINISHED SUCCESSFULLY"

################################################################################################
    
except Exception,e:
    print "Ended badly"
    file.write(str(time.ctime()) +": Ended badly")
    print str(e)
    file.write(str(e))
    file.close()
   
