#-------------------------------------------------------------------------------
# Name:        Subbasin_properties_table
# Purpose:     Create a table for subbasins containing different characteristics
#
# Author:      Prasanna Dahal
#
# Created:     05/08/2016
# Copyright:   (c) Prasanna Dahal 2016
# ArcGIS:      10.3
# Python:      2.7
#-------------------------------------------------------------------------------

import arcpy
import os
'''
Input :
subbasin: A catchment / subbasin shapefile
raster_folder: A folder that contains raster files in TIF
Optional Input: GDB

Output
Subbasin_characteristics.xls named excel file in TIF folder
'''

subbasin= arcpy.GetParameterAsText(0)
raster_folder = arcpy.GetParameterAsText(1)
outGDB= arcpy.GetParameterAsText(2)

if subbasin == "":
    subbasin = r"C:\Users\WIN10-HOME\Dropbox\CLASSES\CEE6450 Hydrological Modeling\Project_cee6450\hechms_Prasanna_Sal_RBC\HECHMS_Projects\hecgeohms.gdb\Catchment"
    raster_folder = r"E:\Research Data\00 Red Butte Creek\RBC_3\tif"
    outGDB = r"E:\Research Data\00 Red Butte Creek\RBC_3\New File Geodatabase (2).gdb"

# create a GDB if user does not specify
if outGDB == "":
    try:
        arcpy.CreateFileGDB_management(raster_folder, "TABLES.gdb")
        outGDB = os.path.join(raster_folder, "TABLES.gdb")
    except Exception, e:
        arcpy.AddMessage(outGDB)


arcpy.env.overwriteOutput = True
arcpy.env.workspace = arcpy.env.scratchWorkspace = outGDB

list_of_rasters = [os.path.join(raster_folder, file) for file in os.listdir(raster_folder) if file.endswith(".tif")]

mxd = arcpy.mapping.MapDocument("CURRENT")                                  # get the map document
df = arcpy.mapping.ListDataFrames(mxd,"*")[0]                               #first dataframe in the document


for aRaster_fullname in list_of_rasters:

    raster_layer = arcpy.mapping.Layer(aRaster_fullname )                 # create a new layer
    arcpy.mapping.AddLayer(df, raster_layer ,"TOP")

    table_name = os.path.basename(aRaster_fullname).split(".")[0]

    arcpy.AddMessage("%s and %s"%(raster_layer,table_name) )

    arcpy.gp.ZonalStatisticsAsTable_sa(subbasin, "GridID", raster_layer, table_name , "DATA", "MEAN")

    arcpy.AddField_management(in_table=table_name, field_name=table_name, field_type="DOUBLE",
                              field_precision="", field_scale="", field_length="", field_alias="",
                              field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    arcpy.CalculateField_management(in_table=table_name, field=table_name,
                                    expression="[MEAN]", expression_type="VB", code_block="")


for aRaster_fullname in list_of_rasters:
    table_name = os.path.basename(aRaster_fullname).split(".")[0]
    arcpy.AddJoin_management(in_layer_or_view=subbasin,
                             in_field="GridID",
                             join_table=table_name,
                             join_field="GridID", join_type="KEEP_ALL")


arcpy.TableToExcel_conversion(Input_Table=subbasin,
                              Output_Excel_File=os.path.join(raster_folder, "Subbasin_characteristics.xls"),
                              Use_field_alias_as_column_header="NAME",
                              Use_domain_and_subtype_description="DESCRIPTION")


