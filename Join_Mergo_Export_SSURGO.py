#-------------------------------------------------------------------------------
# Name:        Join_Mergo_Export_SSURGO
# Purpose:     Join the soil properties table, merge them to SSURGO and/ or STATSGO
#				Export the data as Raster (to TIF)
#
# Author:      Prasanna Dahal
#
# Created:     05/08/2016
# Copyright:   (c) Prasanna Dahal 2016
# ArcGIS:      10.3
# Python:      2.7
#-------------------------------------------------------------------------------
import os
import arcpy
from arcpy import env

path2_ssurgo = arcpy.GetParameterAsText(0)
path2statsgo = arcpy.GetParameterAsText(1) # make it optional
outDir = arcpy.GetParameterAsText(2)
MaskRaster = arcpy.GetParameterAsText(3)  



def STEP5_Join_Merge_Export (path2_ssurgo, path2statsgo, outDir,MaskRaster ):

    arcpy.AddMessage("*** This scripts joins the soil values to mushape, and exports as Rasters *** ")

    if not os.path.exists(outDir+"/TEMP"):
        os.mkdir(outDir+"/TEMP")
    TEMP = os.path.join(outDir, "TEMP")
    # soilProperties =[ [name from csv, new name for raster, field name defaulted by ArcGIS],.....
    soilProperties = [
                   ["Ks_WtAvg", "KSAT", "MUKEY_Vs_3" ],
                   ["ResidualWaterContent_WtAvg", "RSM", "MUKEY_Vs_6" ],
                   ["Porosity_WtAvg","POR", "MUKEY_Vs_7" ],
                   ["EffectivePorosity_WtAvg","EFPO", "MUKEY_Vs_8"  ] ,
                   ["BubblingPressure_Geometric_WtAvg", "BBL", "MUKEY_Vs_9" ] ,
                   ["PoreSizeDistribution_geometric_WtAvg_y","PSD", "MUKEY_V_10"],
                   ["HydroGrp", "HSG", "MUKEY_V_12"]
                   ]

    mxd = arcpy.mapping.MapDocument("CURRENT")                                  # get the map document
    df = arcpy.mapping.ListDataFrames(mxd,"*")[0]                               #first dataframe in the document

    # this file is used to clip the soil features
    arcpy.RasterToPolygon_conversion(in_raster=MaskRaster,
                                     out_polygon_features=os.path.join(TEMP,"mask_polygon"),
                                     simplify="NO_SIMPLIFY", raster_field="Value")

    # for each ssurgo or statsgo folders, navigates to sub-folders, adds join and adds to mxd as a layer
    def join_field( path2statsgo_or_ssurgo, MaskRaster, dataType ):
        """
        :param path2_soilFolderFolders: The path to a folder containing the collection of SSURGO (or Statsgo) folders
        :param MaskRaster: DEM or any raster whose extent, coordinate system are considered while creating SSURGO rasters
        :param dataType: string, "statsgo" or "ssurgo"
        :return:
        """

        mxd = arcpy.mapping.MapDocument("CURRENT")                                  # get the map document
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]                               #first dataframe in the document

        # create a list of folders containing SSURGO folders only
        folderList = []
        [folderList.append(folders) for folders in os.listdir(path2statsgo_or_ssurgo)
            if os.path.isdir(os.path.join(path2statsgo_or_ssurgo, folders))]
        
        # One ssurgo or statsgo folder, one at a time
        for folder in folderList:
            arcpy.AddMessage(folder)
            path2_soilFolder= os.path.join(path2statsgo_or_ssurgo, folder)
            path2tabular = os.path.join(path2_soilFolder, "tabular")
            path2Spatial= os.path.join(path2_soilFolder,"spatial")
            # arcpy.env.workspace =  path2_soilFolder   # arcpy.env.scratchWorkspace =

            muShapefile = os.listdir(path2Spatial)[1].split('.')[0]                     # muShapefile = 'soilmu_a_ut612'

            # project the shapefile in ssurgo table, FILE SELECTION
            if dataType.lower() == "statsgo":
                new_mu = muShapefile +"_statsgo_prj"
            if dataType.lower() == "ssurgo" :
                new_mu = muShapefile +"_ssurgo_prj"
            try:
                arcpy.Project_management(in_dataset=path2_soilFolder+"/spatial/" + muShapefile +".shp",
                                     out_dataset=TEMP + "/"+ new_mu,
                                     out_coor_system= MaskRaster, transform_method="", max_deviation="")
                arcpy.AddMessage("SUCCESS: Shapefile projection")
            except Exception, e:
                print e
                arcpy.AddMessage("FAILED: Shapefile projection")
    
            # to add the projected shapefile from ssurgo, as a layer to the map at the bottom of the TOC in data frame 0
            muShapefileAsLayer = new_mu

            layer1 = arcpy.mapping.Layer(TEMP + "/"+ muShapefileAsLayer+ ".shp" ) # create a new layer
            arcpy.mapping.AddLayer(df, layer1,"TOP")             #added to layer because this will be used code below

            try:
                # join the table that had mUKEY mapped to all soil properties
                arcpy.AddJoin_management(muShapefileAsLayer, "MUKEY", path2_soilFolder+"/MUKEY-Vs-Values.csv", "MUKEY")
                arcpy.AddMessage("SUCCESS: Field Addition")
            except Exception, e:
                arcpy.AddMessage("FAILED: Field Addition")

        return


    def merge_and_clip(TEMP):
        layers = arcpy.mapping.ListLayers(mxd, "", df)
        statsgo_layers = [lyr.name for lyr in layers if lyr.name.endswith("_statsgo_prj")]
        ssurgo_layers = [lyr.name for lyr in layers if lyr.name.endswith("_ssurgo_prj")]

        arcpy.AddMessage("MERGING ")

        arcpy.Merge_management(inputs= ";".join(ssurgo_layers),
                           output=os.path.join(TEMP,"ssurgo_merged") ,
                           field_mappings="""AREASYMBOL "AREASYMBOL" true true false 20 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.AREASYMBOL,-1,-1;SPATIALVER "SPATIALVER" true true false 10 Long 0 10 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.SPATIALVER,-1,-1;MUSYM "MUSYM" true true false 6 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.MUSYM,-1,-1;MUKEY "MUKEY" true true false 30 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.MUKEY,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_x,-1,-1;MUKEY_Vs_Values_csv_MUKEY "MUKEY_Vs_Values_csv_MUKEY" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.MUKEY,-1,-1;MUKEY_Vs_Values_csv_ksat_r_WtAvg "MUKEY_Vs_Values_csv_ksat_r_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.ksat_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Ks_WtAvg "MUKEY_Vs_Values_csv_Ks_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.Ks_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg "MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.dbthirdbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg "MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.dbfifteenbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg "MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.ResidualWaterContent_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Porosity_WtAvg "MUKEY_Vs_Values_csv_Porosity_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.Porosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg "MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.EffectivePorosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg "MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.BubblingPressure_Geometric_WtAvg,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_y,-1,-1;AREASYMBOL_1 "AREASYMBOL_1" true true false 20 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.AREASYMBOL,-1,-1;SPATIALVER_1 "SPATIALVER_1" true true false 10 Long 0 10 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.SPATIALVER,-1,-1;MUSYM_1 "MUSYM_1" true true false 6 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.MUSYM,-1,-1;MUKEY_1 "MUKEY_1" true true false 30 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.MUKEY,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x_1 "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_x,-1,-1;MUKEY_Vs_Values_csv_MUKEY_1 "MUKEY_Vs_Values_csv_MUKEY_1" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.MUKEY,-1,-1;MUKEY_Vs_Values_csv_ksat_r_WtAvg_1 "MUKEY_Vs_Values_csv_ksat_r_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.ksat_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Ks_WtAvg_1 "MUKEY_Vs_Values_csv_Ks_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.Ks_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg_1 "MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.dbthirdbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg_1 "MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg_1" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.dbfifteenbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg_1 "MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.ResidualWaterContent_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Porosity_WtAvg_1 "MUKEY_Vs_Values_csv_Porosity_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.Porosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg_1 "MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.EffectivePorosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg_1 "MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.BubblingPressure_Geometric_WtAvg,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y_1 "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_y,-1,-1;MUKEY_Vs_Values_csv_AvaWaterCon "MUKEY_Vs_Values_csv_AvaWaterCon" true true false 4 Long 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.AvaWaterCon,-1,-1;MUKEY_Vs_Values_csv_HydroGrp "MUKEY_Vs_Values_csv_HydroGrp" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.HydroGrp,-1,-1""")

        arcpy.Merge_management(inputs= ";".join(statsgo_layers),
                               output=os.path.join(TEMP,"statgo_merged") ,
                               field_mappings="""AREASYMBOL "AREASYMBOL" true true false 20 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.AREASYMBOL,-1,-1;SPATIALVER "SPATIALVER" true true false 10 Long 0 10 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.SPATIALVER,-1,-1;MUSYM "MUSYM" true true false 6 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.MUSYM,-1,-1;MUKEY "MUKEY" true true false 30 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,gsmsoilmu_a_id_statsgo_prj.MUKEY,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_x,-1,-1;MUKEY_Vs_Values_csv_MUKEY "MUKEY_Vs_Values_csv_MUKEY" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.MUKEY,-1,-1;MUKEY_Vs_Values_csv_ksat_r_WtAvg "MUKEY_Vs_Values_csv_ksat_r_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.ksat_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Ks_WtAvg "MUKEY_Vs_Values_csv_Ks_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.Ks_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg "MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.dbthirdbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg "MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.dbfifteenbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg "MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.ResidualWaterContent_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Porosity_WtAvg "MUKEY_Vs_Values_csv_Porosity_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.Porosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg "MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.EffectivePorosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg "MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.BubblingPressure_Geometric_WtAvg,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_id_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_y,-1,-1;AREASYMBOL_1 "AREASYMBOL_1" true true false 20 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.AREASYMBOL,-1,-1;SPATIALVER_1 "SPATIALVER_1" true true false 10 Long 0 10 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.SPATIALVER,-1,-1;MUSYM_1 "MUSYM_1" true true false 6 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.MUSYM,-1,-1;MUKEY_1 "MUKEY_1" true true false 30 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,gsmsoilmu_a_ut_statsgo_prj.MUKEY,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x_1 "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_x_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_x,-1,-1;MUKEY_Vs_Values_csv_MUKEY_1 "MUKEY_Vs_Values_csv_MUKEY_1" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.MUKEY,-1,-1;MUKEY_Vs_Values_csv_ksat_r_WtAvg_1 "MUKEY_Vs_Values_csv_ksat_r_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.ksat_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Ks_WtAvg_1 "MUKEY_Vs_Values_csv_Ks_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.Ks_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg_1 "MUKEY_Vs_Values_csv_dbthirdbar_r_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.dbthirdbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg_1 "MUKEY_Vs_Values_csv_dbfifteenbar_r_WtAvg_1" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.dbfifteenbar_r_WtAvg,-1,-1;MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg_1 "MUKEY_Vs_Values_csv_ResidualWaterContent_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.ResidualWaterContent_WtAvg,-1,-1;MUKEY_Vs_Values_csv_Porosity_WtAvg_1 "MUKEY_Vs_Values_csv_Porosity_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.Porosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg_1 "MUKEY_Vs_Values_csv_EffectivePorosity_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.EffectivePorosity_WtAvg,-1,-1;MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg_1 "MUKEY_Vs_Values_csv_BubblingPressure_Geometric_WtAvg_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.BubblingPressure_Geometric_WtAvg,-1,-1;MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y_1 "MUKEY_Vs_Values_csv_PoreSizeDistribution_geometric_WtAvg_y_1" true true false 8 Double 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.PoreSizeDistribution_geometric_WtAvg_y,-1,-1;MUKEY_Vs_Values_csv_AvaWaterCon "MUKEY_Vs_Values_csv_AvaWaterCon" true true false 4 Long 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.AvaWaterCon,-1,-1;MUKEY_Vs_Values_csv_HydroGrp "MUKEY_Vs_Values_csv_HydroGrp" true true false 8000 Text 0 0 ,First,#,gsmsoilmu_a_ut_statsgo_prj,MUKEY-Vs-Values.csv.HydroGrp,-1,-1""")

        arcpy.Clip_analysis(in_features=os.path.join(TEMP,"ssurgo_merged.shp"),
                            clip_features=os.path.join(TEMP,"mask_polygon.shp"),
                            out_feature_class=os.path.join(TEMP,"Project_ssurgo_merged"), cluster_tolerance="")

        arcpy.Clip_analysis(in_features=os.path.join(TEMP,"statgo_merged.shp"),
                            clip_features=os.path.join(TEMP,"mask_polygon.shp"),
                            out_feature_class=os.path.join(TEMP,"Project_statsgo_merged"), cluster_tolerance="")

        arcpy.AddMessage("SUCCESS: SSURGO and STATSGO for the area merged")
        return

    def erase_statsgo_and_merge():
        # select the NULL values in SSURGO, and delete them
        try:
            arcpy.MakeFeatureLayer_management(os.path.join(TEMP,"Project_ssurgo_merged.shp"), "Project_ssurgo_merged")

            arcpy.SelectLayerByAttribute_management(in_layer_or_view="Project_ssurgo_merged",
                                                    selection_type="NEW_SELECTION",
                                                    where_clause=""""MUKEY_Vs_9" = 0""")

            arcpy.DeleteFeatures_management(in_features= "Project_ssurgo_merged")
            arcpy.AddMessage("SUCCESS: NULL records from SSURGO deleted")

            arcpy.Erase_analysis(in_features=os.path.join(TEMP,"Project_statsgo_merged.shp"),
                                 erase_features=os.path.join(TEMP,"Project_ssurgo_merged.shp"),
                                 out_feature_class=os.path.join(TEMP,"useful_stasgo"), cluster_tolerance="")

            arcpy.Merge_management(inputs="%suseful_stasgo.shp;%sProject_ssurgo_merged.shp"%(TEMP+"/", TEMP+"/"),
                                   output=os.path.join(TEMP,"Project_ssurgo_statsgo_merge"),
                                   field_mappings="""AREASYMBOL "AREASYMBOL" true true false 20 Text 0 0 ,First,#,Project_statsgo_merged_Erase,AREASYMBOL,-1,-1,Project_ssurgo_merged,AREASYMBOL,-1,-1;SPATIALVER "SPATIALVER" true true false 4 Long 0 0 ,First,#,Project_statsgo_merged_Erase,SPATIALVER,-1,-1,Project_ssurgo_merged,SPATIALVER,-1,-1;MUSYM "MUSYM" true true false 6 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUSYM,-1,-1,Project_ssurgo_merged,MUSYM,-1,-1;MUKEY "MUKEY" true true false 30 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY,-1,-1,Project_ssurgo_merged,MUKEY,-1,-1;MUKEY_Vs_V "MUKEY_Vs_V" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_V,-1,-1,Project_ssurgo_merged,MUKEY_Vs_V,-1,-1;MUKEY_Vs_1 "MUKEY_Vs_1" true true false 254 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_1,-1,-1,Project_ssurgo_merged,MUKEY_Vs_1,-1,-1;MUKEY_Vs_2 "MUKEY_Vs_2" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_2,-1,-1,Project_ssurgo_merged,MUKEY_Vs_2,-1,-1;MUKEY_Vs_3 "MUKEY_Vs_3" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_3,-1,-1,Project_ssurgo_merged,MUKEY_Vs_3,-1,-1;MUKEY_Vs_4 "MUKEY_Vs_4" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_4,-1,-1,Project_ssurgo_merged,MUKEY_Vs_4,-1,-1;MUKEY_Vs_5 "MUKEY_Vs_5" true true false 254 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_5,-1,-1,Project_ssurgo_merged,MUKEY_Vs_5,-1,-1;MUKEY_Vs_6 "MUKEY_Vs_6" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_6,-1,-1,Project_ssurgo_merged,MUKEY_Vs_6,-1,-1;MUKEY_Vs_7 "MUKEY_Vs_7" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_7,-1,-1,Project_ssurgo_merged,MUKEY_Vs_7,-1,-1;MUKEY_Vs_8 "MUKEY_Vs_8" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_8,-1,-1,Project_ssurgo_merged,MUKEY_Vs_8,-1,-1;MUKEY_Vs_9 "MUKEY_Vs_9" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_Vs_9,-1,-1,Project_ssurgo_merged,MUKEY_Vs_9,-1,-1;MUKEY_V_10 "MUKEY_V_10" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_10,-1,-1,Project_ssurgo_merged,MUKEY_V_10,-1,-1;AREASYMB_1 "AREASYMB_1" true true false 20 Text 0 0 ,First,#,Project_statsgo_merged_Erase,AREASYMB_1,-1,-1;SPATIALV_1 "SPATIALV_1" true true false 4 Long 0 0 ,First,#,Project_statsgo_merged_Erase,SPATIALV_1,-1,-1;MUSYM_1 "MUSYM_1" true true false 6 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUSYM_1,-1,-1;MUKEY_1 "MUKEY_1" true true false 30 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_1,-1,-1;MUKEY_V_11 "MUKEY_V_11" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_11,-1,-1,Project_ssurgo_merged,MUKEY_V_11,-1,-1;MUKEY_V_12 "MUKEY_V_12" true true false 254 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_12,-1,-1,Project_ssurgo_merged,MUKEY_V_12,-1,-1;MUKEY_V_13 "MUKEY_V_13" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_13,-1,-1;MUKEY_V_14 "MUKEY_V_14" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_14,-1,-1;MUKEY_V_15 "MUKEY_V_15" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_15,-1,-1;MUKEY_V_16 "MUKEY_V_16" true true false 254 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_16,-1,-1;MUKEY_V_17 "MUKEY_V_17" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_17,-1,-1;MUKEY_V_18 "MUKEY_V_18" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_18,-1,-1;MUKEY_V_19 "MUKEY_V_19" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_19,-1,-1;MUKEY_V_20 "MUKEY_V_20" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_20,-1,-1;MUKEY_V_21 "MUKEY_V_21" true true false 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_21,-1,-1;MUKEY_V_22 "MUKEY_V_22" true true false 4 Long 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_22,-1,-1;MUKEY_V_23 "MUKEY_V_23" true true false 254 Text 0 0 ,First,#,Project_statsgo_merged_Erase,MUKEY_V_23,-1,-1;Shape_Length "Shape_Length" false true true 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0 ,First,#,Project_statsgo_merged_Erase,Shape_Area,-1,-1""")
            arcpy.AddMessage("SUCCESS: Erasing NULL SSURGO records and Merging STATSGO to SSURGO")

            merged_layer = arcpy.mapping.Layer(os.path.join(TEMP,"Project_ssurgo_statsgo_merge.shp") )                 # create a new layer
            arcpy.mapping.AddLayer(df, merged_layer ,"TOP")
        except Exception,e:
            arcpy.AddMessage("FAILED: Erasing NULL SSURGO records and Merging STATSGO to SSURGO")
        return os.path.join(TEMP,"Project_ssurgo_statsgo_merge")

    def export(project_ssurgo_statsgo, soilProperties, MaskRaster, TEMP= TEMP, outDir= outDir ):
        arcpy.env.snapRaster = MaskRaster                                          # Set Snap Raster environment

        project_ssurgo_statsgo = project_ssurgo_statsgo+".shp"

        # soilProperties = [[ "ksat_r_WtAvg", "Ksat-s_UT612" ], ["Ks_WtAvg", "Ksat-t_ut612" ], .... ]
        for a_soil_property in soilProperties:
            soil_property_name = a_soil_property[1]            #e.g. Ksat-s, Bblpr-t, PoreSz-t etc.
            field_name = a_soil_property[2]
            outRaster = os.path.join(TEMP, soil_property_name)

            arcpy.FeatureToRaster_conversion(in_features=project_ssurgo_statsgo,
                                          field= field_name ,
                                          out_raster=outRaster, cell_size= MaskRaster  )

            arcpy.gp.ExtractByMask_sa(outRaster, MaskRaster,outRaster+"X")       #c=clipped

            # to clip the rasters to the consistent extent, so that their (nrows x ncol) matches
            arcpy.Clip_management(in_raster=outRaster+"X",
                  out_raster= outRaster+"c" , in_template_dataset=MaskRaster, nodata_value="-9999",
                  clipping_geometry="NONE", maintain_clipping_extent="MAINTAIN_EXTENT")
            arcpy.RasterToOtherFormat_conversion(Input_Rasters="'%s'"%(outRaster+"c"), Output_Workspace=outDir, Raster_Format="TIFF")
            arcpy.AddMessage("SUCCESS: TIF representing %s values saved in %s"%(soil_property_name, outDir))

            # load the TIF to arcmap
            # tif_layer = arcpy.mapping.Layer(os.path.join(outDir, soil_property_name+"c.tif") )                 # create a new layer
            # arcpy.mapping.AddLayer(df, tif_layer ,"TOP")
    # __main__
    join_field( path2_ssurgo, MaskRaster,"ssurgo")
    if path2statsgo != "":
        join_field( path2statsgo, MaskRaster, "statsgo" )
    merge_and_clip(TEMP)
    erase_statsgo_and_merge()
    export(erase_statsgo_and_merge(), soilProperties, MaskRaster )

if __name__ == "__main__":
    STEP5_Join_Merge_Export (path2_ssurgo, path2statsgo, outDir, MaskRaster )

