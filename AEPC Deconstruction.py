# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 09:37:05 2023

@author: fdoktorm
"""

import PyUber
import pandas as pd
import numpy as np
#from pathlib import Path
import datetime


def DataExtractFromXEUS():
    #Connection sites definition
    sites = ["F28_PROD_XEUS", "F32_PROD_XEUS"]
    combined_df = pd.DataFrame()
    for site in sites:
        now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
        print(f"Connecting to {site}  {now}")
        conn = PyUber.connect(datasource=site)

        myQuery='''
      select distinct ah.LOAD_DATE, fac.FACILITY ,ah.APC_OBJECT_NAME, ah.LOT, ah.OPERATION , ah.LOT_PROCESS,VARCHAR(ah.ONLINE_ROW_ID) as ONLINE_ROW_ID, 
        ad.ATTRIBUTE_NAME, ad.ATTRIBUTE_VALUE
        
        from P_APC_TXN_HIST  ah
        inner join P_APC_TXN_DATA ad on ad.APC_DATA_ID = ah.APC_DATA_ID
        CROSS JOIN F_FACILITY fac
          
        where ah.APC_OBJECT_NAME = 'AEPC_LOT'
        and ah.APC_OBJECT_TYPE = 'LOT'
        and ah.LAST_VERSION_FLAG = 'Y'
        and ad.ATTRIBUTE_NAME In ('AREA','LOTID','ROUTE','PROCESS','OPERATION','MES_WAFER_IDS','MES_SLOTS','SLOTS','PROCESS_OPN','PRODGROUP','PRODUCT'
                                                        ,'SUBENTITY','SUBENTITIES','UPDATE_TIME','B_TOOL_PRIOR','B_TOOL_RS','B_TOOL','B_PART_PRIOR','B_PART_RS'
                                                        ,'B_PART','SETTING_USED','LOTSETTINGS','WAFERSETTINGS','FF_SUC','FB_SUC','CALCULATED_SETTING','OPENRUNS'
                                                        ,'OPENRUNS_PART','METROAVGBYWAFER','METROAVGLOT','TARGET','FB_METRODATA','FB_METRODATA_IDX','FB_METRODATA2','FB_METRODATA2_IDX','FB_METRODATA3','FB_METRODATA3_IDX','FB_TARGET'
                                                        ,'WAFERS1_ACT','WAFERS1_ACT_IDX','WAFERS2_ACT','WAFERS2_ACT_IDX','WAFERS3_ACT','WAFERS3_ACT_IDX','LAMBDA_TOOL_USED'
                                                        ,'LAMBDA_PART_USED','PM_COUNTER_PRIOR','PM_COUNTER','REFERENCE_SETTING','M_ETCHRATE','METRO_LOLIMIT','METRO_HILIMIT'
                                                        ,'BATCH_ID','RSTIME','SHORTWAFERIDS','CHAMBER','CHAMBER_IDX','VALIDDATA','APC_DATA_ID','UPTIME','METROAVG_CHBR'
                                                        ,'MACHINE', 'MOMLOT', 'SMTIME', 'LAMBDA_TOOL','LAMBDA_PART') 
        and ah.LOAD_DATE >= SYSDATE - 7
    '''
    
        lotcursor = conn.execute(myQuery)
        field_name = [field[0] for field in lotcursor.description]
        print("Query Completed...!")
        site_df = pd.DataFrame(lotcursor.fetchall(), columns=field_name)
   
        combined_df = pd.concat([combined_df, site_df], axis = 0)           
    return  combined_df

def PivotRawData(df):
   df.fillna('[NULL]', inplace=True) #fill empty cells with strings
   columns = df.columns
   index_col = columns.drop(['ATTRIBUTE_NAME', 'ATTRIBUTE_VALUE']).to_list()

   df_pivot = df.pivot_table(index = index_col , columns = 'ATTRIBUTE_NAME', values = 'ATTRIBUTE_VALUE', aggfunc=lambda x: ' '.join(str(v) for v in x)) 
   df_pivot.drop_duplicates(keep = 'first', inplace = True)
   df_pivot.reset_index(inplace = True)
   
   #We can remove all rows with UPTIME empty - eliminates empty chambers rows
   df_pivot = df_pivot.loc[df_pivot['UPTIME'] != '[NULL]']
   return df_pivot
   

def create_df_batchid_waferid_lotdata(df_pivot):
    list_mes_slots, list_mes_wafer_ids = df_pivot['MES_SLOTS'].str.split(','), df_pivot['MES_WAFER_IDS'].str.split(',') 
    series_mes_slots, series_mes_wafer_ids = pd.Series(list_mes_slots.explode().tolist(), name='MES_SLOTS'), pd.Series(list_mes_wafer_ids.explode().tolist(), name='MES_WAFER_IDS')
    new_df = pd.concat([series_mes_slots, series_mes_wafer_ids], axis=1)
    list_num_of_wfr = [len(i) for i in df_pivot['MES_SLOTS'].str.split(',')]

    list_lot_data = ['FB_METRODATA3_IDX', 'FB_METRODATA3', 'FB_METRODATA2_IDX', 'FB_METRODATA2', 'FB_METRODATA_IDX', 'FB_METRODATA', 'WAFERS3_ACT_IDX', 'WAFERS3_ACT', 'WAFERS2_ACT_IDX', 'WAFERS2_ACT', 'WAFERS1_ACT_IDX', 'WAFERS1_ACT', 'TARGET', 'METRO3AVGLOT', 'METRO2AVGLOT', 'METROAVGLOT', 'SETTING_USED', 'LOTSETTINGS', 'B_PART_RS', 'B_PART', 'B_PART_PRIOR', 'B_TOOL_RS', 'B_TOOL', 'B_TOOL_PRIOR', 'BATCH_ID_SUBENTITY', 'LAMBDA_TOOL', 'LAMBDA_PART', 'OPENRUNS', 'OPENRUNS_PART', 'SUBENTITY', 'PRODGROUP', 'PROCESS_OPN', 'APC_LOAD_DATE', 'PRODUCT', 'RSTIME', 'CHAMBER_IDX', 'CHAMBER', 'MACHINE', 'ROUTE', 'PROCESS', 'LOTID', 'AREA']
    list_lot_data_to_split = ['TARGET', 'METRO3AVGLOT', 'METRO2AVGLOT', 'METROAVGLOT', 'SETTING_USED', 'B_PART_RS', 'B_PART', 'B_PART_PRIOR', 'B_TOOL_RS', 'B_TOOL', 'B_TOOL_PRIOR']  
    list_lot_data_to_split_1 = ['LOTSETTINGS']
    for i in list_lot_data: 
        if i in  df_pivot.columns.to_list():
            # serilize lot level data to wafer level
           series_lot_data = df_pivot[i].repeat(list_num_of_wfr).reset_index(drop=True)
            # concat 
           new_df = pd.concat([series_lot_data, new_df], axis=1)
           if i in list_lot_data_to_split:
               df_splited_columns = new_df[i].str.split(',', expand=True)
               df_splited_columns.columns = [f'{i}_{j + 1}' for j in range(df_splited_columns.shape[1])]
                # place [NULL] to np.nan
               df_splited_columns = df_splited_columns.replace('[NULL]', np.nan)  
            # concat
               new_df = pd.concat([df_splited_columns, new_df], axis=1) 
           if i in list_lot_data_to_split_1:
            # split to a tmp df
                df_splited_columns = new_df[i].str.split('|', expand=True)
            # rename column (e.g., LOTSETTINGS_1, LOTSETTINGS_2, LOTSETTINGS_3)
                df_splited_columns.columns = [f'{i}_{j + 1}' for j in range(df_splited_columns.shape[1])]
            # place [NULL] to np.nan
                df_splited_columns = df_splited_columns.replace('[NULL]', np.nan)  
            # concat
                new_df = pd.concat([df_splited_columns, new_df], axis=1)    
                
    return new_df

output_path = "//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/"

    
###### Real Time Data Extract ##################
DF = DataExtractFromXEUS()
#DF.to_csv(output_path+"RawExtractDataAEPC.csv", index = False)
########################################################################################################################
#DF = pd.read_csv(output_path+"RawExtractDataAEPC.csv")

DF_pivot = PivotRawData(DF)
LotData = create_df_batchid_waferid_lotdata(DF_pivot)




#Save output for debug
LotData.to_csv(output_path+"AEPCLotData.csv", index = False)
