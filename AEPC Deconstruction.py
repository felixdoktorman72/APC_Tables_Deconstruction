# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 09:37:05 2023

@author: fdoktorm
"""

import PyUber
import pandas as pd
import numpy as np
#from pathlib import Path
from datetime import datetime  
from collections import Counter
import logging

# Create a custom logger
custom_logger = logging.getLogger("custom_logger")
custom_logger.setLevel(logging.INFO)

# Create a handler and set the formatter
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)


# Add the handler to the logger
custom_logger.addHandler(handler)


def DataExtractFromXEUS():
    #Connection sites definition
    sites = ["F28_PROD_XEUS", "F32_PROD_XEUS"]
    combined_df = pd.DataFrame()
    for site in sites:
        #now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
        #print(f"Connecting to {site}  {now}")
        custom_logger.info(f"Connecing to {site}")
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
        and ah.LOAD_DATE >= SYSDATE - 1
    '''
    
        lotcursor = conn.execute(myQuery)
        field_name = [field[0] for field in lotcursor.description]
        #print("Query Completed...!")
        site_df = pd.DataFrame(lotcursor.fetchall(), columns=field_name)   
        combined_df = pd.concat([combined_df, site_df], axis = 0)       
        custom_logger.info(f"Data extract from {site} completed")
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
   df_pivot['BATCH_ID_SUBENTITY'] = df_pivot['BATCH_ID'] + "_" + df_pivot['SUBENTITY']     
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

def add_WAFERSx_ACT_data(new_df):
    #Adding WAFERSx_ACT
    dic_to_parse = {'WAFERS3_ACT_IDX':'WAFERS3_ACT', 'WAFERS2_ACT_IDX':'WAFERS2_ACT','WAFERS1_ACT_IDX':'WAFERS1_ACT' , 'CHAMBER_IDX':'CHAMBER' }
    for key, value in dic_to_parse.items():
        if key in new_df.columns.to_list():
            new_df[key] = new_df[key].apply(lambda x: x.split(';') if isinstance(x, str) else x)
            new_df[value] = new_df[value].apply(lambda x: x.split(',') if isinstance(x, str) else x)
            new_df[key] = new_df[key].apply(lambda x: [int(i) if i not in ['nan', '[NULL]', 'NONE'] else np.nan for i in x] if isinstance(x, list) else x) 
            if key == 'CHAMBER_IDX':
                new_df[value] = new_df[value].apply(lambda x: [str(i) if i not in ['nan', '[NULL]'] else np.nan for i in x] if isinstance(x, list) else x)
                continue
            new_df[value] = new_df[value].apply(lambda x: [float(i) if i not in ['nan', '[NULL]'] else np.nan for i in x] if isinstance(x, list) else x)
    # choose the correct value (for each wafer) based on MES slot of the wafer 
    for row_index, row_data in new_df.iterrows():
        for key in dic_to_parse:
            if key in new_df.columns.to_list():
                # replace np.nan, [0], [np.nan] to np.nan
                if row_data[key] in [np.nan, [0], [np.nan]]:
                    new_df.loc[row_index, key] = np.nan
                    new_df.loc[row_index, dic_to_parse[key]] = np.nan
                    continue
                # replace 'list of values for all wfr' to the specific value of the wafer based on MES slot
                for i in range(0, len(row_data[key])):
                    if row_data[key][i] == int(row_data['MES_SLOTS']):
                        new_df.loc[row_index, key] = row_data[key][i]
                        new_df.loc[row_index, dic_to_parse[key]] = row_data[dic_to_parse[key]][i]
                        break
                # for wfrs not having data, set to np.nan. 20231116
                if '[' in str(new_df.at[row_index, dic_to_parse[key]]): 
                    new_df.at[row_index, key] = np.nan 
                    new_df.at[row_index, dic_to_parse[key]] = np.nan 
    return new_df

# Format SPC metro data to add two new columes (in below) to data table.
# 1) list of number of sites for each measurement. 
# 2) list of slots having metro for each measurement.
def add_fb_metrodatax_data(new_df):
    list_fb_metrodatax_to_split = ['FB_METRODATA3', 'FB_METRODATA2', 'FB_METRODATA'] 
    for fb_metrodatax in list_fb_metrodatax_to_split:
        if fb_metrodatax in new_df.columns.to_list():
            # variable names
            fb_metrodatax_idx = fb_metrodatax + '_IDX'
            fb_metrodatax_idx_num_meas = fb_metrodatax_idx + '_num_meas'  # FB_METRODATAx_IDX_num_meas (will add to table)
            fb_metrodatax_idx_slot = fb_metrodatax + '_slot'              # FB_METRODATAx_slot (will add to table)
            # get list of number of sites for each measurement
            new_df[fb_metrodatax_idx_num_meas] = new_df[fb_metrodatax_idx]
            new_df[fb_metrodatax_idx_num_meas] = new_df[fb_metrodatax_idx_num_meas].apply(lambda x: x.split(';') if isinstance(x, str) else x) 
            new_df[fb_metrodatax_idx_num_meas] = new_df[fb_metrodatax_idx_num_meas].apply(lambda x: list(Counter(list([int(j.split(',')[0]) if j not in ['nan', '[NULL]'] else np.nan for j in x])).values()) if isinstance(x, list) else x)
            # get list of slots having metro for each measurement.
            new_df[fb_metrodatax_idx_slot] = new_df[fb_metrodatax_idx].apply(lambda x: x.split(';') if isinstance(x, str) else x)
            new_df[fb_metrodatax_idx_slot] = new_df[fb_metrodatax_idx_slot].apply(lambda x: list(dict.fromkeys([int(j.split(',')[0]) if j not in ['nan', '[NULL]'] else np.nan for j in x])) if isinstance(x, list) else x)
    return new_df 

# Process FB_METRODATAx to make it wafer level.
def process_fb_metrodatax_data(new_df):
    # dic ('FB_METRODATAx':'FB_METRODATAx_IDX_num_meas') that required to parse.
    dic = {'FB_METRODATA':'FB_METRODATA_IDX_num_meas', 'FB_METRODATA2':'FB_METRODATA2_IDX_num_meas', 'FB_METRODATA3':'FB_METRODATA3_IDX_num_meas'}
    # parse FB_METRODATAx and FB_METRODATAx_IDX_num_meas to list of float.
    for key in dic:
        if key in new_df.columns.to_list():
            new_df[key] = new_df[key].apply(lambda x: [float(i) for i in x.split(',') if i != '[NULL]'] if isinstance(x, str) else x)    
            new_df[dic[key]] = new_df[dic[key]].apply(lambda x: [float(i) for i in x.split(',') if i != '[NULL]'] if isinstance(x, str) else x)
    # calculate mean of metro data by num of measured wafers/sites. 
    for row_index, row_data in new_df.iterrows():
        for key in dic:
            if key in new_df.columns.to_list():
                # replace np.nan, [0], [np.nan] to np.nan
                if row_data[key] in [np.nan, [0], [np.nan], '[NULL]']:
                    new_df.loc[row_index, key] = np.nan
                    continue
                # replace np.nan, [0], [np.nan] to np.nan
                if row_data[dic[key]] in [np.nan, [0], [np.nan], '[NULL]']:
                    new_df.loc[row_index, key] = np.nan 
                    continue
                # calculate mean of metro data by num of measured wafers/sites.
                if len(row_data[key]) > 1:
                    res = []
                    for i in range(len(row_data[dic[key]])):
                        start = sum(row_data[dic[key]][:i])
                        end = start + row_data[dic[key]][i]
                        res.append(np.mean(row_data[key][start:end])) 
                    new_df.at[row_index, key] = res
    # dic ('FB_METRODATAx_slot':'FB_METRODATAx') that required to parse
    dic_to_parse = {'FB_METRODATA_slot':'FB_METRODATA', 'FB_METRODATA2_slot':'FB_METRODATA2', 'FB_METRODATA3_slot':'FB_METRODATA3'}
    # parse FB_METRODATAx_slot and FB_METRODATAx to list of int and list of float
    for key, value in dic_to_parse.items():
        if value in new_df.columns.to_list():
            new_df[key] = new_df[key].apply(lambda x: x.split(',') if isinstance(x, str) else x)
            new_df[value] = new_df[value].apply(lambda x: x.split(',') if isinstance(x, str) else x)
            new_df[key] = new_df[key].apply(lambda x: [int(i) if i not in ['nan', '[NULL]', 'NaN', np.nan] else np.nan for i in x] if isinstance(x, list) else x) 
            new_df[value] = new_df[value].apply(lambda x: [float(i) if i not in ['nan', '[NULL]', 'NaN', np.nan] else np.nan for i in x] if isinstance(x, list) else x)
    # choose the correct value (for each wafer) based on MES slot of the wafer 
    for row_index, row_data in new_df.iterrows():
        for key in dic_to_parse:
            if key in new_df.columns.to_list():
                # replace np.nan, [0], [np.nan] to np.nan
                if row_data[key] in [np.nan, [0], [np.nan], '[NULL]']:
                    new_df.loc[row_index, key] = np.nan
                    new_df.loc[row_index, dic_to_parse[key]] = np.nan
                    continue
                # replace 'list of values for all wfr' to the specific value of the wafer based on MES slot
                for i in range(0, len(row_data[key])):
                    if row_data[key][i] == int(row_data['MES_SLOTS']):
                        new_df.loc[row_index, key] = row_data[key][i]
                        new_df.loc[row_index, dic_to_parse[key]] = row_data[dic_to_parse[key]][i]
                        break
                # for wfrs not having metrology, set their FB_METRODATAx_slot and FB_METRODATAx to np.nan.
                if '[' in str(new_df.at[row_index, dic_to_parse[key]]): 
                    new_df.at[row_index, key] = np.nan 
                    new_df.at[row_index, dic_to_parse[key]] = np.nan            
    return new_df

# Matching CHAMBER and SUBENTITY to remove redundant rows
def match_chamber_to_subentity(new_df):
    new_df['SUBENTITY_split'] = new_df['SUBENTITY'].str.split("_").str[-1]
    new_df = new_df[(new_df['CHAMBER'] == new_df['SUBENTITY_split']) | (new_df['CHAMBER'].isna())]  
    return new_df

# Standardize RSTIME
def convert_date_format(date):  
    try:  
        # Try to convert date from the first format  
        dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%SZ")  
    except ValueError:  
        try:  
            # If the first format fails, try the second format  
            dt = datetime.strptime(date, "%m/%d/%Y %I:%M:%S %p")  
        except ValueError:  
            # If both formats fail, return the original date  
            return date    
    # Convert the datetime object to the desired format  
    return dt.strftime("%m/%d/%Y %I:%M:%S %p")


output_path = "//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/AEPC/"

    
###### Real Time Data Extract ##################
DF = DataExtractFromXEUS()
DF.to_csv(output_path+"RawExtractDataAEPC.csv", index = False)
################################################################
#DF = pd.read_csv(output_path+"RawExtractDataAEPC.csv")


custom_logger.info("Data Manipulation Starts")
DF_pivot = PivotRawData(DF)
LotData = create_df_batchid_waferid_lotdata(DF_pivot)
LotWaferData = add_WAFERSx_ACT_data(LotData)
LotWaferData = add_fb_metrodatax_data(LotWaferData)
LotWaferData = process_fb_metrodatax_data(LotWaferData)
LotWaferData = match_chamber_to_subentity(LotWaferData)
LotWaferData.loc[:, 'RSTIME'] = LotWaferData['RSTIME'].apply(convert_date_format)
custom_logger.info("Data Manipulation Finished")







#Save output for debug
custom_logger.info("Starting Save LVL Pivot data to Server")
DF_pivot.to_csv(output_path+"AEPCPivot.csv", index = False)
custom_logger.info("LVL Pivot data to Server Saved")


custom_logger.info("Starting Save WVL data to Server")
LotData.to_csv(output_path+"AEPCLotData.csv", index = False)
LotWaferData.to_csv(output_path+"AEPCLotWaferData.csv", index = False)
custom_logger.info("WLV data to Server Saved")