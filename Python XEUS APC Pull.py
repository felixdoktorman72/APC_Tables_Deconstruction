# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 16:15:24 2023

@author: fdoktorm
"""

import PyUber
import pandas as pd
import numpy as np
from pathlib import Path
import datetime
# import json


#Output path need to define function
#\\ORshfs.intel.com\ORanalysis$\1274_MAODATA\GAJT\WIJT\ByPath\GER_fdoktorm\
def ColumnDecomposition(column, column_name):
    wlv = {}
    wlv[column_name] = column
    
    try: 
        column_split = column.split(',')
    except AttributeError: 
        column_split = ['']
    except TypeError:
        column_split = ['']
       
    if len(column_split) == 1:
       return(wlv[column_name],column_split[0],'','','','','','')      
    elif len(column_split) == 2:
        return(wlv[column_name],column_split[0],column_split[1],'','','','','')        
    elif len(column_split) == 3:
        return(wlv[column_name],column_split[0],column_split[1],column_split[2],'','','','')
    elif len(column_split) == 4:
        return(wlv[column_name],column_split[0],column_split[1],column_split[2],column_split[3],'','','')
    elif len(column_split) == 5:
        return(wlv[column_name],column_split[0],column_split[1],column_split[2],column_split[3],column_split[4],'','' )
    elif len(column_split) == 6:
        return(wlv[column_name],column_split[0],column_split[1],column_split[2],column_split[3],column_split[4],column_split[5],'' )
    elif len(column_split) == 7:
        return(wlv[column_name],column_split[0],column_split[1],column_split[2],column_split[3],column_split[4],column_split[5],column_split[6] )
        
def DataExtractFromXEUS():
    #Connection sites definition
    sites = ["F28_PROD_XEUS", "F32_PROD_XEUS"]
    combined_df = pd.DataFrame()
    for site in sites:
        now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
        print(f"Connecting to {site}  {now}")
        conn = PyUber.connect(datasource=site)

        myQuery='''
        SELECT  DISTINCT 
              facility AS facility
             ,apc_object_name AS apc_object_name
             ,To_Char(change_date,'yyyy-mm-dd hh24:mi:ss') AS change_date
             ,operation AS operation
             ,attribute_name AS attribute_name
             ,attribute_value AS attribute_value
             ,apc_job_id AS apc_job_id
             FROM
             (
                 
                 SELECT  
                 fac.facility AS facility
                 ,ah.apc_object_name AS apc_object_name
                 ,ah.change_time AS change_date
                 ,ah.operation AS operation
                 ,ad.attribute_name AS attribute_name
                 ,ad.attribute_value AS attribute_value
                 ,ah.apc_job_id AS apc_job_id
                 FROM 
                 P_APC_Txn_Hist ah
                 INNER JOIN P_APC_Txn_Data ad ON ad.apc_data_id = ah.apc_data_id
                 CROSS JOIN F_FACILITY fac
                 WHERE
                 ah.last_version_flag = 'Y' 
                 AND      ah.apc_object_type = 'LOT' 
                 AND      ah.apc_object_name Like 'AEPCMC_LOT' 
                 AND      ad.attribute_name In ('AREA','LOTID','ROUTE','PROCESS','OPERATION','MES_WAFER_IDS','MES_SLOTS','SLOTS','PROCESS_OPN','PRODGROUP','PRODUCT'
                                                ,'SUBENTITY','SUBENTITIES','UPDATE_TIME','B_TOOL_PRIOR','B_TOOL_RS','B_TOOL','B_PART_PRIOR','B_PART_RS'
                                                ,'B_PART','SETTING_USED','LOTSETTINGS','WAFERSETTINGS','FF_SUC','FB_SUC','CALCULATED_SETTING','OPENRUNS'
                                                ,'OPENRUNS_PART','METROAVGBYWAFER','METROAVGLOT','TARGET','FB_METRODATA','FB_METRODATA_IDX','FB_TARGET'
                                                ,'WAFERS1_ACT','WAFERS1_ACT_IDX','WAFERS2_ACT','WAFERS2_ACT_IDX','WAFERS3_ACT','WAFERS3_ACT_IDX','LAMBDA_TOOL_USED'
                                                ,'LAMBDA_PART_USED','PM_COUNTER_PRIOR','PM_COUNTER','REFERENCE_SETTING','M_ETCHRATE','METRO_LOLIMIT','METRO_HILIMIT'
                                                ,'BATCH_ID','RSTIME','SHORTWAFERIDS','CHAMBER','CHAMBER_IDX','VALIDDATA','APC_DATA_ID','UPTIME','METROAVG_CHBR'
                                                ,'MACHINE', 'MOMLOT', 'SMTIME', 'LAMBDA_TOOL','LAMBDA_PART') 
                                                 AND ah.change_time >= SYSDATE - 1)
    '''
    
        lotcursor = conn.execute(myQuery)
        field_name = [field[0] for field in lotcursor.description]
        print("Query Completed...!")
        site_df = pd.DataFrame(lotcursor.fetchall(), columns=field_name)
   
        combined_df = pd.concat([combined_df, site_df], axis = 0)
    
    return  combined_df

def PivotRawData(df):
    df.fillna('[NULL]', inplace=True) #fill empty cells with strings
    index_col = ['FACILITY','APC_OBJECT_NAME','CHANGE_DATE','OPERATION','APC_JOB_ID']
    df_pivot = df.pivot_table(index = index_col , columns = 'ATTRIBUTE_NAME', values = 'ATTRIBUTE_VALUE', aggfunc=lambda x: ' '.join(x)) 
    df_pivot.drop_duplicates(keep = 'first', inplace = True)
    
    return df_pivot

def DataQualityChecks(df_pivot):
    empty_slots = df_pivot[df_pivot['MES_SLOTS'].isnull()].index.tolist()
    print("empty slots are: ",empty_slots)
    
    empty_chambers = df_pivot[df_pivot['CHAMBER'] == '[NULL]'].index.tolist()
    print("empty chambers are: ", len(empty_chambers))
    
    empty_batch_id = df_pivot[df_pivot['BATCH_ID'].isnull()].index.tolist()
    print("total empty batch id rows:", len(empty_batch_id))
    
    #removing rows with empty batch id and chambers
    df_pivot_checked = df_pivot
    df_pivot_checked.dropna(subset = ['BATCH_ID'], inplace = True)
    df_pivot_checked.drop(df_pivot[df_pivot.CHAMBER == '[NULL]'].index, inplace = True)
    
    #key definition for parsing
    df_pivot_checked['KEY'] = df_pivot_checked['BATCH_ID'] + "_" + df_pivot_checked['SUBENTITY']
    df_pivot.fillna('[NULL]', inplace=True)
    
    #Need to check if any UPTIME empty cells
    
    return df_pivot_checked
 
def WafersACTValuesBySlot(PC_MES_SLOTS, WAFERS_ACT, WAFERS_ACT_IDX, uptime):    
    result = []
    if uptime != '[NULL]':
        for slot in PC_MES_SLOTS:
            try:
                # Find the index of the slot in WAFERS1_ACT_IDX
                index = WAFERS_ACT_IDX.index(slot)        
                # Use the index to pull the corresponding element from WAFERS1_ACT
                result.append(WAFERS_ACT[index])
            except ValueError:
                # Handle the case where the element is not found
                #print(f"Warning: Element {slot} not found in {WAFERS_ACT_IDX} Skipping.")
                result.append('')
    elif WAFERS_ACT[0] == '0':
        result = ['']*len(PC_MES_SLOTS)
    elif WAFERS_ACT[0] == np.nan:
        result = ['']*len(PC_MES_SLOTS)
    else:
        result = ['']*len(PC_MES_SLOTS)
        
    return result  

   
def WaferLevelData(df):
    
    #APC_UI_ORDER = []
    LVL_Columns=['AREA', 'BATCH_ID', 'B_PART', 'B_PART_PRIOR', 'B_PART_RS', 'B_TOOL',
           'B_TOOL_PRIOR', 'B_TOOL_RS', 'CALCULATED_SETTING', 'FB_SUC',
           'FB_TARGET', 'FF_SUC', 'LAMBDA_PART', 'LAMBDA_PART_USED', 'LAMBDA_TOOL',
           'LAMBDA_TOOL_USED', 'LOTID', 'LOTSETTINGS',  'METROAVGLOT', 'METROAVG_CHBR',
           'METRO_HILIMIT', 'METRO_LOLIMIT', 'MOMLOT', 'M_ETCHRATE', 'OPENRUNS',
           'OPENRUNS_PART', 'PM_COUNTER', 'PM_COUNTER_PRIOR', 'PROCESS',
           'PROCESS_OPN', 'PRODGROUP', 'PRODUCT', 'REFERENCE_SETTING', 'ROUTE',
           'RSTIME', 'SETTING_USED', 'SMTIME','SUBENTITIES', 'SUBENTITY', 'TARGET', 'UPDATE_TIME', 'UPTIME',
           'VALIDDATA', 'WAFERSETTINGS',  'KEY']
        
    
    
    WLV = {}
    WLV['LOT'] = df['LOTID']
    WLV['KEY'] = df['KEY']
    WLV['subentity'] = df['SUBENTITY']
    WLV['subentity_3'] = WLV['subentity'][-3:]
    WLV['CHAMBERS_LIST'] = df['CHAMBER']
    #####New addition for debug
    WLV['AREA'] = df['AREA']
    WLV['BATCH_ID'] = df['BATCH_ID']
    ##################################
    
    mes_slots , mes_wid=  df['MES_SLOTS'].split(',') , df['MES_WAFER_IDS'].split(',')
    #This can be local variable
    #WLV['MES_SLOTS'] = mes_slots
    
    
    chambers_list_by_slot = df['CHAMBER'].split(',') #list of all chambers by MES slots. In MC process contains multiple chambers
    if chambers_list_by_slot[0] == '[NULL]': #this case probably single chamber process        
        chamber_by_slot = [WLV['subentity_3']]*len(mes_slots) #to treat single chamber process when CHAMBER column is empty
    else:
        chamber_by_slot =  chambers_list_by_slot
    
    #Calculation slots and wafers per defined chamber
    pc_indices = [i for i, chamber in enumerate(chamber_by_slot) if chamber == WLV['subentity_3']]
    
    WLV['PC_MES_SLOT'] = [mes_slots[i] for i in pc_indices] #This variable should be local post debug
    WLV['PC_WID'] = [mes_wid[i] for i in pc_indices] #This variable should be local post debug



############################   New FB Metro Parsing #####################################################
    fb_metro_d = df['FB_METRODATA'].split(",")
    fb_metro_idx = df['FB_METRODATA_IDX'].split(";")
    
    slot_metro = {}
    for item in fb_metro_idx:
        slot = item.split(',')    
        if slot[0] not in slot_metro:
            slot_metro[slot[0]] = []
        slot_metro[slot[0]].append(fb_metro_d[fb_metro_idx.index(item)])

    for key in slot_metro:
        try:
            slot_metro[key] = np.mean(np.array(slot_metro[key], dtype = float))
        except ValueError:
            slot_metro[key] = np.nan

    #For debug
    #WLV['Slot Metro Data'] = slot_metro
    
    WLV['PC_METRO_DATA'] = []
    for pc_slot in WLV['PC_MES_SLOT']:
        if pc_slot in slot_metro:
            WLV['PC_METRO_DATA'].append(slot_metro[pc_slot])
        else:
            WLV['PC_METRO_DATA'].append("")
        
    
    ##############################  APC settings conversion by lot ###################
    WLV['B_PART'], WLV['B_PART_1'], WLV['B_PART_2'], WLV['B_PART_3'],WLV['B_PART_4'],WLV['B_PART_5'],WLV['B_PART_6'],WLV['B_PART_7']= ColumnDecomposition(df['B_PART'], 'B_PART')
    WLV['B_TOOL'], WLV['B_TOOL_1'], WLV['B_TOOL_2'], WLV['B_TOOL_3'],WLV['B_TOOL_4'],WLV['B_TOOL_5'],WLV['B_TOOL_6'],WLV['B_TOOL_7']= ColumnDecomposition(df['B_TOOL'], 'B_TOOL')
    WLV['B_PART_PRIOR'], WLV['B_PART_PRIOR_1'], WLV['B_PART_PRIOR_2'], WLV['B_PART_PRIOR_3'],WLV['B_PART_PRIOR_4'],WLV['B_PART_PRIOR_5'],WLV['B_PART_PRIOR_6'],WLV['B_PART_PRIOR_7']= ColumnDecomposition(df['B_PART_PRIOR'], 'B_PART_PRIOR')
    try: 
        WLV['B_TOOL_RS'], WLV['B_TOOL_RS_1'], WLV['B_TOOL_RS_2'], WLV['B_TOOL_RS_3'],WLV['B_TOOL_RS_4'],WLV['B_TOOL_RS_5'],WLV['B_TOOL_RS_6'],WLV['B_TOOL_RS_7'] = ColumnDecomposition(df['B_TOOL_RS'], 'B_TOOL_RS')
    except TypeError:
        print("problem is ", df['B_TOOL_RS'], df['KEY'])
    WLV['B_PART_RS'], WLV['B_PART_RS_1'], WLV['B_PART_RS_2'], WLV['B_PART_RS_3'],WLV['B_PART_RS_4'],WLV['B_PART_RS_5'],WLV['B_PART_RS_6'],WLV['B_PART_RS_7']  = ColumnDecomposition(df['B_PART_RS'], 'B_PART_RS')
    WLV['SETTING_USED'], WLV['SETTING_USED_1'], WLV['SETTING_USED_2'], WLV['SETTING_USED_3'],WLV['SETTING_USED_4'],WLV['SETTING_USED_5'],WLV['SETTING_USED_6'], WLV['SETTING_USED_7']  = ColumnDecomposition(df['SETTING_USED'], 'SETTING_USED')
    WLV['TARGET'], WLV['TARGET_1'], WLV['TARGET_2'], WLV['TARGET_3'],WLV['TARGET_4'],WLV['TARGET_5'],WLV['TARGET_6'],WLV['TARGET_7']  = ColumnDecomposition(df['TARGET'], 'TARGET')
    #Need to add B_TOOL
    
    try:
        wfr1_act, wfr1_act_idx = df['WAFERS1_ACT'].split(','), df['WAFERS1_ACT_IDX'].split(';')
    except AttributeError:
        wfr1_act = []              
        wfr1_act_idx = []
    try:
        wfr2_act, wfr2_act_idx = df['WAFERS2_ACT'].split(','), df['WAFERS2_ACT_IDX'].split(';')    
    except AttributeError:
        wfr2_act = []              
        wfr2_act_idx = []
    try:
        wfr3_act, wfr3_act_idx = df['WAFERS3_ACT'].split(','), df['WAFERS3_ACT_IDX'].split(';')     
    except AttributeError:
        wfr3_act = []              
        wfr3_act_idx = []
    except KeyError:
        wfr3_act = []              
        wfr3_act_idx = []
        
   
    WLV['WFR1_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOT'],  wfr1_act, wfr1_act_idx, df['UPTIME'])
    WLV['WFR2_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOT'],  wfr2_act, wfr2_act_idx, df['UPTIME'])
    WLV['WFR3_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOT'],  wfr3_act, wfr3_act_idx, df['UPTIME'])
             

    return WLV



def WaferLevelDataConstruction(wlv_lst):
    wafer_level_long_df = pd.DataFrame()
    
    for item in wlv_lst:
        temp_df = pd.DataFrame(item)    
        wafer_level_long_df = pd.concat([wafer_level_long_df,temp_df], ignore_index=True)
    return wafer_level_long_df

    
###### Real Time Data Extract ##################
DF = DataExtractFromXEUS()
DF.to_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/RawExtractData.csv")
########################################################################################################################
# DF = pd.read_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/RawExtractData.csv")
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Starting Pivot  {now}")
DF_pivot = PivotRawData(DF)
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Starting Quality Check  {now}")
DF_Pivot_Checked = DataQualityChecks(DF_pivot)
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Saving raw data to SD  {now}")
DF_Pivot_Checked.to_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/LotLevelValidationvsUI.csv", index = False)
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Starting WLV data parsing  {now}")
df_wlv = list(DF_Pivot_Checked.apply(WaferLevelData, axis = 1))
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Ending WLV data parsing  {now}")
WLV_data = WaferLevelDataConstruction(df_wlv)
WLV_data.to_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/FinalWaferLevelData.csv", index = False)








#output_file = Path("\\ORshfs.intel.com\ORanalysis$\1274_MAODATA\GAJT\WIJT\ByPath\GER_fdoktorm\APC_Data_extract.csv") #not working due to special char
#DF_pivot.to_csv(output_file)

