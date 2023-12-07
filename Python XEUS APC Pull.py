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
        select distinct ah.LOAD_DATE, fac.FACILITY ,ah.APC_OBJECT_NAME, ah.LOT, ah.OPERATION ,
        VARCHAR(ah.ONLINE_ROW_ID) as ONLINE_ROW_ID, 
        ad.ATTRIBUTE_NAME, ad.ATTRIBUTE_VALUE
        
        from P_APC_TXN_HIST  ah
        inner join P_APC_TXN_DATA ad on ad.APC_DATA_ID = ah.APC_DATA_ID
        CROSS JOIN F_FACILITY fac
        
        where ah.APC_OBJECT_NAME = 'AEPCMC_LOT'
        and ah.APC_OBJECT_TYPE = 'LOT'
        and ah.LAST_VERSION_FLAG = 'Y'
        and ad.ATTRIBUTE_NAME In ('AREA','LOTID','ROUTE','PROCESS','OPERATION','MES_WAFER_IDS','MES_SLOTS','SLOTS','PROCESS_OPN','PRODGROUP','PRODUCT'
                                                        ,'SUBENTITY','SUBENTITIES','UPDATE_TIME','B_TOOL_PRIOR','B_TOOL_RS','B_TOOL','B_PART_PRIOR','B_PART_RS'
                                                        ,'B_PART','SETTING_USED','LOTSETTINGS','WAFERSETTINGS','FF_SUC','FB_SUC','CALCULATED_SETTING','OPENRUNS'
                                                        ,'OPENRUNS_PART','METROAVGBYWAFER','METROAVGLOT','TARGET','FB_METRODATA','FB_METRODATA_IDX'
                                                        ,'FB_METRODATA2','FB_METRODATA2_IDX','FB_METRODATA3','FB_METRODATA3_IDX','FB_TARGET'
                                                        ,'WAFERS1_ACT','WAFERS1_ACT_IDX','WAFERS2_ACT','WAFERS2_ACT_IDX','WAFERS3_ACT','WAFERS3_ACT_IDX','LAMBDA_TOOL_USED'
                                                        ,'LAMBDA_PART_USED','PM_COUNTER_PRIOR','PM_COUNTER','REFERENCE_SETTING','M_ETCHRATE','METRO_LOLIMIT','METRO_HILIMIT'
                                                        ,'BATCH_ID','RSTIME','SHORTWAFERIDS','CHAMBER','CHAMBER_IDX','VALIDDATA','APC_DATA_ID','UPTIME','METROAVG_CHBR'
                                                        ,'MACHINE', 'MOMLOT', 'SMTIME', 'LAMBDA_TOOL','LAMBDA_PART') 
        and ah.LOAD_DATE >= SYSDATE - 2
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
   
   return df_pivot

def DataQualityChecks(df_pivot):
    empty_slots = df_pivot[df_pivot['MES_SLOTS'].isnull()].index.tolist()
    print("empty slots are: ",empty_slots)
    
    empty_chambers = df_pivot[df_pivot['CHAMBER'] == '[NULL]'].index.tolist()
    print("empty chambers are: ", len(empty_chambers))
    
    empty_batch_id = df_pivot[df_pivot['BATCH_ID'].isnull()].index.tolist()
    print("total empty batch id rows:", len(empty_batch_id))
    
  
    # #removing rows with empty batch id and chambers
    df_pivot_checked = df_pivot
    df_pivot_checked.dropna(subset = ['BATCH_ID'], inplace = True)
    df_pivot_checked.drop(df_pivot[df_pivot.CHAMBER == '[NULL]'].index, inplace = True)
    
      
    #key definition for parsing
    df_pivot_checked['KEY'] = df_pivot_checked['BATCH_ID'] + "_" + df_pivot_checked['SUBENTITY']
    df_pivot.fillna('[NULL]', inplace=True)
    
    #Need to check if any UPTIME empty cells
    
    return df_pivot_checked

def WaferChamberAssociation(CHAMBER,MES_SLOTS,SUBENTITY):
    underscore = SUBENTITY.find('_')
    SUBENTITY3 = SUBENTITY[underscore+1:]
    
    CHAMBER_LIST = CHAMBER.split(',')
    MES_SLOTS_LIST = MES_SLOTS.split(',')
        
    CHAMBERS_SLOTS = [MES_SLOTS_LIST[idx] for idx, chamber in enumerate(CHAMBER_LIST) if chamber == SUBENTITY3]
    
    
    return CHAMBERS_SLOTS
    

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
    WLV['OPERATION'] = df['OPERATION']
    ##################################
    
    
    #####################################  Wafer Chamber Association #################################
    
    WLV['PC_MES_SLOTS_DEBUG'] = WaferChamberAssociation(df['CHAMBER'],df['MES_SLOTS'],df['SUBENTITY'])
    WLV['PC_WID'] = WaferChamberAssociation(df['CHAMBER'],df['MES_WAFER_IDS'],df['SUBENTITY'])
  
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
    for pc_slot in WLV['PC_MES_SLOTS_DEBUG']:
        if pc_slot in slot_metro:
            WLV['PC_METRO_DATA'].append(slot_metro[pc_slot])
        else:
            WLV['PC_METRO_DATA'].append("")
        
    
    ##############################  APC settings conversion by lot ###################
    try: 
        WLV['B_PART'], WLV['B_PART_1'], WLV['B_PART_2'], WLV['B_PART_3'],WLV['B_PART_4'],WLV['B_PART_5'],WLV['B_PART_6'],WLV['B_PART_7']= ColumnDecomposition(df['B_PART'], 'B_PART')
    except TypeError:
        print(df['KEY'])
        print(df['B_PART'])
        
        
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
        
   
    WLV['WFR1_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOTS_DEBUG'],  wfr1_act, wfr1_act_idx, df['UPTIME'])
    WLV['WFR2_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOTS_DEBUG'],  wfr2_act, wfr2_act_idx, df['UPTIME'])
    WLV['WFR3_ACT_BY_SLOT'] = WafersACTValuesBySlot(WLV['PC_MES_SLOTS_DEBUG'],  wfr3_act, wfr3_act_idx, df['UPTIME'])
             

    return WLV



def WaferLevelDataConstruction(wlv_lst):
    wafer_level_long_df = pd.DataFrame()
    
    for item in wlv_lst:
        temp_df = pd.DataFrame(item)    
        wafer_level_long_df = pd.concat([wafer_level_long_df,temp_df], ignore_index=True)
    return wafer_level_long_df

    
###### Real Time Data Extract ##################
DF = DataExtractFromXEUS()
DF.to_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/RawExtractData.csv", index = False)
########################################################################################################################
# DF = pd.read_csv("//ORshfs.intel.com/ORanalysis$/1274_MAODATA/GAJT/WIJT/ByPath/GER_fdoktorm/DeconstructionTest/RawExtractData.csv")
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
print(f"Starting Pivot  {now}")
DF_pivot = PivotRawData(DF)
now = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")    
#######################  NEED VALIDATION #################################################################################
#print(f"Starting Quality Check  {now}")
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



