# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 14:23:45 2023

@author: fdoktorm
"""

import PyUber
import pandas as pd

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
      
    where ah.APC_OBJECT_NAME = :apc_obj_name
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
    and ah.LOAD_DATE >= SYSDATE - :days_back
'''

    lotcursor = conn.execute(myQuery, days_back = 0.5, apc_obj_name = 'AEPC_LOT')
    field_name = [field[0] for field in lotcursor.description]
    #print("Query Completed...!")
    site_df = pd.DataFrame(lotcursor.fetchall(), columns=field_name)   
    combined_df = pd.concat([combined_df, site_df], axis = 0)       
    custom_logger.info(f"Data extract from {site} completed")