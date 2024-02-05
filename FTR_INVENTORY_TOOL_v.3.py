# ###########################################################################################################################################################################################
# ##############                                               BioUrja FTR Portfolio Construction Tool version 4.0                                                             ##############
# ###########################################################################################################################################################################################
# import logging
# import sys
# import os
# import pandas as pd
# import numpy as np
# import warnings
# warnings.filterwarnings("ignore")
# from datetime import datetime
# from datetime import timedelta
# import time
# import pyodbc
# import math
# import csv
# from tkinter import *
# import email
# import smtplib
# from dateutil.rrule import rrule, MONTHLY
# import bu_alerts
# from bu_snowflake import get_connection


# job_id=np.random.randint(1000000,9999999)

# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

# # log file location
# log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'FTR_INVENTORY_TOOL_LOG.txt'
# if os.path.isfile(log_file_location):
#     os.remove(log_file_location)

# logging.basicConfig(
#     level=logging.INFO, 
#     format='%(asctime)s [%(levelname)s] - %(message)s',
#     filename=log_file_location)

# # receiver_email = 'radha.waswani@biourja.com,shweta.srivastava@biourja.com,rahul.chaturvedi@biourja.com'
# receiver_email = "tushar.verma@biourja.com,bhavya.bhatt@biourja.com,srishti.sharma@biourja,pritish.jain@biourja.com,indiapowerit@biourja.com"
# # receiver_email = 'indiapowerit@biourja.com,DAPower@biourja.com'
# #receiver_email = 'rohit.pawar@biourja.com'

# # json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
# warehouse = 'QUANT_WH'
# try:
#     logging.info('Execution Started')
#     json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
#     bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB_DEV',status='Started',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=0, log=json, warehouse=warehouse,process_owner='RAHUL')
#     conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')     
#     def button4():
#         window_no_output.destroy()

#     def button3():
#         window_exit.destroy()

#     # folder_path = 'G:\\Gas & Pwr\\Pwr\\FTR\\Python Scripts\\Anil\\PQUANT\\FTRPortfolioTool_v5.0'
#     iso_list = ['MISO','PJMISO','ERCOT','SPPISO','NYISO','CAISO']
#     # iso_list = ['NYISO','NYISO']
#     Current_Obj = datetime.today().date() + timedelta(days=-3)

    
#     FLOW_MONTH = Current_Obj.strftime('%Y-%m')+'-01'
#     logging.info(f"===={FLOW_MONTH}======")
#     start_time = time.time()
    
#     # os.chdir(folder_path)  
#     RUN_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

#     dir2 = 'C://TEMP'
#     if not os.path.exists(dir2):
#         os.makedirs(dir2)
    
#     FTR_INV_QUERYTxt = '''SELECT AUCTION_NAME,ISO_SELL_FTR_ID,BU_ISO,FTR_PARTICIPANT,BU_TRADER,SOURCE_ISO_PNODE SOURCE_PNODE,LATEST_YES_SOURCE_PNODE SOURCE_ID,SINK_ISO_PNODE SINK_PNODE,LATEST_YES_SINK_PNODE SINK_ID,
#                         PEAK_TYPE BU_PEAK_TYPE,TRADE_TYPE BU_TRADE_TYPE,HEDGE_TYPE BU_HEDGE_TYPE,FLOW_MONTH,FLOW_MONTH FLOW_STARTDATE,last_day(FLOW_MONTH) FLOW_ENDDATE,HRCNT_FLOWMONTH,MW,MCP_PRICE_MWH,LTP_PRICE_MWH
#                         FROM PowerDB.PQUANT.FIM_PLUS_VW WHERE FLOW_MONTH = '{}' AND BU_ISO in {} AND (FTR_PARTICIPANT LIKE '%BIO%')'''.format(FLOW_MONTH,tuple(iso_list))
#     conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')  
#     cs=conn.cursor()
#     cs.execute(FTR_INV_QUERYTxt)       
#     FTR_INV_MONTHLY = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
#     conn.close()                        
#     FTR_INV_MONTHLY = FTR_INV_MONTHLY.rename(columns = {'FTR_PARTICIPANT':'ISO_FTR_PARTICIPANT','ISO_SELL_FTR_ID':'SELL_FTRID','MW':'FTR_MW','MCP_PRICE_MWH':'FTR_PRICE_MWH','HRCNT_FLOWMONTH':'HRS_IN_PERIOD'})
#     #  replacing this peace of code with anil provided code i.e  ticket 2393 
#     # FTR_AUC_DTLSTxt = '''SELECT *,row_number() over (partition by BU_ISO,AUCTION_DATE order by AUCTION_RESULTDATE desc) RNK  FROM PowerDB.PQUANT.BU_FTR_AUCTION_VW
#     #                     WHERE AUCTION_DATE = '{}' AND BU_ISO in {}
#     #                     and BU_AUCTION_TYPE = 'MONTHLY' order by AUCTION_NAME asc'''.format(FLOW_MONTH,tuple(iso_list))
#     FTR_AUC_DTLSTxt = '''SELECT *,row_number() over (partition by BU_ISO,flow_startdate order by AUCTION_RESULTDATE desc) RNK 
#                             FROM POWERDB.PQUANT.BU_FTR_AUCTION_VW
#                             WHERE flow_startdate = '{}' AND BU_ISO in {}
#                             order by AUCTION_NAME asc'''.format(FLOW_MONTH,tuple(iso_list))
#     conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
#     cs=conn.cursor()
#     cs.execute(FTR_AUC_DTLSTxt)       
#     FTR_AUC_DTLS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])  
#     conn.close()                        
#     FTR_AUC_DTLS = FTR_AUC_DTLS[FTR_AUC_DTLS.RNK == 1]

#     ######## If exists in database delete before insertion ...... 
#     FPT_ID_EXISTSTxt = '''select distinct(FPT_ID) from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' order by FPT_ID asc'''.format(FLOW_MONTH,tuple(iso_list))
#     conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
#     cs=conn.cursor()
#     cs.execute(FPT_ID_EXISTSTxt)       
#     FPT_ID_EXISTS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
#     conn.close()            
        
#     FPT_PATH_EXISTSTxt = '''select * from PowerDB.PQUANT.FPT_PATH_DETAILS where FPT_ID in 
#                             (select distinct(FPT_ID) from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY')'''.format(FLOW_MONTH,tuple(iso_list))
#     conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
#     cs=conn.cursor()
#     cs.execute(FPT_PATH_EXISTSTxt)       
#     FPT_PATH_EXISTS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
#     conn.close()   

#     if (FPT_ID_EXISTS.shape[0] > 0):    
#         DEL_FPT_PATH_EXISTSTxt = '''delete from PowerDB.PARCHIVE.FPT_PATH_DETAILS where FPT_ID in 
#                             (select distinct(FPT_ID) from PowerDB.PARCHIVE.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY')'''.format(FLOW_MONTH,tuple(iso_list))
#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PARCHIVE')   
#         cs=conn.cursor()
#         cs.execute(DEL_FPT_PATH_EXISTSTxt)       
#         conn.close()  
        
#         DEL_FPT_ID_EXISTSTxt = '''delete from PowerDB.PARCHIVE.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' and BU_ISO in {}'''.format(FLOW_MONTH,tuple(iso_list))
#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PARCHIVE')    
#         cs=conn.cursor()
#         cs.execute(DEL_FPT_ID_EXISTSTxt)         
#         conn.close() 
#         logging.info(f"Previous data deleted for ===={FLOW_MONTH}======")

#     if (FTR_INV_MONTHLY.shape[0] > 0):
#         FPT_RUN_DTLS = FTR_INV_MONTHLY[['BU_ISO','FLOW_STARTDATE','FLOW_ENDDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','ISO_FTR_PARTICIPANT','BU_TRADER']].drop_duplicates()
       
#        # commented these lines of code as suggested by anil  and replacing with new lines provided by anil  i.e  ticket 2393
#         # FPT_RUN_PARAMS = pd.merge(FPT_RUN_DTLS,FTR_AUC_DTLS[['BU_ISO','AUCTION_NAME','AUCTION_ROUND','AUCTION_RESULTDATE','AUCTION_DATE']],left_on=['FLOW_STARTDATE','BU_ISO'],right_on=['AUCTION_DATE','BU_ISO'],how='left')
#         # FPT_RUN_PARAMS['ALIAS'] = np.where(FPT_RUN_PARAMS.BU_TRADER == 'AO','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'ET','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'RW','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'PP','WB',np.where(FPT_RUN_PARAMS.BU_TRADER == 'KP & PP','WB',FPT_RUN_PARAMS.BU_TRADER)))))
#         # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'VR'
#         # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'VR'      
#         # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'NEMO'),'ALIAS'] = 'VR'
#         # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'FK'       
#         # FPT_RUN_PARAMS = FPT_RUN_PARAMS.drop(['AUCTION_DATE'],axis=1)


#         FPT_RUN_PARAMS = pd.merge(FPT_RUN_DTLS,FTR_AUC_DTLS[['BU_ISO','AUCTION_NAME','AUCTION_ROUND','AUCTION_RESULTDATE','FLOW_STARTDATE']],on=['FLOW_STARTDATE','BU_ISO'],how='left')
#         FPT_RUN_PARAMS['ALIAS'] = np.where(FPT_RUN_PARAMS.BU_TRADER == 'AO','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'ET','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'RW','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'PP','WB',np.where(FPT_RUN_PARAMS.BU_TRADER == 'KP & PP','WB',FPT_RUN_PARAMS.BU_TRADER)))))
#         FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'NEMO'
#         FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'NEMO'     
#         FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'VR'),'ALIAS'] = 'NEMO'
#         FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'FK' 
#         FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'FK'  


        
#         TCP_RUN_DATES = list()
#         TCP_ID_LISTS = list()
#         for ii in range(FPT_RUN_PARAMS.shape[0]):
#             #[TCP_ID,REPORT_DATE,TCP_IDs,tcpid_selection] = TKinterTCPIDSelection.main(errorlog,FPT_RUN_PARAMS['BU_ISO'][ii],FPT_RUN_PARAMS['ALIAS'][ii],FPT_RUN_PARAMS['FLOW_STARTDATE'][ii].strftime('%Y-%m-%d'),FPT_RUN_PARAMS['FLOW_ENDDATE'][ii].strftime('%Y-%m-%d'))        
#             BU_ISO = FPT_RUN_PARAMS['BU_ISO'][ii]
#             FLOW_STDT = FPT_RUN_PARAMS['FLOW_STARTDATE'][ii].strftime('%Y-%m-%d')
#             FLOW_EDDT = FPT_RUN_PARAMS['FLOW_ENDDATE'][ii].strftime('%Y-%m-%d')
#             trader_name = FPT_RUN_PARAMS['ALIAS'][ii]
            
#             if BU_ISO == 'MISO':

#                 TCP_IDQuery = '''select distinct TCP_ID,REPORT_DATE, RUN_REL_DAY from PowerDB.PMODEL.TRADER_CONSTRAINT_PLAYBOOK 
#                         where BU_ISO = \'''' + BU_ISO + '\'' + ''' and FLOW_STARTDATE = \'''' + FLOW_STDT + '\'' + ''' and FLOW_ENDDATE = \'''' + FLOW_EDDT + '\'' + ''' 
#                         and TRADER_ANALYST = 'FK' and BU_PEAK_TYPE NOT in ('24HR', 'HR17','HR7') order by REPORT_DATE desc'''   
#             else:    
#                 TCP_IDQuery = '''select distinct TCP_ID,REPORT_DATE, RUN_REL_DAY from PowerDB.PMODEL.TRADER_CONSTRAINT_PLAYBOOK 
#                         where BU_ISO = \'''' + BU_ISO + '\'' + ''' and FLOW_STARTDATE = \'''' + FLOW_STDT + '\'' + ''' and FLOW_ENDDATE = \'''' + FLOW_EDDT + '\'' + ''' 
#                         and TRADER_ANALYST = \'''' + trader_name + '\'' + ''' and BU_PEAK_TYPE NOT in ('24HR', 'HR17','HR7') order by REPORT_DATE desc'''             

#             conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')        
#             cs=conn.cursor()
#             cs.execute(TCP_IDQuery)       
#             TCP_IDs = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
#             conn.close()  

#             # TCP_ID = TCP_IDs.loc[0].TCP_ID
#             # REPORT_DATE = TCP_IDs.loc[0].REPORT_DATE
            
#             if (TCP_IDs.shape[0] == 0):
#                 TCP_RUN_DATES.append('1900-01-01 00:00:00.000')
#                 TCP_ID_LISTS.append('NULL')
#             else: 
#                 TCP_ID = TCP_IDs.loc[0].TCP_ID
#                 REPORT_DATE = TCP_IDs.loc[0].REPORT_DATE   
#                 TCP_RUN_DATES.append(str(REPORT_DATE))
#                 TCP_ID_LISTS.append(str(TCP_ID))
        
#         if len(TCP_RUN_DATES) == 1:
#             FPT_RUN_PARAMS['TCP_REPORT_DATE'] = TCP_RUN_DATES[0]
#             FPT_RUN_PARAMS['TCP_ID'] = TCP_ID_LISTS[0]       
#         else :
#             FPT_RUN_PARAMS['TCP_REPORT_DATE'] = TCP_RUN_DATES
#             FPT_RUN_PARAMS['TCP_ID'] = TCP_ID_LISTS
    

#         FPT_RUN_PARAMS['RUN_DATE'] = RUN_DATETIME
#         FPT_RUN_PARAMS['TOOL_VERSION'] = 'FTR Inventory'
#         FPT_RUN_PARAMS['FPT_STRATEGY'] = 'DA_PnL_Tracker'

#         FPT_RUN_PARAMS['RUN_PARAMETERS'] = 'BU_ISO:'+FPT_RUN_PARAMS['BU_ISO']+'~'+'AuctionName:'+FPT_RUN_PARAMS['AUCTION_NAME']+'~'+'FLOW_ST:'+ pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%Y-%m-%d')+'~'+'FLOW_END:'+ pd.to_datetime(FPT_RUN_PARAMS['FLOW_ENDDATE']).dt.strftime('%Y-%m-%d')
#         FPT_RUN_PARAMS['FILENAME'] = FPT_RUN_PARAMS['BU_ISO']+'_'+str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%b')[0])+'_'+FPT_RUN_PARAMS['BU_TRADER']+'_'+FPT_RUN_PARAMS['BU_TRADE_TYPE']
#         FPT_RUN_PARAMS['CALCULATE_DAILY_YN'] = 'Y'
#         FPT_RUN_PARAMS['RUN_STATUS'] = 'TRANSACTION_INVENTORY_MONTHLY'
#         FPT_RUN_PARAMS['PORTFOLIO_NAME'] = 'FTR - For PNL ' + str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%B')[0]) + ' ' + str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%Y')[0]) + ' ' + FPT_RUN_PARAMS['BU_ISO'] + ' ' + FPT_RUN_PARAMS['ISO_FTR_PARTICIPANT'] + ' ' + FPT_RUN_PARAMS['BU_TRADER']
#         FPT_RUN_PARAMS = FPT_RUN_PARAMS.rename(columns = {'FTR_PARTICIPANT':'ISO_FTR_PARTICIPANT','AUCTION_RESULTDATE':'TRADE_DATE'})  
#         FPT_RUN_PARAMS_INSERT = FPT_RUN_PARAMS.copy()
#         FPT_RUN_PARAMS_INSERT['FLOW_STARTDATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['FLOW_STARTDATE'])
#         FPT_RUN_PARAMS_INSERT['FLOW_ENDDATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['FLOW_ENDDATE'])
#         FPT_RUN_PARAMS_INSERT['TCP_REPORT_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['TCP_REPORT_DATE'], format='%Y-%m-%d %H:%M:%S')
#         FPT_RUN_PARAMS_INSERT['TRADE_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['TRADE_DATE'])
#         #FPT_RUN_PARAMS_INSERT['RUN_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['RUN_DATE'], format='%Y-%m-%d %H:%M:%S')
#         FTR_AUC_DTLS.reset_index(drop=True)
#         AuctionName = FTR_AUC_DTLS['AUCTION_NAME'][0]

        

#         print('Inserting FPT_RUN_PARAMS dataframe to PowerDB.PQUANT.FPT_RUN_PARAMS \n')
#         logging.info('Inserting FPT_RUN_PARAMS dataframe to PowerDB.PQUANT.FPT_RUN_PARAMS')
#         sql = '''select * from POWERDB.PQUANT.FPT_RUN_LOG T limit 1'''    
#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
#         cs=conn.cursor()
#         cs.execute(sql)
#         col_dtls=pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
#         conn.close()
#         cols = col_dtls.columns


#         ##### dropping duplicates before we are inserting rows into FPT_RUN_PARAMS table ...
#         FPT_RUN_PARAMS_INSERT=FPT_RUN_PARAMS_INSERT.groupby(['BU_ISO','AUCTION_NAME','BU_HEDGE_TYPE','BU_TRADE_TYPE','BU_TRADER','FLOW_STARTDATE','FLOW_ENDDATE','ISO_FTR_PARTICIPANT']).head(1)    
#         FPT_RUN_PARAMS_INSERT['FPT_ID'] = None    
#         FPT_RUN_PARAMS_INSERT['TRADER_PARAMETERS'] = None
#         FPT_RUN_PARAMS_INSERT['MODEL_PARAMETERS'] = None   
#         FPT_RUN_PARAMS_INSERT = FPT_RUN_PARAMS_INSERT[cols]    ######## get column order 

#         csv_file=r'C:\TEMP\FPT__DEV{}.csv'.format(AuctionName.replace(' ', '_'))
#         FPT_RUN_PARAMS_INSERT.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_MINIMAL)
        
#         conn = get_connection(role='OWNER_POWERDB_DEV',database='POWERDB',schema='PARCHIVE')
#         conn.cursor().execute('USE DATABASE POWERDB')
#         conn.cursor().execute('USE SCHEMA POWERDB.PARCHIVE')    
#         conn.cursor().execute("PUT file://{} @%FPT_RUN_LOG overwrite=true".format(csv_file))
#         conn.cursor().execute('''copy into FPT_RUN_LOG
#             (BU_ISO,AUCTION_NAME,AUCTION_ROUND,TRADE_DATE,BU_HEDGE_TYPE,BU_TRADE_TYPE,BU_TRADER,FLOW_STARTDATE,FLOW_ENDDATE,TCP_REPORT_DATE,RUN_DATE,TOOL_VERSION,RUN_PARAMETERS,FILENAME,ISO_FTR_PARTICIPANT,CALCULATE_DAILY_YN,RUN_STATUS,PORTFOLIO_NAME,TCP_ID,TRADER_PARAMETERS,MODEL_PARAMETERS,FPT_STRATEGY)
#             from (select $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23 from @%FPT_RUN_LOG) file_format=(type=csv skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')''')
#         conn.close()             
                
#         FPT_IDquerytxt = '''select FPT_ID,BU_ISO,AUCTION_NAME,TRADE_DATE,BU_TRADE_TYPE,BU_TRADER,BU_HEDGE_TYPE,FLOW_STARTDATE,FLOW_ENDDATE,dateadd(month,-2,FLOW_STARTDATE) PRIOR_FLOW_MONTH,
#                             dateadd(month,-2,FLOW_STARTDATE) PRIOR_ST_DT,dateadd(month,-2,FLOW_ENDDATE) PRIOR_ED_DT,TCP_REPORT_DATE,TOOL_VERSION,FILENAME,RUN_PARAMETERS,CALCULATE_DAILY_YN,ISO_FTR_PARTICIPANT from PowerDB.PARCHIVE.FPT_RUN_LOG
#                             where RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' and RUN_DATE = \'''' + str(RUN_DATETIME) + '\''            
#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')       
#         conn.cursor().execute('USE DATABASE POWERDB') 
#         cs=conn.cursor()
#         cs.execute(FPT_IDquerytxt)       
#         FPT_RUN_PARAMS_DTLS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])       
#         conn.close()   

#         FPT_RUN_PARAMS_DTLS[['FLOW_STARTDATE','FLOW_ENDDATE']] = FPT_RUN_PARAMS_DTLS[['FLOW_STARTDATE','FLOW_ENDDATE']].astype('datetime64')
#         FTR_INV_MONTHLY[['FLOW_STARTDATE','FLOW_ENDDATE']] = FTR_INV_MONTHLY[['FLOW_STARTDATE','FLOW_ENDDATE']].astype('datetime64')
        
#         FPT_PATH_DTLS = pd.merge(FTR_INV_MONTHLY,FPT_RUN_PARAMS_DTLS[['FPT_ID','BU_ISO','FLOW_STARTDATE','FLOW_ENDDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','BU_TRADER','ISO_FTR_PARTICIPANT']],on=['BU_ISO','FLOW_STARTDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','FLOW_ENDDATE','BU_TRADER','ISO_FTR_PARTICIPANT'],how='left')

#         FPT_PATH_DTLS['PA_BU_CLEARED_MW'] = FPT_PATH_DTLS['FTR_MW']
#         FPT_PATH_DTLS['TRADE_FLAG'] = FPT_PATH_DTLS['AUCTION_NAME']
#         FPT_PATH_DTLS['AUCTION_TAG'] = FPT_PATH_DTLS['AUCTION_NAME']
#         FPT_PATH_DTLS['PA_MCP_MWH'] = FPT_PATH_DTLS['LTP_PRICE_MWH']
#         FPT_PATH_DTLS = FPT_PATH_DTLS.rename(columns = {'FTR_MW':'BID_MW_TRADER','FTR_PRICE_MWH':'BID_PRICE_MWH_TRADER'})
#         FPT_PATH_DTLS['BID_PRICE_MWH_MODEL'] = FPT_PATH_DTLS['BID_PRICE_MWH_TRADER']
#         FPT_PATH_DTLS['BID_MW_MODEL'] = FPT_PATH_DTLS['BID_MW_TRADER']    
#         FPT_PATH_DTLS['FTR_SEGMENT'] = 1
#         FPT_PATH_DTLS = FPT_PATH_DTLS.drop('LTP_PRICE_MWH',axis=1)
    
#         sql = '''select * from POWERDB.PQUANT.FPT_PATH_DETAILS T limit 1'''    
#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
#         cs=conn.cursor()
#         cs.execute(sql)
#         fpd_col_dtls=pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
#         conn.close()
                
#         fpd_cols = fpd_col_dtls.columns
#         for x in fpd_cols:
#             if x not in list(FPT_PATH_DTLS.columns):
#                 FPT_PATH_DTLS[x]=None   
        
#         FPT_PATH_DTLS = FPT_PATH_DTLS[fpd_cols]
#         FPT_PATH_DTLS['FPT_SEGMENT_ID'] = None
#         csv_file=r'C:\TEMP\FTR_INVENTORY_TRANSACTION_DTLS_DEV{}.csv'.format(FTR_AUC_DTLS['AUCTION_NAME'][0].replace(' ','_'))
        
#         FPT_PATH_DTLS.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_MINIMAL)

#         conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PARCHIVE')
#         conn.cursor().execute('USE DATABASE POWERDB')
#         conn.cursor().execute('USE SCHEMA POWERDB.PARCHIVE')  
#         conn.cursor().execute('remove @%FPT_PATH_DETAILS')      
#         conn.cursor().execute("PUT file://{} @%FPT_PATH_DETAILS overwrite=true".format(csv_file))
#         conn.cursor().execute('''copy into FPT_PATH_DETAILS
#                  (FPT_ID,SOURCE_PNODE,SINK_PNODE,BU_PEAK_TYPE,BID_MW_TRADER,BID_PRICE_MWH_TRADER,TRADE_FLAG,SELL_FTRID,FTR_SEGMENT,HRS_IN_PERIOD,LTP_MWH,PA_MCP_MWH,PA_DA_REVENUE_MWH,PA_RT_REVENUE_MWH,BID_CREDIT_MWH,HOLDING_CREDIT_MWH,DA_MCC_RECENT_MWH,DA_RECENT_PAYOFF,NET_POS2_CNX_SF_EXP,NET_NEG2_CNX_SF_EXP,CNT_BULL_POS2_CNX_SF_EXP,CNT_BEAR_POS2_CNX_SF_EXP,PA_DA_PNL_AVG,PA_BU_CLEARED_MW,NET_POS1_CNX_SF_EXP,NTP_MWH,NTP_CLEARED_MW,SOURCE_CLUSTER_NAME,SINK_CLUSTER_NAME,PEER_BUY_VWAP,PEER_NET_MW,PRCNT_PEERS_OWN,PEER_PATH_WT_PRCNT,PEER_PARTICIPANT_TAG,BU_NET_MW,SOURCE_CLUSTER_NET_MW_EXP_PEER,SOURCE_CLUSTER_NET_MW_EXP_BU,SINK_CLUSTER_NET_MW_EXP_PEER,SINK_CLUSTER_NET_MW_EXP_BU,NET_NEG1_CNX_SF_EXP,GROSS_POS2_CNX_SF_EXP,GROSS_NEG2_CNX_SF_EXP,GROSS_POS1_CNX_SF_EXP,GROSS_NEG1_CNX_SF_EXP,CNT_BULL_POS1_CNX_SF_EXP,CNT_BEAR_POS1_CNX_SF_EXP,CNT_BULL_NEG2_CNX_SF_EXP,CNT_BEAR_NEG2_CNX_SF_EXP,CNT_BULL_NEG1_CNX_SF_EXP,CNT_BEAR_NEG1_CNX_SF_EXP,DA_MCC_M1,DA_MCC_M2,DA_MCC_M3,DA_MCC_M4,DA_MCC_M5,DA_MCC_M6,DA_MCC_M7,DA_MCC_M8,DA_MCC_M9,DA_MCC_M10,DA_MCC_M11,DA_MCC_M12,DA_MCC_M13,DA_MCC_M14,DA_MCC_M15,DA_MCC_M16,DA_MCC_M17,DA_MCC_M18,DA_MCC_M19,DA_MCC_M20,DA_MCC_M21,DA_MCC_M22,DA_MCC_M23,DA_MCC_M24,LATEST_AUCTION_PRICE,LATEST_AUCTION_NAME,BULLISH_POS_RNK1_CNX_TAG,BULLISH_POS_RNK1_SF_TAG,BEARISH_POS_RNK1_CNX_TAG,BEARISH_POS_RNK1_SF_TAG,BULLISH_POS_RNK2_CNX_TAG,BULLISH_POS_RNK2_SF_TAG,BEARISH_POS_RNK2_CNX_TAG,BEARISH_POS_RNK2_SF_TAG,BULLISH_NEG_RNK1_CNX_TAG,BULLISH_NEG_RNK1_SF_TAG,BEARISH_NEG_RNK1_CNX_TAG,BEARISH_NEG_RNK1_SF_TAG,BULLISH_NEG_RNK2_CNX_TAG,BULLISH_NEG_RNK2_SF_TAG,BEARISH_NEG_RNK2_CNX_TAG,BEARISH_NEG_RNK2_SF_TAG,LATEST_AUCTION_NET_MW_PEER,BU_BUY_VWAP,PEER_AUCTION_TAG,SIP_TAG,FILTER_TAG,NET_POS2_CNX_VALUE,NET_POS1_CNX_VALUE,NET_NEG1_CNX_VALUE,NET_NEG2_CNX_VALUE,DIVERSITY_SCORE,NET_POS2_CNX_VALUE_NORMALIZED,NET_NEG2_CNX_VALUE_NORMALIZED,NET_POS1_CNX_VALUE_NORMALIZED,NET_NEG1_CNX_VALUE_NORMALIZED,TCP_PATH_SCORE,TCP_PATH_RANK,PRCNT_PEERS_OWN_NORMALIZED,PEER_PATH_WT_PRCNT_NORMALIZED,PEER_POPULARITY_SCORE,PEER_POPULARITY_RANK,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_LTP_NORMALIZED,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_LTP_RANK,TOTAL_PATH_SCORE,TOTAL_PATH_RANK,HIST_2YR_DA_MCC_MONTHLY_MEDIAN,HIST_1YR_DA_MCC_MONTHLY_MEDIAN,HIST_SEAS_DA_MCC_MONTHLY_AVG,PEER_PATH_WT_PRCNT_MEDIAN,PEER_PATH_WT_PRCNT_MIN,PEER_PATH_WT_PRCNT_MAX,BU_PATH_WT_PRCNT,PEER_SOLD_MW,PEER_SELL_VWAP,BU_SELL_VWAP,TRADER_COMMENTS,PATH_DETAILS,BID_PRICE_MWH_MODEL,BID_MW_MODEL,PEER_NET_MW_TAG,LATEST_AUCTION_NET_MW_BU,RT_MCC_M1,RT_MCC_M2,RT_MCC_M3,RT_MCC_M4,RT_MCC_M5,RT_MCC_M6,RT_MCC_M7,RT_MCC_M8,RT_MCC_M9,RT_MCC_M10,RT_MCC_M11,RT_MCC_M12,RT_MCC_M13,RT_MCC_M14,RT_MCC_M15,RT_MCC_M16,RT_MCC_M17,RT_MCC_M18,RT_MCC_M19,RT_MCC_M20,RT_MCC_M21,RT_MCC_M22,RT_MCC_M23,RT_MCC_M24,TCP_SOURCE_SCORE,TCP_SINK_SCORE,PA_PEER_CLEARED_MW,PA_PEER_PARTICIPANT_CLEARED_TAG,PA_PEER_CLEARED_MW_TAG,PA_DA_REVENUE_MWH_MONTHLY_MIN,PA_RT_REVENUE_MWH_MONTHLY_MIN,PA_DA_REVENUE_MWH_MONTHLY_MAX,PA_RT_REVENUE_MWH_MONTHLY_MAX,HIST_2YR_DA_MCC_MONTHLY_MIN,HIST_2YR_DA_MCC_MONTHLY_MAX,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_PEER_BUY_VWAP_NORMALIZED,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_PEER_BUY_VWAP_RANK,GROSS_POS2_CNX_VALUE,GROSS_POS1_CNX_VALUE,GROSS_POS2_CNX_VALUE_NORMALIZED,GROSS_POS1_CNX_VALUE_NORMALIZED,HIST_1YR_DA_MCC_MONTHLY_MEDIAN_LTP_NORMALIZED,HIST_1YR_DA_MCC_MONTHLY_MEDIAN_LTP_RANK,STRATEGY_METRIC,DZ_SOURCE_MCC_MWH,DZ_SINK_MCC_MWH,PEER_BOUGHT_MW,CB_DIST_ST,LTP_MWH_M1,LTP_MWH_M2,LTP_MWH_M3,LTP_MWH_M4,LTP_MWH_M5,LTP_MWH_M6,LTP_MWH_M7,LTP_MWH_M8,LTP_MWH_M9,LTP_MWH_M10,LTP_MWH_M11,LTP_MWH_M12,LTP_MWH_M13,LTP_MWH_M14,LTP_MWH_M15,LTP_MWH_M16,LTP_MWH_M17,LTP_MWH_M18,LTP_MWH_M19,LTP_MWH_M20,LTP_MWH_M21,LTP_MWH_M22,LTP_MWH_M23,LTP_MWH_M24)
#                 from (select $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98,$99,$100,$101,$102,$103,$104,$105,$106,$107,$108,$109,$110,$111,$112,$113,$114,$115,$116,$117,$118,$119,$120,$121,$122,$123,$124,$125,$126,$127,$128,$129,$130,$131,$132,$133,$134,$135,$136,$137,$138,$139,$140,$141,$142,$143,$144,$145,$146,$147,$148,$149,$150,$151,$152,$153,$154,$155,$156,$157,$158,$159,$160,$161,$162,$163,$164,$165,$166,$167,$168,$169,$170,$171,$172,$173,$174,$175,$176,$177,$178,$179,$180,$181,$182,$183,$184,$185,$186,$187,$188,$189,$190,$191,$192,$193,$194,$195,$196,$197,$198,$199,$200,$201,$202,$203,$204,$205  from @%FPT_PATH_DETAILS) file_format=(type=csv skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')''')

#         conn.close()                 
#         if os.path.exists(dir2):
#             os.remove(csv_file)
    
#     logging.info('\n')
#     logging.info('Finished pushing ..... FTR INVENTORY to FPT_PATH_DETAILS .....  :::'+'\n')

#     print('processing done. sending email...')
#     logging.info('Execution Done')
#     json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
#     bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB_DEV',status='COMPLETED',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=len(FPT_PATH_DTLS), log=json, warehouse=warehouse,process_owner='Rahul')
#     bu_alerts.send_mail(
#         receiver_email = receiver_email,
#         mail_subject='JOB SUCCESS - TEST KIPI_DEV FPT_MONTHLY_TRANSACTION_INV_PUSH',
#         mail_body='FPT_MONTHLY_TRANSACTION_INV_PUSH completed successfully, Attached logs',
#         attachment_location = log_file_location
#     )
# except Exception as e:
#     print(f"Exception caught {e} during execution")
#     logging.exception(f'Exception caught during execution: {e}')
#     json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
#     bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB_DEV',status='FAILED',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=0,log=json,warehouse=warehouse,process_owner='Rahul')
#     bu_alerts.send_mail(
#         receiver_email = receiver_email,
#         mail_subject='JOB FAILED - TEST KIPI_DEV FPT_MONTHLY_TRANSACTION_INV_PUSH',
#         mail_body='FPT_MONTHLY_TRANSACTION_INV_PUSH failed during execution, Attached logs',
#         attachment_location = log_file_location
#     )


###########################################################################################################################################################################################
##############                                               BioUrja FTR Portfolio Construction Tool version 4.0                                                             ##############
###########################################################################################################################################################################################
import logging
import sys
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
from datetime import timedelta
import time
import pyodbc
import math
import csv
from tkinter import *
import email
import smtplib
from dateutil.rrule import rrule, MONTHLY
import bu_alerts
from bu_snowflake import get_connection


job_id=np.random.randint(1000000,9999999)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'FTR_INVENTORY_TOOL_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

# receiver_email = 'radha.waswani@biourja.com,shweta.srivastava@biourja.com,rahul.chaturvedi@biourja.com'
receiver_email = 'indiapowerit@biourja.com,anil.upadrasta@biourja.com, DAPower@biourja.com'
# receiver_email = 'indiapowerit@biourja.com,DAPower@biourja.com'
#receiver_email = 'rohit.pawar@biourja.com'

# json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
warehouse = 'QUANT_WH'
try:
    logging.info('Execution Started')
    json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
    bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB',status='Started',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=0, log=json, warehouse=warehouse,process_owner='RAHUL')
    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')     
    def button4():
        window_no_output.destroy()

    def button3():
        window_exit.destroy()

    # folder_path = 'G:\\Gas & Pwr\\Pwr\\FTR\\Python Scripts\\Anil\\PQUANT\\FTRPortfolioTool_v5.0'
    iso_list = ['MISO','PJMISO','ERCOT','SPPISO','NYISO','CAISO']
    # iso_list = ['NYISO','NYISO']
    Current_Obj = datetime.today().date() + timedelta(days=15)
    
    FLOW_MONTH = Current_Obj.strftime('%Y-%m')+'-01'
    logging.info(f"===={FLOW_MONTH}======")
    start_time = time.time()
    
    # os.chdir(folder_path)  
    RUN_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    dir2 = 'C://TEMP'
    if not os.path.exists(dir2):
        os.makedirs(dir2)
    
    FTR_INV_QUERYTxt = '''SELECT AUCTION_NAME,ISO_SELL_FTR_ID,BU_ISO,FTR_PARTICIPANT,BU_TRADER,SOURCE_ISO_PNODE SOURCE_PNODE,LATEST_YES_SOURCE_PNODE SOURCE_ID,SINK_ISO_PNODE SINK_PNODE,LATEST_YES_SINK_PNODE SINK_ID,
                        PEAK_TYPE BU_PEAK_TYPE,TRADE_TYPE BU_TRADE_TYPE,HEDGE_TYPE BU_HEDGE_TYPE,FLOW_MONTH,FLOW_MONTH FLOW_STARTDATE,last_day(FLOW_MONTH) FLOW_ENDDATE,HRCNT_FLOWMONTH,MW,MCP_PRICE_MWH,LTP_PRICE_MWH
                        FROM PowerDB.PQUANT.FIM_PLUS_VW WHERE FLOW_MONTH = '{}' AND BU_ISO in {} AND (FTR_PARTICIPANT LIKE '%BIO%')'''.format(FLOW_MONTH,tuple(iso_list))
    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')  
    cs=conn.cursor()
    cs.execute(FTR_INV_QUERYTxt)       
    FTR_INV_MONTHLY = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
    conn.close()                        
    FTR_INV_MONTHLY = FTR_INV_MONTHLY.rename(columns = {'FTR_PARTICIPANT':'ISO_FTR_PARTICIPANT','ISO_SELL_FTR_ID':'SELL_FTRID','MW':'FTR_MW','MCP_PRICE_MWH':'FTR_PRICE_MWH','HRCNT_FLOWMONTH':'HRS_IN_PERIOD'})
    #  replacing this peace of code with anil provided code i.e  ticket 2393 
    # FTR_AUC_DTLSTxt = '''SELECT *,row_number() over (partition by BU_ISO,AUCTION_DATE order by AUCTION_RESULTDATE desc) RNK  FROM PowerDB.PQUANT.BU_FTR_AUCTION_VW
    #                     WHERE AUCTION_DATE = '{}' AND BU_ISO in {}
    #                     and BU_AUCTION_TYPE = 'MONTHLY' order by AUCTION_NAME asc'''.format(FLOW_MONTH,tuple(iso_list))
    FTR_AUC_DTLSTxt = '''SELECT *,row_number() over (partition by BU_ISO,flow_startdate order by AUCTION_RESULTDATE desc) RNK 
                            FROM POWERDB.PQUANT.BU_FTR_AUCTION_VW
                            WHERE flow_startdate = '{}' AND BU_ISO in {}
                            order by AUCTION_NAME asc'''.format(FLOW_MONTH,tuple(iso_list))
    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
    cs=conn.cursor()
    cs.execute(FTR_AUC_DTLSTxt)       
    FTR_AUC_DTLS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])  
    conn.close()                        
    FTR_AUC_DTLS = FTR_AUC_DTLS[FTR_AUC_DTLS.RNK == 1]

    ######## If exists in database delete before insertion ...... 
    FPT_ID_EXISTSTxt = '''select distinct(FPT_ID) from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' order by FPT_ID asc'''.format(FLOW_MONTH,tuple(iso_list))
    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
    cs=conn.cursor()
    cs.execute(FPT_ID_EXISTSTxt)       
    FPT_ID_EXISTS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
    conn.close()            
        
    FPT_PATH_EXISTSTxt = '''select * from PowerDB.PQUANT.FPT_PATH_DETAILS where FPT_ID in 
                            (select distinct(FPT_ID) from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY')'''.format(FLOW_MONTH,tuple(iso_list))
    conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
    cs=conn.cursor()
    cs.execute(FPT_PATH_EXISTSTxt)       
    FPT_PATH_EXISTS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
    conn.close()   

    if (FPT_ID_EXISTS.shape[0] > 0):    
        DEL_FPT_PATH_EXISTSTxt = '''delete from PowerDB.PQUANT.FPT_PATH_DETAILS where FPT_ID in 
                            (select distinct(FPT_ID) from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and BU_ISO in {} and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY')'''.format(FLOW_MONTH,tuple(iso_list))
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')   
        cs=conn.cursor()
        cs.execute(DEL_FPT_PATH_EXISTSTxt)       
        conn.close()  
        
        DEL_FPT_ID_EXISTSTxt = '''delete from PowerDB.PQUANT.FPT_RUN_LOG where FLOW_STARTDATE = '{}' and RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' and BU_ISO in {}'''.format(FLOW_MONTH,tuple(iso_list))
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')    
        cs=conn.cursor()
        cs.execute(DEL_FPT_ID_EXISTSTxt)         
        conn.close() 
        logging.info(f"Previous data deleted for ===={FLOW_MONTH}======")

    if (FTR_INV_MONTHLY.shape[0] > 0):
        FPT_RUN_DTLS = FTR_INV_MONTHLY[['BU_ISO','FLOW_STARTDATE','FLOW_ENDDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','ISO_FTR_PARTICIPANT','BU_TRADER']].drop_duplicates()
       
       # commented these lines of code as suggested by anil  and replacing with new lines provided by anil  i.e  ticket 2393
        # FPT_RUN_PARAMS = pd.merge(FPT_RUN_DTLS,FTR_AUC_DTLS[['BU_ISO','AUCTION_NAME','AUCTION_ROUND','AUCTION_RESULTDATE','AUCTION_DATE']],left_on=['FLOW_STARTDATE','BU_ISO'],right_on=['AUCTION_DATE','BU_ISO'],how='left')
        # FPT_RUN_PARAMS['ALIAS'] = np.where(FPT_RUN_PARAMS.BU_TRADER == 'AO','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'ET','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'RW','VR',np.where(FPT_RUN_PARAMS.BU_TRADER == 'PP','WB',np.where(FPT_RUN_PARAMS.BU_TRADER == 'KP & PP','WB',FPT_RUN_PARAMS.BU_TRADER)))))
        # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'VR'
        # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'VR'      
        # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'NEMO'),'ALIAS'] = 'VR'
        # FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'FK'       
        # FPT_RUN_PARAMS = FPT_RUN_PARAMS.drop(['AUCTION_DATE'],axis=1)


        FPT_RUN_PARAMS = pd.merge(FPT_RUN_DTLS,FTR_AUC_DTLS[['BU_ISO','AUCTION_NAME','AUCTION_ROUND','AUCTION_RESULTDATE','FLOW_STARTDATE']],on=['FLOW_STARTDATE','BU_ISO'],how='left')
        FPT_RUN_PARAMS['ALIAS'] = np.where(FPT_RUN_PARAMS.BU_TRADER == 'AO','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'ET','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'RW','NEMO',np.where(FPT_RUN_PARAMS.BU_TRADER == 'PP','WB',np.where(FPT_RUN_PARAMS.BU_TRADER == 'KP & PP','WB',FPT_RUN_PARAMS.BU_TRADER)))))
        FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'NEMO'
        FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'NEMO'     
        FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'PJMISO') & (FPT_RUN_PARAMS.BU_TRADER == 'VR'),'ALIAS'] = 'NEMO'
        FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'JS'),'ALIAS'] = 'FK' 
        FPT_RUN_PARAMS.loc[(FPT_RUN_PARAMS.BU_ISO == 'MISO') & (FPT_RUN_PARAMS.BU_TRADER == 'AU'),'ALIAS'] = 'FK'  


        
        TCP_RUN_DATES = list()
        TCP_ID_LISTS = list()
        for ii in range(FPT_RUN_PARAMS.shape[0]):
            #[TCP_ID,REPORT_DATE,TCP_IDs,tcpid_selection] = TKinterTCPIDSelection.main(errorlog,FPT_RUN_PARAMS['BU_ISO'][ii],FPT_RUN_PARAMS['ALIAS'][ii],FPT_RUN_PARAMS['FLOW_STARTDATE'][ii].strftime('%Y-%m-%d'),FPT_RUN_PARAMS['FLOW_ENDDATE'][ii].strftime('%Y-%m-%d'))        
            BU_ISO = FPT_RUN_PARAMS['BU_ISO'][ii]
            FLOW_STDT = FPT_RUN_PARAMS['FLOW_STARTDATE'][ii].strftime('%Y-%m-%d')
            FLOW_EDDT = FPT_RUN_PARAMS['FLOW_ENDDATE'][ii].strftime('%Y-%m-%d')
            trader_name = FPT_RUN_PARAMS['ALIAS'][ii]
            
            if BU_ISO == 'MISO':

                TCP_IDQuery = '''select distinct TCP_ID,REPORT_DATE, RUN_REL_DAY from PowerDB.PMODEL.TRADER_CONSTRAINT_PLAYBOOK 
                        where BU_ISO = \'''' + BU_ISO + '\'' + ''' and FLOW_STARTDATE = \'''' + FLOW_STDT + '\'' + ''' and FLOW_ENDDATE = \'''' + FLOW_EDDT + '\'' + ''' 
                        and TRADER_ANALYST = 'FK' and BU_PEAK_TYPE NOT in ('24HR', 'HR17','HR7') order by REPORT_DATE desc'''   
            else:    
                TCP_IDQuery = '''select distinct TCP_ID,REPORT_DATE, RUN_REL_DAY from PowerDB.PMODEL.TRADER_CONSTRAINT_PLAYBOOK 
                        where BU_ISO = \'''' + BU_ISO + '\'' + ''' and FLOW_STARTDATE = \'''' + FLOW_STDT + '\'' + ''' and FLOW_ENDDATE = \'''' + FLOW_EDDT + '\'' + ''' 
                        and TRADER_ANALYST = \'''' + trader_name + '\'' + ''' and BU_PEAK_TYPE NOT in ('24HR', 'HR17','HR7') order by REPORT_DATE desc'''             

            conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')        
            cs=conn.cursor()
            cs.execute(TCP_IDQuery)       
            TCP_IDs = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
            conn.close()  

            # TCP_ID = TCP_IDs.loc[0].TCP_ID
            # REPORT_DATE = TCP_IDs.loc[0].REPORT_DATE
            
            if (TCP_IDs.shape[0] == 0):
                TCP_RUN_DATES.append('1900-01-01 00:00:00.000')
                TCP_ID_LISTS.append('NULL')
            else: 
                TCP_ID = TCP_IDs.loc[0].TCP_ID
                REPORT_DATE = TCP_IDs.loc[0].REPORT_DATE   
                TCP_RUN_DATES.append(str(REPORT_DATE))
                TCP_ID_LISTS.append(str(TCP_ID))
        
        if len(TCP_RUN_DATES) == 1:
            FPT_RUN_PARAMS['TCP_REPORT_DATE'] = TCP_RUN_DATES[0]
            FPT_RUN_PARAMS['TCP_ID'] = TCP_ID_LISTS[0]       
        else :
            FPT_RUN_PARAMS['TCP_REPORT_DATE'] = TCP_RUN_DATES
            FPT_RUN_PARAMS['TCP_ID'] = TCP_ID_LISTS
    

        FPT_RUN_PARAMS['RUN_DATE'] = RUN_DATETIME
        FPT_RUN_PARAMS['TOOL_VERSION'] = 'FTR Inventory'
        FPT_RUN_PARAMS['FPT_STRATEGY'] = 'DA_PnL_Tracker'

        FPT_RUN_PARAMS['RUN_PARAMETERS'] = 'BU_ISO:'+FPT_RUN_PARAMS['BU_ISO']+'~'+'AuctionName:'+FPT_RUN_PARAMS['AUCTION_NAME']+'~'+'FLOW_ST:'+ pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%Y-%m-%d')+'~'+'FLOW_END:'+ pd.to_datetime(FPT_RUN_PARAMS['FLOW_ENDDATE']).dt.strftime('%Y-%m-%d')
        FPT_RUN_PARAMS['FILENAME'] = FPT_RUN_PARAMS['BU_ISO']+'_'+str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%b')[0])+'_'+FPT_RUN_PARAMS['BU_TRADER']+'_'+FPT_RUN_PARAMS['BU_TRADE_TYPE']
        FPT_RUN_PARAMS['CALCULATE_DAILY_YN'] = 'Y'
        FPT_RUN_PARAMS['RUN_STATUS'] = 'TRANSACTION_INVENTORY_MONTHLY'
        FPT_RUN_PARAMS['PORTFOLIO_NAME'] = 'FTR - For PNL ' + str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%B')[0]) + ' ' + str(pd.to_datetime(FPT_RUN_PARAMS['FLOW_STARTDATE']).dt.strftime('%Y')[0]) + ' ' + FPT_RUN_PARAMS['BU_ISO'] + ' ' + FPT_RUN_PARAMS['ISO_FTR_PARTICIPANT'] + ' ' + FPT_RUN_PARAMS['BU_TRADER']
        FPT_RUN_PARAMS = FPT_RUN_PARAMS.rename(columns = {'FTR_PARTICIPANT':'ISO_FTR_PARTICIPANT','AUCTION_RESULTDATE':'TRADE_DATE'})  
        FPT_RUN_PARAMS_INSERT = FPT_RUN_PARAMS.copy()
        FPT_RUN_PARAMS_INSERT['FLOW_STARTDATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['FLOW_STARTDATE'])
        FPT_RUN_PARAMS_INSERT['FLOW_ENDDATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['FLOW_ENDDATE'])
        FPT_RUN_PARAMS_INSERT['TCP_REPORT_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['TCP_REPORT_DATE'], format='%Y-%m-%d %H:%M:%S')
        FPT_RUN_PARAMS_INSERT['TRADE_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['TRADE_DATE'])
        #FPT_RUN_PARAMS_INSERT['RUN_DATE'] =  pd.to_datetime(FPT_RUN_PARAMS_INSERT['RUN_DATE'], format='%Y-%m-%d %H:%M:%S')
        AuctionName = FTR_AUC_DTLS['AUCTION_NAME'][1]
    
        print('Inserting FPT_RUN_PARAMS dataframe to PowerDB.PQUANT.FPT_RUN_PARAMS \n')
        logging.info('Inserting FPT_RUN_PARAMS dataframe to PowerDB.PQUANT.FPT_RUN_PARAMS')
        sql = '''select * from POWERDB.PQUANT.FPT_RUN_LOG T limit 1'''    
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
        cs=conn.cursor()
        cs.execute(sql)
        col_dtls=pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        cols = col_dtls.columns

        ##### dropping duplicates before we are inserting rows into FPT_RUN_PARAMS table ...
        FPT_RUN_PARAMS_INSERT=FPT_RUN_PARAMS_INSERT.groupby(['BU_ISO','AUCTION_NAME','BU_HEDGE_TYPE','BU_TRADE_TYPE','BU_TRADER','FLOW_STARTDATE','FLOW_ENDDATE','ISO_FTR_PARTICIPANT']).head(1)    
        FPT_RUN_PARAMS_INSERT['FPT_ID'] = None    
        FPT_RUN_PARAMS_INSERT['TRADER_PARAMETERS'] = None
        FPT_RUN_PARAMS_INSERT['MODEL_PARAMETERS'] = None   
        FPT_RUN_PARAMS_INSERT = FPT_RUN_PARAMS_INSERT[cols]    ######## get column order 

        csv_file=r'C:\TEMP\FPT{}.csv'.format(AuctionName.replace(' ', '_'))
        FPT_RUN_PARAMS_INSERT.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_MINIMAL)
        
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
        conn.cursor().execute('USE DATABASE POWERDB')
        conn.cursor().execute('USE SCHEMA POWERDB.PQUANT')    
        conn.cursor().execute("PUT file://{} @%FPT_RUN_LOG overwrite=true".format(csv_file))
        conn.cursor().execute('''copy into FPT_RUN_LOG
            (BU_ISO,AUCTION_NAME,AUCTION_ROUND,TRADE_DATE,BU_HEDGE_TYPE,BU_TRADE_TYPE,BU_TRADER,FLOW_STARTDATE,FLOW_ENDDATE,TCP_REPORT_DATE,RUN_DATE,TOOL_VERSION,RUN_PARAMETERS,FILENAME,ISO_FTR_PARTICIPANT,CALCULATE_DAILY_YN,RUN_STATUS,PORTFOLIO_NAME,TCP_ID,TRADER_PARAMETERS,MODEL_PARAMETERS,FPT_STRATEGY)
            from (select $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23 from @%FPT_RUN_LOG) file_format=(type=csv skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')''')
        conn.close()             
                
        FPT_IDquerytxt = '''select FPT_ID,BU_ISO,AUCTION_NAME,TRADE_DATE,BU_TRADE_TYPE,BU_TRADER,BU_HEDGE_TYPE,FLOW_STARTDATE,FLOW_ENDDATE,dateadd(month,-2,FLOW_STARTDATE) PRIOR_FLOW_MONTH,
                            dateadd(month,-2,FLOW_STARTDATE) PRIOR_ST_DT,dateadd(month,-2,FLOW_ENDDATE) PRIOR_ED_DT,TCP_REPORT_DATE,TOOL_VERSION,FILENAME,RUN_PARAMETERS,CALCULATE_DAILY_YN,ISO_FTR_PARTICIPANT from PowerDB.PQUANT.FPT_RUN_LOG
                            where RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' and RUN_DATE = \'''' + str(RUN_DATETIME) + '\''            
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')       
        conn.cursor().execute('USE DATABASE POWERDB') 
        cs=conn.cursor()
        cs.execute(FPT_IDquerytxt)       
        FPT_RUN_PARAMS_DTLS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])       
        conn.close()   

        FPT_RUN_PARAMS_DTLS[['FLOW_STARTDATE','FLOW_ENDDATE']] = FPT_RUN_PARAMS_DTLS[['FLOW_STARTDATE','FLOW_ENDDATE']].astype('datetime64')
        FTR_INV_MONTHLY[['FLOW_STARTDATE','FLOW_ENDDATE']] = FTR_INV_MONTHLY[['FLOW_STARTDATE','FLOW_ENDDATE']].astype('datetime64')
        
        FPT_PATH_DTLS = pd.merge(FTR_INV_MONTHLY,FPT_RUN_PARAMS_DTLS[['FPT_ID','BU_ISO','FLOW_STARTDATE','FLOW_ENDDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','BU_TRADER','ISO_FTR_PARTICIPANT']],on=['BU_ISO','FLOW_STARTDATE','BU_TRADE_TYPE','BU_HEDGE_TYPE','FLOW_ENDDATE','BU_TRADER','ISO_FTR_PARTICIPANT'],how='left')

        FPT_PATH_DTLS['PA_BU_CLEARED_MW'] = FPT_PATH_DTLS['FTR_MW']
        FPT_PATH_DTLS['TRADE_FLAG'] = FPT_PATH_DTLS['AUCTION_NAME']
        FPT_PATH_DTLS['AUCTION_TAG'] = FPT_PATH_DTLS['AUCTION_NAME']
        FPT_PATH_DTLS['PA_MCP_MWH'] = FPT_PATH_DTLS['LTP_PRICE_MWH']
        FPT_PATH_DTLS = FPT_PATH_DTLS.rename(columns = {'FTR_MW':'BID_MW_TRADER','FTR_PRICE_MWH':'BID_PRICE_MWH_TRADER'})
        FPT_PATH_DTLS['BID_PRICE_MWH_MODEL'] = FPT_PATH_DTLS['BID_PRICE_MWH_TRADER']
        FPT_PATH_DTLS['BID_MW_MODEL'] = FPT_PATH_DTLS['BID_MW_TRADER']    
        FPT_PATH_DTLS['FTR_SEGMENT'] = 1
        FPT_PATH_DTLS = FPT_PATH_DTLS.drop('LTP_PRICE_MWH',axis=1)
    
        sql = '''select * from POWERDB.PQUANT.FPT_PATH_DETAILS T limit 1'''    
        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
        cs=conn.cursor()
        cs.execute(sql)
        fpd_col_dtls=pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
                
        fpd_cols = fpd_col_dtls.columns
        for x in fpd_cols:
            if x not in list(FPT_PATH_DTLS.columns):
                FPT_PATH_DTLS[x]=None   
        
        FPT_PATH_DTLS = FPT_PATH_DTLS[fpd_cols]
        FPT_PATH_DTLS['FPT_SEGMENT_ID'] = None
        
        csv_file=r'C:\TEMP\FTR_INVENTORY_TRANSACTION_DTLS{}.csv'.format(FTR_AUC_DTLS['AUCTION_NAME'][1].replace(' ','_'))
        FPT_PATH_DTLS.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_MINIMAL)

        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
        conn.cursor().execute('USE DATABASE POWERDB')
        conn.cursor().execute('USE SCHEMA POWERDB.PQUANT')  
        conn.cursor().execute('remove @%FPT_PATH_DETAILS')      
        conn.cursor().execute("PUT file://{} @%FPT_PATH_DETAILS overwrite=true".format(csv_file))
        conn.cursor().execute('''copy into FPT_PATH_DETAILS
                 (FPT_ID,SOURCE_PNODE,SINK_PNODE,BU_PEAK_TYPE,BID_MW_TRADER,BID_PRICE_MWH_TRADER,TRADE_FLAG,SELL_FTRID,FTR_SEGMENT,HRS_IN_PERIOD,LTP_MWH,PA_MCP_MWH,PA_DA_REVENUE_MWH,PA_RT_REVENUE_MWH,BID_CREDIT_MWH,HOLDING_CREDIT_MWH,DA_MCC_RECENT_MWH,DA_RECENT_PAYOFF,NET_POS2_CNX_SF_EXP,NET_NEG2_CNX_SF_EXP,CNT_BULL_POS2_CNX_SF_EXP,CNT_BEAR_POS2_CNX_SF_EXP,PA_DA_PNL_AVG,PA_BU_CLEARED_MW,NET_POS1_CNX_SF_EXP,NTP_MWH,NTP_CLEARED_MW,SOURCE_CLUSTER_NAME,SINK_CLUSTER_NAME,PEER_BUY_VWAP,PEER_NET_MW,PRCNT_PEERS_OWN,PEER_PATH_WT_PRCNT,PEER_PARTICIPANT_TAG,BU_NET_MW,SOURCE_CLUSTER_NET_MW_EXP_PEER,SOURCE_CLUSTER_NET_MW_EXP_BU,SINK_CLUSTER_NET_MW_EXP_PEER,SINK_CLUSTER_NET_MW_EXP_BU,NET_NEG1_CNX_SF_EXP,GROSS_POS2_CNX_SF_EXP,GROSS_NEG2_CNX_SF_EXP,GROSS_POS1_CNX_SF_EXP,GROSS_NEG1_CNX_SF_EXP,CNT_BULL_POS1_CNX_SF_EXP,CNT_BEAR_POS1_CNX_SF_EXP,CNT_BULL_NEG2_CNX_SF_EXP,CNT_BEAR_NEG2_CNX_SF_EXP,CNT_BULL_NEG1_CNX_SF_EXP,CNT_BEAR_NEG1_CNX_SF_EXP,DA_MCC_M1,DA_MCC_M2,DA_MCC_M3,DA_MCC_M4,DA_MCC_M5,DA_MCC_M6,DA_MCC_M7,DA_MCC_M8,DA_MCC_M9,DA_MCC_M10,DA_MCC_M11,DA_MCC_M12,DA_MCC_M13,DA_MCC_M14,DA_MCC_M15,DA_MCC_M16,DA_MCC_M17,DA_MCC_M18,DA_MCC_M19,DA_MCC_M20,DA_MCC_M21,DA_MCC_M22,DA_MCC_M23,DA_MCC_M24,LATEST_AUCTION_PRICE,LATEST_AUCTION_NAME,BULLISH_POS_RNK1_CNX_TAG,BULLISH_POS_RNK1_SF_TAG,BEARISH_POS_RNK1_CNX_TAG,BEARISH_POS_RNK1_SF_TAG,BULLISH_POS_RNK2_CNX_TAG,BULLISH_POS_RNK2_SF_TAG,BEARISH_POS_RNK2_CNX_TAG,BEARISH_POS_RNK2_SF_TAG,BULLISH_NEG_RNK1_CNX_TAG,BULLISH_NEG_RNK1_SF_TAG,BEARISH_NEG_RNK1_CNX_TAG,BEARISH_NEG_RNK1_SF_TAG,BULLISH_NEG_RNK2_CNX_TAG,BULLISH_NEG_RNK2_SF_TAG,BEARISH_NEG_RNK2_CNX_TAG,BEARISH_NEG_RNK2_SF_TAG,LATEST_AUCTION_NET_MW_PEER,BU_BUY_VWAP,PEER_AUCTION_TAG,SIP_TAG,FILTER_TAG,NET_POS2_CNX_VALUE,NET_POS1_CNX_VALUE,NET_NEG1_CNX_VALUE,NET_NEG2_CNX_VALUE,DIVERSITY_SCORE,NET_POS2_CNX_VALUE_NORMALIZED,NET_NEG2_CNX_VALUE_NORMALIZED,NET_POS1_CNX_VALUE_NORMALIZED,NET_NEG1_CNX_VALUE_NORMALIZED,TCP_PATH_SCORE,TCP_PATH_RANK,PRCNT_PEERS_OWN_NORMALIZED,PEER_PATH_WT_PRCNT_NORMALIZED,PEER_POPULARITY_SCORE,PEER_POPULARITY_RANK,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_LTP_NORMALIZED,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_LTP_RANK,TOTAL_PATH_SCORE,TOTAL_PATH_RANK,HIST_2YR_DA_MCC_MONTHLY_MEDIAN,HIST_1YR_DA_MCC_MONTHLY_MEDIAN,HIST_SEAS_DA_MCC_MONTHLY_AVG,PEER_PATH_WT_PRCNT_MEDIAN,PEER_PATH_WT_PRCNT_MIN,PEER_PATH_WT_PRCNT_MAX,BU_PATH_WT_PRCNT,PEER_SOLD_MW,PEER_SELL_VWAP,BU_SELL_VWAP,TRADER_COMMENTS,PATH_DETAILS,BID_PRICE_MWH_MODEL,BID_MW_MODEL,PEER_NET_MW_TAG,LATEST_AUCTION_NET_MW_BU,RT_MCC_M1,RT_MCC_M2,RT_MCC_M3,RT_MCC_M4,RT_MCC_M5,RT_MCC_M6,RT_MCC_M7,RT_MCC_M8,RT_MCC_M9,RT_MCC_M10,RT_MCC_M11,RT_MCC_M12,RT_MCC_M13,RT_MCC_M14,RT_MCC_M15,RT_MCC_M16,RT_MCC_M17,RT_MCC_M18,RT_MCC_M19,RT_MCC_M20,RT_MCC_M21,RT_MCC_M22,RT_MCC_M23,RT_MCC_M24,TCP_SOURCE_SCORE,TCP_SINK_SCORE,PA_PEER_CLEARED_MW,PA_PEER_PARTICIPANT_CLEARED_TAG,PA_PEER_CLEARED_MW_TAG,PA_DA_REVENUE_MWH_MONTHLY_MIN,PA_RT_REVENUE_MWH_MONTHLY_MIN,PA_DA_REVENUE_MWH_MONTHLY_MAX,PA_RT_REVENUE_MWH_MONTHLY_MAX,HIST_2YR_DA_MCC_MONTHLY_MIN,HIST_2YR_DA_MCC_MONTHLY_MAX,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_PEER_BUY_VWAP_NORMALIZED,HIST_2YR_DA_MCC_MONTHLY_MEDIAN_PEER_BUY_VWAP_RANK,GROSS_POS2_CNX_VALUE,GROSS_POS1_CNX_VALUE,GROSS_POS2_CNX_VALUE_NORMALIZED,GROSS_POS1_CNX_VALUE_NORMALIZED,HIST_1YR_DA_MCC_MONTHLY_MEDIAN_LTP_NORMALIZED,HIST_1YR_DA_MCC_MONTHLY_MEDIAN_LTP_RANK,STRATEGY_METRIC,DZ_SOURCE_MCC_MWH,DZ_SINK_MCC_MWH,PEER_BOUGHT_MW,CB_DIST_ST,LTP_MWH_M1,LTP_MWH_M2,LTP_MWH_M3,LTP_MWH_M4,LTP_MWH_M5,LTP_MWH_M6,LTP_MWH_M7,LTP_MWH_M8,LTP_MWH_M9,LTP_MWH_M10,LTP_MWH_M11,LTP_MWH_M12,LTP_MWH_M13,LTP_MWH_M14,LTP_MWH_M15,LTP_MWH_M16,LTP_MWH_M17,LTP_MWH_M18,LTP_MWH_M19,LTP_MWH_M20,LTP_MWH_M21,LTP_MWH_M22,LTP_MWH_M23,LTP_MWH_M24)
                from (select $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98,$99,$100,$101,$102,$103,$104,$105,$106,$107,$108,$109,$110,$111,$112,$113,$114,$115,$116,$117,$118,$119,$120,$121,$122,$123,$124,$125,$126,$127,$128,$129,$130,$131,$132,$133,$134,$135,$136,$137,$138,$139,$140,$141,$142,$143,$144,$145,$146,$147,$148,$149,$150,$151,$152,$153,$154,$155,$156,$157,$158,$159,$160,$161,$162,$163,$164,$165,$166,$167,$168,$169,$170,$171,$172,$173,$174,$175,$176,$177,$178,$179,$180,$181,$182,$183,$184,$185,$186,$187,$188,$189,$190,$191,$192,$193,$194,$195,$196,$197,$198,$199,$200,$201,$202,$203,$204,$205  from @%FPT_PATH_DETAILS) file_format=(type=csv skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')''')

        conn.close()                 
        if os.path.exists(dir2):
            os.remove(csv_file)
    
    logging.info('\n')
    logging.info('Finished pushing ..... FTR INVENTORY to FPT_PATH_DETAILS .....  :::'+'\n')

    print('processing done. sending email...')
    logging.info('Execution Done')
    json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
    bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB',status='COMPLETED',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=len(FPT_PATH_DTLS), log=json, warehouse=warehouse,process_owner='Rahul')
    bu_alerts.send_mail(
        receiver_email = receiver_email,
        mail_subject='JOB SUCCESS -FPT_MONTHLY_TRANSACTION_INV_PUSH',
        mail_body='FPT_MONTHLY_TRANSACTION_INV_PUSH completed successfully, Attached logs',
        attachment_location = log_file_location
    )
except Exception as e:
    print(f"Exception caught {e} during execution")
    logging.exception(f'Exception caught during execution: {e}')
    json='[{"JOB_ID": '+str(job_id)+', "DATE":"'+datetime.now().strftime('%Y-%m-%d')+'"}]'
    bu_alerts.bulog(process_name='FPT_MONTHLY_TRANSACTION_INV_PUSH',database='POWERDB',status='FAILED',table_name='POWERDB.PQUANT.FPT_PATH_DETAILS', row_count=0,log=json,warehouse=warehouse,process_owner='Rahul')
    bu_alerts.send_mail(
        receiver_email = receiver_email,
        mail_subject='JOB FAILED -FPT_MONTHLY_TRANSACTION_INV_PUSH',
        mail_body='FPT_MONTHLY_TRANSACTION_INV_PUSH failed during execution, Attached logs',
        attachment_location = log_file_location
    )