import logging
import pandas as pd
import numpy as np
import os
import datetime
from datetime import timedelta,datetime
import email.message
import smtplib
import math
import csv
import sys
import bu_alerts
from bu_snowflake import get_connection
susername = "RPAWAR"
spass = "Download@1"

job_id=np.random.randint(1000000,9999999)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# log file location
log_file_location=os.getcwd() + '\\' + 'logs' + '\\' + 'FPT_DAILY_DA_PNL_MODULE_LOG.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

#receiver_email = 'indiapowerit@biourja.com,Anil.Upadrasta@BioUrja.com,vihar.vaghasiya@biourja.com,DAPower@biourja.com'
#receiver_email_business = 'power@biourja.com,radha.waswani@biourja.com,rahul.chaturvedi@biourja.com,DAPower@biourja.com'

receiver_email = 'rohit.pawar@biourja.com'
receiver_email_business = 'rohit.pawar@biourja.com,vihar.vaghasiya@biourja.com'

def get_lmp(Current_Date, iso_list):
    try:
        sql='''SELECT s.DATETIME, t.ISO, cast(t.OBJECTID as bigint) as NODE, ifnull(dacong, dalmp) as DALMP, ifnull(rtcong, rtlmp) as RTLMP
            FROM YESDB.YESDATA.DART_PRICES s, YESDB.YESDATA.PRICE_NODES t
            where s.objectid=t.objectid and datetime > '{}' and t.iso in {} 
            '''.format(Current_Date, tuple(iso_list))

        conn = get_connection(role='OWNER_POWERDB',database='POWERDB',schema='PQUANT')
        cs=conn.cursor()
        cs.execute(sql)       
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
        conn.close()  
        return df
    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def get_inventory(month_start):
    try:
        sql='''select fpt_run.BU_ISO,fpt_run.AUCTION_NAME,fpt_run.AUCTION_ROUND,fpt_run.FLOW_STARTDATE FLOW_MONTH,fpt_run.BU_HEDGE_TYPE,fpt_run.BU_TRADE_TYPE,
            fpt_run.BU_TRADER,fpt_run.ISO_FTR_PARTICIPANT,fpt_run.PORTFOLIO_NAME,fpt_fpd.FPT_SEGMENT_ID,fpt_fpd.FPT_ID,fpt_fpd.SOURCE_PNODE,src_bup.YES_PNODEID SOURCE_ID,
            fpt_fpd.SINK_PNODE,snk_bup.YES_PNODEID SINK_ID,fpt_fpd.BU_PEAK_TYPE,fpt_fpd.BID_MW_TRADER FTR_MW, fpt_fpd.BID_PRICE_MWH_TRADER FTR_COST
            from POWERDB_DEV.PQUANT.FPT_PATH_DETAILS fpt_fpd
            left join POWERDB_DEV.PQUANT.FPT_RUN_LOG fpt_run
            On fpt_run.FPT_ID = fpt_fpd.FPT_ID
            left join PowerDB_DEV.PDIM.BU_PNODE src_bup
            on src_bup.BU_ISO = fpt_run.BU_ISO
            and src_bup.ISO_PNODE = fpt_fpd.SOURCE_PNODE
            left join PowerDB_DEV.PDIM.BU_PNODE snk_bup
            on snk_bup.BU_ISO = fpt_run.BU_ISO
            and snk_bup.ISO_PNODE = fpt_fpd.SINK_PNODE
            where fpt_run.RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY'
            and fpt_run.FLOW_STARTDATE = '{}'
            '''.format(month_start)
        
        conn = get_connection(username=susername, password=spass,role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
        cs=conn.cursor()
        cs.execute(sql)       
        df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
        conn.close()
        return df
    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

def get_market_times(iso_list):
    try:

        sql='''select * from YESDB.YESDATA.ISO_MARKET_TIMES
            where datetime > current_date()-60 and datetime < current_date()+60
            and iso in ({})'''.format(','.join(["'"+x+"'" for x in iso_list]))
            
        conn = get_connection(role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
        cs=conn.cursor()
        cs.execute(sql)       
        market_times = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
        conn.close() 
        market_times['24HR']=1
        return market_times
    except Exception as e:
        print(f"Exception caught {e} during execution")
        logging.exception(f'Exception caught during execution: {e}')
        raise e

if __name__ == "__main__":
    starttime=datetime.now()
    rows = 0
    logging.info('Execution Started')
    for i in range(1):
        try:
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name="FPT_DAILY_DA_PNL_MODULE",database='POWERDB_DEV',status='Started',table_name= 'POWERDB_DEV.PQUANT.FPT_DAILY', row_count= rows, log=log_json, warehouse='QUANT_WH',process_owner='Rahul')
            
            Current_Obj = datetime.today().date() + timedelta(days=1)
            #Current_Obj = datetime.today().date() - timedelta(days=(1))
            Current_Date = Current_Obj.strftime('%Y-%m-%d')
            print(f"Run started for the Date. {Current_Obj}")
            month_start=Current_Obj.strftime('%Y-%m')+'-01'
            print(f"Run started for the month. {month_start}")
            inventory=get_inventory(month_start)

            iso_list=list(inventory['BU_ISO'].unique())
            market_times=get_market_times(iso_list)
            iso_peak=inventory.groupby(['BU_ISO','BU_PEAK_TYPE']).head(1)[['BU_ISO','BU_PEAK_TYPE']]
            PRICE_DTLS = get_lmp(Current_Date,iso_list)
            PRICE_DTLS['DATETIME'] = PRICE_DTLS['DATETIME'].astype('datetime64[ns]')
            market_times['DATETIME'] = market_times['DATETIME'].astype('datetime64[ns]')

            MKT_HRCNT = pd.melt(market_times,id_vars=['ISO','DATETIME','MARKETDAY'],value_vars=['OFFPEAK','ONPEAK','WEPEAK','WDPEAK','24HR'])
            MKT_HRCNT.columns = ['BU_ISO','DATETIME','MARKETDAY','BU_PEAK_TYPE','HRCNT']

            LMP_PRICES = pd.merge(PRICE_DTLS,market_times[['ISO','DATETIME','MARKETDAY','ONPEAK','OFFPEAK','WEPEAK','WDPEAK','24HR']],on=['ISO','DATETIME'],how='left')
            LMP_PRICES['DA_WD_PK'] = LMP_PRICES['DALMP']*LMP_PRICES['WDPEAK']
            LMP_PRICES['DA_WE_PK'] = LMP_PRICES['DALMP']*LMP_PRICES['WEPEAK']
            LMP_PRICES['DA_ON_PK'] = LMP_PRICES['DALMP']*LMP_PRICES['ONPEAK']
            LMP_PRICES['DA_OFF_PK'] = LMP_PRICES['DALMP']*LMP_PRICES['OFFPEAK']
            LMP_PRICES['DA_24HR'] = LMP_PRICES['DALMP']*LMP_PRICES['24HR']

            LMP_DA_PRICES = LMP_PRICES.drop(['ONPEAK','OFFPEAK','WEPEAK','24HR','WDPEAK','DALMP','RTLMP'],axis=1)
            LMP_DA_PRICES = LMP_DA_PRICES.rename(columns = {'DA_ON_PK':'ONPEAK','DA_OFF_PK':'OFFPEAK','DA_WE_PK':'WEPEAK','DA_WD_PK':'WDPEAK','DA_24HR':'24HR'})
            SRC_LMP_DA_PRICES = pd.melt(LMP_DA_PRICES,id_vars=['ISO','DATETIME','MARKETDAY','NODE'],value_vars=['OFFPEAK','ONPEAK','WEPEAK','WDPEAK','24HR'])
            SRC_LMP_DA_PRICES.columns = ['BU_ISO','DATETIME','MARKETDAY','SOURCE_ID','BU_PEAK_TYPE','SRC_DA_MCC_MWH']
            SRC_DA_inventory = pd.merge(inventory,SRC_LMP_DA_PRICES,on=['BU_ISO','SOURCE_ID','BU_PEAK_TYPE'],how='left')
            SRC_DA_inventory=SRC_DA_inventory.groupby(['FPT_SEGMENT_ID','DATETIME']).head(1)

            SNK_LMP_DA_PRICES = SRC_LMP_DA_PRICES.copy()
            SRC_LMP_DA_PRICES.columns = ['BU_ISO','DATETIME','MARKETDAY','SINK_ID','BU_PEAK_TYPE','SNK_DA_MCC_MWH']
            SRC_SNK_DA_inventory = pd.merge(SRC_DA_inventory,SRC_LMP_DA_PRICES,on=['BU_ISO','MARKETDAY','DATETIME','SINK_ID','BU_PEAK_TYPE'],how='left')
            SRC_SNK_DA_inventory=SRC_SNK_DA_inventory.groupby(['FPT_SEGMENT_ID','DATETIME']).head(1)

            SRC_SNK_DA_inventory['DA_MCC'] = np.where(SRC_SNK_DA_inventory['BU_HEDGE_TYPE'] == 'Obligation',SRC_SNK_DA_inventory['SNK_DA_MCC_MWH']-SRC_SNK_DA_inventory['SRC_DA_MCC_MWH'],np.maximum(SRC_SNK_DA_inventory['SNK_DA_MCC_MWH'].fillna(0)-SRC_SNK_DA_inventory['SRC_DA_MCC_MWH'].fillna(0),0))
            SRC_SNK_DA_INV_HRCNT = pd.merge(SRC_SNK_DA_inventory,MKT_HRCNT,on=['BU_ISO', 'DATETIME', 'MARKETDAY', 'BU_PEAK_TYPE'],how='left')

            FPT_DAILY_DTLS = SRC_SNK_DA_INV_HRCNT.groupby(['BU_ISO','FPT_ID','FPT_SEGMENT_ID','BU_PEAK_TYPE','MARKETDAY']).agg({'DA_MCC':'sum','HRCNT':'sum'}).reset_index()
            FPT_DAILY_DTLS = FPT_DAILY_DTLS[FPT_DAILY_DTLS['HRCNT']>0]
            FPT_DAILY_DTLS['DA_MCC'] = FPT_DAILY_DTLS['DA_MCC'].astype(float)
            FPT_DAILY_DTLS['HRCNT'] = FPT_DAILY_DTLS['HRCNT'].astype(int)
            FPT_DAILY_DTLS['DA_MCC_MWH'] = FPT_DAILY_DTLS['DA_MCC']/FPT_DAILY_DTLS['HRCNT']
            # FOR NYISO multipying value by -1
            FPT_DAILY_DTLS['DA_MCC_MWH']=np.where(FPT_DAILY_DTLS['BU_ISO'] == 'NYISO', -FPT_DAILY_DTLS['DA_MCC_MWH'],FPT_DAILY_DTLS['DA_MCC_MWH'])

            FPT_DAILY_DTLS['RT_MCC_MWH'] = None
            FPT_DAILY_DTLS = FPT_DAILY_DTLS.rename(columns = {'MARKETDAY':'FLOW_DATE'})
            FPT_DAILY_DTLS = FPT_DAILY_DTLS.drop(['BU_ISO','DA_MCC','BU_PEAK_TYPE'],axis=1)
            FPT_DAILY_DTLS = FPT_DAILY_DTLS[FPT_DAILY_DTLS['FLOW_DATE'] == Current_Date]

            FPT_DAILY_EXISTStxt = '''select fpd.* from POWERDB_DEV.PQUANT.FPT_DAILY fpd where fpd.FLOW_DATE = '{}' order by FPT_ID asc'''.format(Current_Date)
            conn = get_connection(username=susername, password=spass,role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
            cs=conn.cursor()
            cs.execute(FPT_DAILY_EXISTStxt)       
            FPT_DAILY_EXISTS = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
            conn.close()

            if (FPT_DAILY_EXISTS.shape[0] > 0):
                print(f"Delete for date {Current_Obj}")
                conn = get_connection(username=susername, password=spass,role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
                cs=conn.cursor()
                cs.execute('''delete from POWERDB_DEV.PQUANT.FPT_DAILY where FLOW_DATE = \'''' + Current_Date+ '\'')
                conn.close()

            FPT_DAILY_DTLS=FPT_DAILY_DTLS.groupby(['FPT_SEGMENT_ID','FLOW_DATE']).head(1)
            fpt_daily_txt = '''select top 1 * from PowerDB.PQUANT.FPT_DAILY'''    
            conn = get_connection(username=susername, password=spass,role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
            cs=conn.cursor()
            cs.execute(fpt_daily_txt)       
            fpt_daily_df = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])
            conn.close()

            cols = fpt_daily_df.columns
            for x in cols:
                if x not in list(FPT_DAILY_DTLS.columns):
                    FPT_DAILY_DTLS[x]=None   

            FPT_DAILY_DTLS = FPT_DAILY_DTLS[cols]  
            dir2 = 'C://TEMP'
            if not os.path.exists(dir2):
                os.makedirs(dir2)

            csv_file=r'C:\TEMP\FPT_DAILY_DA_DTLS_{}.csv'.format(Current_Date)
            FPT_DAILY_DTLS.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_MINIMAL)    
            conn=get_connection(username=susername, password=spass, role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
            conn.cursor().execute('USE DATABASE POWERDB_DEV')
            conn.cursor().execute('USE SCHEMA POWERDB_DEV.PQUANT')  
            conn.cursor().execute('remove @%FPT_DAILY')    
            conn.cursor().execute("PUT file://{} @%FPT_DAILY overwrite=true".format(csv_file))
            conn.cursor().execute('''copy into FPT_DAILY file_format=(type=csv skip_header=1)''')
            conn.close()   

            FPT_DAILY_PULLTxt= '''select fpd.* from POWERDB_DEV.PQUANT.FPT_DAILY fpd 
                                left join POWERDB_DEV.PQUANT.FPT_RUN_LOG rp
                                on rp.FPT_ID = fpd.FPT_ID
                                where rp.RUN_STATUS = 'TRANSACTION_INVENTORY_MONTHLY' and fpd.FLOW_DATE >= '{}' order by FPT_ID asc'''.format(month_start)
            conn = get_connection(username=susername, password=spass, role='OWNER_POWERDB_DEV',database='POWERDB_DEV',schema='PQUANT')
            cs=conn.cursor()
            cs.execute(FPT_DAILY_PULLTxt)       
            FPT_DAILY_PULL = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])   
            conn.close()  
            rows = len(csv_file)
            if os.path.exists(dir2):
                os.remove(csv_file)

            FPT_DAILY_INV_DTLS = pd.merge(FPT_DAILY_PULL,inventory,on=['FPT_ID','FPT_SEGMENT_ID'],how='left')
            FPT_DAILY_INV_DTLS['DA_PNL'] = np.where(FPT_DAILY_INV_DTLS['BU_TRADE_TYPE'] == 'BUY',(FPT_DAILY_INV_DTLS['DA_MCC_MWH'] - FPT_DAILY_INV_DTLS['FTR_COST']),(FPT_DAILY_INV_DTLS['FTR_COST'] - FPT_DAILY_INV_DTLS['DA_MCC_MWH']))*FPT_DAILY_INV_DTLS['FTR_MW']*FPT_DAILY_INV_DTLS['HRCNT']
            FPT_DAILY_INV_DTLS.FLOW_DATE = pd.to_datetime(FPT_DAILY_INV_DTLS.FLOW_DATE)
            FPT_DAILY_INV_DTLS['FLOW_DATE'] = FPT_DAILY_INV_DTLS['FLOW_DATE'].dt.strftime('%Y-%m-%d')
            FPT_DAILY_INV_DTLS['DAYNO'] = FPT_DAILY_INV_DTLS['FLOW_DATE'].str[8:10]
            
            #Net invesment calculations
            #FPT_DAILY_INV_DTLS['MTD_INVEST'] = FPT_DAILY_INV_DTLS['FTR_COST']*FPT_DAILY_INV_DTLS['FTR_MW']*FPT_DAILY_INV_DTLS['HRCNT']
            FPT_DAILY_INV_DTLS['MTD_INVEST'] = np.where(FPT_DAILY_INV_DTLS['BU_TRADE_TYPE'] == 'BUY', FPT_DAILY_INV_DTLS['FTR_COST']*FPT_DAILY_INV_DTLS['FTR_MW']*FPT_DAILY_INV_DTLS['HRCNT'], -1*FPT_DAILY_INV_DTLS['FTR_COST']*FPT_DAILY_INV_DTLS['FTR_MW']*FPT_DAILY_INV_DTLS['HRCNT'])
            stats=FPT_DAILY_INV_DTLS.groupby(['BU_ISO','DAYNO', 'BU_TRADER']).agg({'DA_PNL':'sum','MTD_INVEST':'sum'}).reset_index()
            stats.DAYNO = stats.DAYNO.astype(str)
            stats.DA_PNL = stats.DA_PNL.astype(float)
            stats.MTD_INVEST = stats.MTD_INVEST.astype(float)
            pnl_by_part=stats.groupby(['BU_ISO','BU_TRADER']).agg({'DA_PNL':'sum','MTD_INVEST':'sum'}).reset_index()
            pnl_by_dayno=stats.groupby(['DAYNO'])['DA_PNL'].sum().reset_index()

            # stats=FPT_DAILY_INV_DTLS.groupby(['BU_ISO','DAYNO', 'BU_TRADER'])['DA_PNL'].sum().reset_index()
            # stats.DAYNO = stats.DAYNO.astype(str)
            # stats.DA_PNL = stats.DA_PNL.astype(float)
            # pnl_by_part=stats.groupby(['BU_ISO','BU_TRADER'])['DA_PNL'].sum().reset_index()
            # pnl_by_dayno=stats.groupby(['DAYNO'])['DA_PNL'].sum().reset_index()

            stats_trans=stats.pivot_table(index=['BU_ISO','BU_TRADER'], columns='DAYNO', values='DA_PNL').reset_index()
            stats_trans=stats_trans.merge(pnl_by_part, on=['BU_ISO','BU_TRADER'])
            subject = 'TEST ::FTR DA_PNL MTD as of {}'.format(Current_Obj.strftime('%Y-%m-%d %H:%M:%S'))
            msgbody='<br/><table style="border-top: 1px solid grey; border-left:1px solid grey; border-spacing: 0px">'
            msgbody=msgbody+'<tr>'
            msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">ISO</td>'
            msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">BU_TRADER</td>'
            msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">NET_INVEST_MTD</td>'

            daynos=list(pnl_by_dayno['DAYNO'].unique())
            daynos.sort()

            for i in daynos:
                msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">{}</td>'.format(i)
            msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">MTD</td>'
            msgbody=msgbody+'</tr>'

            
            for i, x in stats_trans.iterrows():
                msgbody=msgbody+'<tr>'
                msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">{}</td>'.format(x['BU_ISO'])
                msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">{}</td>'.format(x['BU_TRADER'])
                msgbody=msgbody+'<td style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">{}</td>'.format(round(x['MTD_INVEST']))

                for j in daynos:
                    if x[j]>0:
                        msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:green">{:,.0f}</td>'.format(x[j])
                    else:
                        msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:red">{:,.0f}</td>'.format(x[j])
                if x['DA_PNL']>0:
                    msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:green">{:,.0f}</td>'.format(x['DA_PNL'])
                else:
                    msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:red">{:,.0f}</td>'.format(x['DA_PNL'])

                msgbody=msgbody+'</tr>'
                
            msgbody=msgbody+'<tr><td colspan="2" style="font-weight: bold; text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;">Grand Total</td>'
            msgbody=msgbody+'<td style="font-weight: bold;text-align:center;border-bottom: 1px solid grey; border-right:1px solid grey;color:solid grey">{:,.0f}</td>'.format(stats['MTD_INVEST'].sum())
            for j in daynos:
                temp_dayno=pnl_by_dayno[pnl_by_dayno['DAYNO']==j]['DA_PNL'].values[0]
                if temp_dayno>0:
                    msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:green">{:,.0f}</td>'.format(temp_dayno)
                else:
                    msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:red">{:,.0f}</td>'.format(temp_dayno)
            if stats['DA_PNL'].sum()>0:
                msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:green">{:,.0f}</td>'.format(stats['DA_PNL'].sum())
            else:
                msgbody=msgbody+'<td style="font-weight: bold;text-align:right;border-bottom: 1px solid grey; border-right:1px solid grey;color:red">{:,.0f}</td>'.format(stats['DA_PNL'].sum())

            msgbody=msgbody+'</tr>'
            msgbody=msgbody+'</table>'

            # msgbody=msgbody+'<br/><br/>CSV file location: G:\Gas & Pwr\Pwr\FTR\Michael\ANILTEST\ftr_daily_detail.csv'
            msgbody=msgbody+'<br/><br/>Python file location: \\biourja.local\biourja\Data\IT Dev\Production_Environment\FPT_DA\FPT_DAILY_DA_PNL_MODULE.py'
            # msgbody=msgbody+'<br/><br/>YES FTR DAILY DA/RT OBL Projection LIVE: https://prod-useast-a.online.tableau.com/#/site/biourja/views/FTRDARevenueProjectionLIVE/ALLPEERTable '
            msgbody=msgbody+'<br/><br/>YES FTR DAILY DA/RT OBL Projection LIVE: https://prod-useast-a.online.tableau.com/#/site/biourja/views/FPTDailyPortfolioAnalysis_LIVE/FTRDAPnLMTD?:iid=6 '
        
            logging.info('Execution Done')
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name="FPT_DAILY_DA_PNL_MODULE",database='POWERDB',status='Completed',table_name= 'POWERDB.PQUANT.FPT_DAILY', row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner='Rahul')
            bu_alerts.send_mail(
                receiver_email = receiver_email,
                mail_subject='JOB SUCCESS - FPT_DAILY_DA_PNL_MODULE',
                mail_body='FPT_DAILY_DA_PNL_MODULE completed successfully, Attached logs',
                attachment_location = log_file_location
            )
            bu_alerts.send_mail(
                receiver_email = receiver_email_business,
                mail_subject = subject,
                mail_body = msgbody,
                attachment_location = log_file_location
            )
        except Exception as e:
            print("Exception caught during execution: ",e)
            logging.exception(f'Exception caught during execution: {e}')
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name="FPT_DAILY_DA_PNL_MODULE",database='POWERDB',status='Failed',table_name='POWERDB.PQUANT.FPT_DAILY', row_count=rows, log=log_json, warehouse='QUANT_WH',process_owner='Rahul') 
            bu_alerts.send_mail(
                receiver_email = receiver_email,
                mail_subject='JOB FAILED - FPT_DAILY_DA_PNL_MODULE',
                mail_body='FPT_DAILY_DA_PNL_MODULE failed during execution, Attached logs',
                attachment_location = log_file_location
            )
    endtime=datetime.now()   
    print('Complete work at {} ...'.format(endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format((endtime-starttime).total_seconds()))