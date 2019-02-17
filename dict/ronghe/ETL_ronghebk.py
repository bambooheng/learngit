import pandas as pd
from datetime import time
import re
import time
import numpy as np
import math
import sys
print("begin")
###############################################信用卡#################################################

#更新信用卡发放时间（直接提取的字段结清状态发放日期为空）
def getsGiveDate(string = None):
    if str(string) == 'None':
        return None        
    try:
        Typefinance = re.findall(r'\d{4}年\d{2}月\d{2}日',string)[0]                        
        return Typefinance
    except IndexError:
        return None 
    
def deadLineDate(string = None):
    if str(string) == 'None':
        return None        
    try:
        Typefinance = re.findall(r'截至(\d{4}年\d{2}月\d{2}日)',string)[0]                        
        return Typefinance
    except IndexError:
        return None 

#取信用卡机构
def getsOperaterfinance(string = None):
    if str(string) == 'None':
        return None
    try:
        Operaterfinance = re.findall(r'日([\u4e00-\u9fa5]{2,12})“',string)[0]                        
        return Operaterfinance
    except IndexError:
        return None  

#取信用卡账户类型
def getsAccountType(string = None):
    if str(string) == 'None':
        return None
    try:
        accountType = re.findall(r'（([\u4e00-\u9fa5]{4,8})）',string)[0]                        
        return accountType
    except IndexError:
        return None 
    
#信用卡类型
def convertLimitType(string = None):
    if string == 0:
        return '贷记卡'
    elif string == 1:
        return '准贷记卡'
    else:
        return None

#处理时间
def handleDate(string = None):
    if str(string) == 'None':
        return '2050-01-01'
    try:
        Date = str(string).replace('年','-')\
                          .replace('月','-')\
                          .replace('日','')
        return pd.to_datetime(Date,format='%Y-%m-%d')
    except IndexError:
        return None
    
#取销户状态
def getcancellation(string = None):
    if str(string) == 'None':
        return None
    if string == '销户':
        cancellation = '已销户'
        return cancellation
    elif string in ['正常','未激活','呆账','止付','冻结']:
        cancellation = '未销户'
        return cancellation
    else:
        return None

#取激活状态
def getactivation(string = None):
    if str(string) == 'None':
        return None
    if string == '未激活':
        activation = '未激活'
        return activation
    elif string in ['正常','销户','呆账','止付','冻结']:
        activation = '已激活'
        return activation
    else:
        return None
        
#overdue明细表中的逾期记录汇总到每张卡
def cal_old_overdue(df):
    #定义：每月平均逾期金额
    df['overdue_amount_permonth'] = df['overdue_amount'] / df['overdue_period']
    df_new = df.drop_duplicates(['person_check_id','card_id'],'last').reset_index(drop=True)
    del df_new['overdue_month'],df_new['overdue_period'],df_new['overdue_amount'],df_new['overdue_amount_permonth']
    seq = [1,3,6,12,18,24,36,48,60]
    for i in seq:
        gap = i * 30
        temp =  df.loc[((df['overdue_period']>0)&(df['od_ddl_gap']*30 + df['ddl_re_gap']>=-gap)),:]
        #seq个月之内的逾期期数
        temp1 = temp.groupby(['person_check_id','card_id'],as_index=False)['overdue_month'].count()
        df_new = df_new.merge(temp1, how='left', on=['person_check_id','card_id'])
        df_new.loc[(np.isnan(df_new['card_id']) == False)&(np.isnan(df_new['overdue_month'])==True),'overdue_month'] = 0
        #seq个月之内最大逾期期数
        temp2 = temp.groupby(['person_check_id','card_id'],as_index=False)['overdue_period'].max()
        df_new = df_new.merge(temp2, how='left', on=['person_check_id','card_id'])
        df_new.loc[(np.isnan(df_new['card_id']) == False)&(np.isnan(df_new['overdue_period'])==True),'overdue_period'] = 0
        #seq个月之内逾期金额
        temp3 = temp.groupby(['person_check_id','card_id'],as_index=False)['overdue_amount_permonth'].sum()
        df_new = df_new.merge(temp3, how='left', on=['person_check_id','card_id'])
        df_new.loc[(np.isnan(df_new['card_id']) == False)&(np.isnan(df_new['overdue_amount_permonth'])==True),'overdue_amount_permonth'] = 0

        df_new.rename(columns={'overdue_month':str(i)+'_old_od','overdue_period':str(i)+'_max_old_od','overdue_amount_permonth':str(i)+'_old_amount'}, inplace = True)
        #seq个月之内账单数
        #关于账单数的处理：以卡片发放时间为起点、截止时间为终点，处于观察范围（观察范围为报告返回时间往前推某个时间段）内的时间天数除以30后向上取整    
        df_new.loc[np.isnan(df_new['card_id']) == False,str(i)+'_bills'] = df_new.loc[np.isnan(df_new['card_id']) == False,:].apply(lambda x: math.ceil(max(0,(min(-x['rl_re_gap'],gap) + x['ddl_re_gap'] +1) / 30) ),axis = 1)

    return df_new                

#求逾期期数
def cal_od_period_sum(flag, old, gap , seq):
    if flag == None or flag == '':
        return old
    else:
        overdue_days_sum = 0
        ls = ["1","2","3","4","5","6","7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        if n !=24:
            return old
        else:
            for i in range(n):
                if flag_list[i] in ls and -gap + (23 - i) * 30 <= seq * 30:
                    overdue_days_sum += 1
            if np.isnan(old) == True:
                return overdue_days_sum
            else:
                return overdue_days_sum + old         

#求最大逾期期数
def cal_od_period_max(flag, old,gap , seq):
    if flag == None or flag == '':
        return old
    else:
        maxseq = 0
        ls = ["1","2","3","4","5","6","7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list2 = []
        if n !=24:
            return old
        else:
            for i in range(n):
                if flag_list[i] in ls and -gap + (23 - i) * 30 <= seq * 30:
                    flag_list2.append(int(flag_list[i]))
            if len(flag_list2) == 0:
                maxseq = 0
            else:
                maxseq = max(flag_list2)   
            if np.isnan(old) == True:
                return maxseq
            else:
                return max(maxseq,old)
    
#求账期逾期率
def cal_od_ratio(flag, gap , old,bills ,seq):
#    flag = df_new['repay_detail_24']
#    gap=df_new['ddl_re_gap']
#    old=df_new['36_old_od']
#    bills=df_new['36_bills']
#    seq = 36
    if bills <= 0 or np.isnan(bills) == True:
        return bills
    else:
        overdue_days_sum = 0
        ls = ["1","2","3","4","5","6","7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        if n !=24:
            return old / bills
        else:
            for i in range(n):
                if flag_list[i] in ls and -gap + (23 - i) * 30 <= seq * 30:
                    overdue_days_sum += 1
            if np.isnan(old) == True:
                return overdue_days_sum / bills
            else:
                return (overdue_days_sum + old) / bills

#按order_id汇总变量 
def con2(df):
    df_new = cal_old_overdue(df)
    df['activation'] = df['account_status'].map(getactivation)
    df = df.loc[df['activation'] != '未激活']
    df_new['cardtype'] = df_new['limit_type'].map(convertLimitType)
    df_new['cancellation'] = df_new['account_status'].map(getcancellation)
    seq = [1,3,6,12,18,24,36,48,60]
    type_names = ['贷记卡','准贷记卡']
    account_status = ['未销户','已销户']
    overDue = pd.DataFrame({'person_check_id':df_new['person_check_id'].unique()})
    for j in seq:
        df_new[str(j)+'_overdue_period'] = df_new.apply(lambda x: cal_od_period_sum(x['repay_detail_24'],x[str(j)+'_old_od'], x['ddl_re_gap'],j),axis = 1)
        df_new[str(j)+'_overdue_ratio'] = df_new.apply(lambda x: cal_od_ratio(x['repay_detail_24'], x['ddl_re_gap'],x[str(j)+'_old_od'],x[str(j)+'_bills'],j),axis = 1)
        df_new[str(j)+'_overdue_max_period'] = df_new.apply(lambda x: cal_od_period_max(x['repay_detail_24'],x[str(j)+'_max_old_od'], x['ddl_re_gap'],j),axis = 1)
        for i in type_names:
            for l in account_status:
                name1 = '近' + str(j) + '个月' + l + i + '总逾期金额'
                name2 = '近' + str(j) + '个月' + l + i + '总逾期期数'
                name3 = '近' + str(j) + '个月' + l + i + '最大账期逾期率'
                name4 = '近' + str(j) + '个月' + l + i + '平均账期逾期率'
                name5 = '近' + str(j) + '个月' + l + i + '最大连续逾期期数'
                temp = df_new.loc[(df_new['cardtype'] == i)&(df_new['cancellation']==l)]
                
                overdue1 = temp.groupby('person_check_id',as_index=False)[str(j)+'_old_amount'].sum()
                overdue1.rename(columns={str(j)+'_old_amount':name1}, inplace = True) 
                
                overdue2 = temp.groupby('person_check_id',as_index=False)[str(j)+'_overdue_period'].sum()
                overdue2.rename(columns={str(j)+'_overdue_period':name2}, inplace = True)
                
                overdue3 = temp.groupby('person_check_id',as_index=False)[str(j)+'_overdue_ratio'].max()
                overdue3.rename(columns={str(j)+'_overdue_ratio':name3}, inplace = True)
                
                overdue4 = temp.groupby('person_check_id',as_index=False)[str(j)+'_overdue_period',str(j)+'_bills'].sum()
                overdue4[name4] = overdue4[str(j)+'_overdue_period'] / overdue4[str(j)+'_bills']
                overdue4 = overdue4.drop([str(j)+'_overdue_period',str(j)+'_bills'], axis=1)

                overdue5 = temp.groupby('person_check_id',as_index=False)[str(j)+'_overdue_max_period'].max()
                overdue5.rename(columns={str(j)+'_overdue_max_period':name5}, inplace = True)

                overDue = overDue.merge(overdue1,how='left',left_on='person_check_id',right_on='person_check_id')\
                                 .merge(overdue2,how='left',left_on='person_check_id',right_on='person_check_id')\
                                 .merge(overdue3,how='left',left_on='person_check_id',right_on='person_check_id')\
                                 .merge(overdue4,how='left',left_on='person_check_id',right_on='person_check_id')\
                                 .merge(overdue5,how='left',left_on='person_check_id',right_on='person_check_id')
                
                overDue.loc[overDue[name3] ==0,name4] = 0 
        overdue1 = df_new.groupby('person_check_id',as_index=False)[str(j)+'_overdue_period'].sum()
        overdue1.rename(columns={str(j)+'_overdue_period':'信用卡'+str(j)+'个月内逾期期数'}, inplace = True)
        overdue2 = df_new.groupby('person_check_id',as_index=False)[str(j)+'_overdue_max_period'].max()
        overdue2.rename(columns={str(j)+'_overdue_period':'信用卡'+str(j)+'个月内最高逾期期数'}, inplace = True)
        overDue = overDue.merge(overdue1,how='left',left_on='person_check_id',right_on='person_check_id')\
                         .merge(overdue2,how='left',left_on='person_check_id',right_on='person_check_id')    
    return overDue
#按order_id汇总变量 
#def con(df):
#    df = dfCard_all
#    df_new = cal_old_overdue(df)
#    df['activation'] = df['account_status'].map(getactivation)
#    df = df.loc[df['activation'] != '未激活']
#    df_new['cardtype'] = df_new['limit_type'].map(convertLimitType)
#    df_new['cancellation'] = df_new['account_status'].map(getcancellation)
#    seq = [36]
#    type_names = ['贷记卡']
#    account_status = ['未销户']
#    overDue = pd.DataFrame({'person_check_id':df_new['person_check_id'].unique()})
#    for j in seq:
#        j = 36
#
#        for i in type_names:
#            for l in account_status:
#                name3 = '近' + str(j) + '个月' + l + i + '最大账期逾期率'
#                temp = df_new.loc[(df_new['cardtype'] == i)&(df_new['cancellation']==l)]
#                
#                overdue3 = temp.groupby('order_id',as_index=False)[str(j)+'_overdue_ratio'].max()
#                overdue3.rename(columns={str(j)+'_overdue_ratio':name3}, inplace = True)
#                
#                overDue = overDue.merge(overdue3,how='left',left_on='person_check_id',right_on='person_check_id')
#        
#    return overDue                     
                
# 将时间从乱七八糟转换成标准时间戳
def time_format(string=None, type=1):
    if str(string) == 'None':
        return None
    elif type == 1:
        string = string.replace('年', '-')
        string = string.replace('月', '-')
        string = string.replace('日', '')
        dt_stamp = time.mktime(time.strptime(string.strip(), '%Y-%m-%d'))
        return dt_stamp
    elif type == 2:
        dt = string.split('.')[0].strip()
        dt_stamp = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
        return dt_stamp
    else:
        pass


# 从字符串中找到截止 年 月 日的日期
def str_find_date(string=None):
    if str(string) == 'None':
        return None
    else:
        try:
            date = re.findall('截至((\d{4})年(\d{1,2})月(\d{1,2})日)', string)[0][0]
            return date
        except IndexError:
            return None


def overdue_cal(flag, deadline_timestamp, re_timestamp, seq):
    if flag == None or flag == '':
        return None
    else:
        period = 3600 * 24 * 30  # 一个月多少秒
        overdue_days_sum = 0
        maxseq = 0
        ls = ["1", "2", "3", "4", "5", "6", "7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list2 = []
        if n != 24:
            return None
        else:
            for i in range(n):
                if flag_list[i] in ls and re_timestamp - (deadline_timestamp - (23 - i) * period) <= seq * period:
                    overdue_days_sum += 1
                    flag_list2.append(int(flag_list[i]))
            if len(flag_list2) == 0:
                maxseq = 0
            else:
                maxseq = max(flag_list2)  # seq内最大逾期期数
    return overdue_days_sum


def overdue_cal3(flag, deadline_timestamp, re_timestamp, seq):
    if flag == None or flag == '':
        return None
    else:
        period = 3600 * 24 * 30  # 一个月多少秒
        overdue_days_sum = 0
        maxseq = 0
        ls = ["1", "2", "3", "4", "5", "6", "7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list2 = []
        if n != 24:
            return None
        else:
            for i in range(n):
                if flag_list[i] in ls and re_timestamp - (deadline_timestamp - (23 - i) * period) <= seq * period:
                    overdue_days_sum += 1
                    flag_list2.append(int(flag_list[i]))
            if len(flag_list2) == 0:
                maxseq = 0
            else:
                maxseq = max(flag_list2)  # seq内最大逾期期数
    return maxseq


# 全部 逾期期数 最高逾期期数
def overdue_cal2(flag):
    if flag == None or flag == '':
        return None
    else:
        period = 3600 * 24 * 30  # 一个月多少秒
        overdue_days_sum24 = 0
        maxseq24 = 0
        ls = ["1", "2", "3", "4", "5", "6", "7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list3 = []
        if n != 24:
            return None
        else:
            for i in range(n):
                if flag_list[i] in ls:
                    overdue_days_sum24 += 1
                    flag_list3.append(int(flag_list[i]))
            if len(flag_list3) == 0:
                maxseq24 = 0
            else:
                maxseq24 = max(flag_list3)  # 24个月最大逾期期数
    return overdue_days_sum24


def overdue_cal4(flag):
    if flag == None or flag == '':
        return None
    else:
        period = 3600 * 24 * 30  # 一个月多少秒
        overdue_days_sum24 = 0
        maxseq24 = 0
        ls = ["1", "2", "3", "4", "5", "6", "7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list3 = []
        if n != 24:
            return None
        else:
            for i in range(n):
                if flag_list[i] in ls:
                    overdue_days_sum24 += 1
                    flag_list3.append(int(flag_list[i]))
            if len(flag_list3) == 0:
                maxseq24 = 0
            else:
                maxseq24 = max(flag_list3)  # 24个月最大逾期期数
    return maxseq24

#求时间差
#按天
def cal_timegap_d(start_time=None,end_time=None):
    if str(start_time) =='None'  or str(end_time) =='None':
        return None
    else:
        days_gap = start_time - pd.to_datetime(end_time)
        days_gap = days_gap/np.timedelta64(1,'D')
    return days_gap

#按月
def cal_timegap_m(start_time=None,end_time=None):
#    if start_time.isnull() or end_time.isnull():
    if str(start_time) =='None'  or str(end_time) =='None':
        return None
    else:
        end_time = str(end_time)
        start_time = str(start_time)
        months_gap = (int(end_time[0:4]) - int(start_time[0:4])) * 12 + (int(end_time[5:7]) - int(start_time[5:7]))
    return -months_gap 


###############################################贷款#################################################

##new融和
def con(df):
    seq = [3,6,12,24]
    overDue = pd.DataFrame({'person_check_id':df['person_check_id'].unique()})
    for j in seq:
        name2 = 'loan_m' + str(j) + '_overdue_max_num'
        df[name2] = df.apply(lambda x: overdue_cal3(x['repay_detail_24'], x['deadline'], x['re_time'],j),axis = 1)    
        overdue2 = df.groupby('person_check_id',as_index=False)[name2].max()
        overDue = overDue.merge(overdue2,how='left',left_on='person_check_id',right_on='person_check_id')
    return overDue[['loan_m24_overdue_max_num']]

# 取贷款金额
def getsLoanamount(string=None):
    if str(string) == 'None':
        return None
    try:
        Loanamount = re.findall('.*发放的(.*?)元.*', string)[0]
        return Loanamount
    except IndexError:
        return None
    
#取贷款机构
def getsOperaterfinance(string = None):
    if str(string) == 'None':
        return None
    try:
        Operaterfinance = re.findall(r'日([\u4e00-\u9fa5]{2,12})“',string)[0]                        
        return Operaterfinance
    except IndexError:
        return None      
 

#合并贷款机构类型
def convertOperaterfinance(string = None):
    if str(string) == 'None':
        return None
    elif str(string).find('银行') >= 0:
        return '银行'
    elif str(string).find('公司') >= 0 or str(string).find('中心') >= 0 :
        return '非银'
    else:
        return '其他'
        
#处理贷款状态
def loanStatus(string = None):
    if str(string) == 'None':
        return None
    elif str(string) == '结清':
        return '已结清'
    else:
        return '未结清'

#求最大逾期期数
def cal_od_period_max(flag, old,gap , seq):
    if flag == None or flag == '':
        return old
    else:
        maxseq = 0
        ls = ["1","2","3","4","5","6","7"]
        flag_list = flag.split('|')
        n = len(flag_list)
        flag_list2 = []
        if n !=24:
            return old
        else:
            for i in range(n):
                if flag_list[i] in ls and -gap + (23 - i) * 30 <= seq * 30:
                    flag_list2.append(int(flag_list[i]))
            if len(flag_list2) == 0:
                maxseq = 0
            else:
                maxseq = max(flag_list2)   
            if np.isnan(old) == True:
                return maxseq
            else:
                return max(maxseq,old)    


           
# 打分部分
def score_al_m12_cell_notbank_orgnum(x):
    if x <= 0:
        y = 18
    elif x <= 1:
        y = 55
    elif x <= 3:
        y = 36
    else:
        y = 0
    return y


def score_td_score(x):
    if x <= 0:
        y = 70
    elif x <= 9:
        y = 60
    elif x <= 17:
        y = 49
    elif x <= 50:
        y = 27
    else:
        y = 0
    return y

def loan_m24_overdue_max_num(x):
    if x <= -1:
        y = 21
    elif x <= 0:
        y = 60
    else:
        y = 0
    return y

def score_loan_max(x):
    if x <= 0:
        y = 28
    elif x <= 5000:
        y = 9
    elif x <= 95000:
        y = 0
    elif x <= 250000:
        y = 35
    else:
        y = 55
    return y

def score_unsettled_nonbank_count(x):
    if x <= -1:
        y = 13
    elif x <= 0:
        y = 28
    elif x <= 1:
        y = 14
    elif x <= 3:
        y = 5
    else:
        y = 0
    return y    
 
def score_ccount_active_rmb_mob_avg(x):
    if x <= 0:
        y = 15
    elif x <= 12:
        y = 0
    elif x <= 24:
        y = 12
    elif x <= 48:
        y = 28
    else:
        y = 71
    return y

def score_query_org_credit_loan_num_12m(x):
    if x <= -1:
        y = 0
    elif x <= 2:
        y = 24
    elif x <= 5:
        y = 19
    else:
        y = 5
    return y

def credit_36m_max_od_ratio(x):
    if x <= -1:
        y = 32
    elif x <= 0:
        y = 70
    elif x <= 0.1:
        y = 48
    else:
        y = 0
    return y



def score_age(x):
    if x <= 24:
        y = 0
    elif x <= 33:
        y = 16
    elif x <= 42:
        y = 37
    elif x <= 50:
        y = 31
    else:
        y = 8
    return y
