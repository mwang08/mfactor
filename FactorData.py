# -*- coding: utf-8 -*-
"""
create at 2017/08/22 by 唐剑刚
因子测试脚本
"""
from DataBaseControl import DataBaseRun
import pickle as p
import pandas as pd
import numpy as np
import alphalens as al
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
import statsmodels.api as sm

class FactorDataBase:
    '因子基础数据类'
    def __init__(self, targetfactor=[],startdate=None,enddate=None):
        self.universes = ['hs300','zz500','zz800','riskmodel']
        self.targetfactor = targetfactor     
        self.startdate = startdate
        self.enddate = enddate
        self.dbrun = DataBaseRun('qa2')
        
    def getFactorData(self):        
        ##获取股票的基本信息(调仓节点前一天的权重、行业属性，调仓当日的收盘价)
        sql = '''                
                select to_date(b.tradedate,'YYYYMMDD') "date",a.secucode asset,d.firstname industry,nvl(a.totalshares*a.closeprice,0) mktcap,
                nvl(a.freefloats*a.closeprice,0) freemktcap,c.adjcloseprice price,
                nvl(e.hs300,0) hs300,nvl(e.zz500,0) zz500,nvl(e.zz800,0) zz800,nvl(e.riskmodel,0) riskmodel
                from allashare a,shtradeday b,asharequote c,mv_industry_today d,                
                (select * from (select tradedate,secucode,indexsecucode,weight from indexweight2 union all
                select tradedate,secucode,'rm' indexsecucode,weight2 weight from rmestu)
                pivot(sum(weight) for(indexsecucode) in ('000300.SH' as HS300,'000905.SH' as ZZ500,'000906.SH' as ZZ800,'rm' as riskmodel)))e                
                where b.tradedate between #startdate# and #enddate#
                and a.tradedate = b.p1 and b.ifmonthstart = 'Y'                       
                and a.secucode=c.secucode(+) and a.tradedate=c.tradedate(+)
                and a.secucode=d.secucode(+)
                and a.secucode=e.secucode(+)
                and a.tradedate=e.tradedate(+)
                order by b.tradedate asc , a.secucode asc  
              
        '''
        sql = sql.replace("#startdate#", str(self.startdate))
        sql = sql.replace("#enddate#", str(self.enddate))        
        print ("获取股票基本信息脚本\n[%s]"%(sql))
        basedata = self.dbrun.exec_sql_byResult(sql)            
        basedata.set_index(['date','asset'], inplace=True)
    
        str_inlist = ",".join([str(t) for t in self.targetfactor])
        sql = '''                
                select to_date(b.tradedate,'YYYYMMDD') "date",a.secucode asset,a.factorcode,a.factorvalue
                from factordata a,shtradeday b
                where b.tradedate between #startdate# and #enddate#
                and a.tradedate = b.p1 and b.ifmonthstart = 'Y'
                and a.factorcode in (#inlist#)
                order by b.tradedate asc , a.secucode asc                 
        '''        
        sql = sql.replace("#inlist#", str_inlist)
        sql = sql.replace("#startdate#", str(self.startdate))
        sql = sql.replace("#enddate#", str(self.enddate))
        print ("获取股票的因子值数据\n[%s]"%(sql))        
        factordata = self.dbrun.exec_sql_byResult(sql)            
        factordata.set_index(['date','asset','factorcode'], inplace=True)
        factordata = factordata.factorvalue.unstack()        
        
        
        print("两个数据矩阵的大小分别为[%s,%s]"%(len(basedata),len(factordata)))         
        self.factordata = pd.concat([basedata, factordata], axis=1)
        del basedata,factordata        


if __name__ == "__main__":
    u = FactorDataBase([100145,100058],20100201,20170822)
    aa = u.getFactorData()
    a = u.factordata
