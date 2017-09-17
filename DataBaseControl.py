
# -*- coding: utf-8 -*-
"""
create at 2016/12/12 by 唐剑刚
数据库基本操作
"""
import pandas as pd
import cx_Oracle
 
class DataBaseRun:
    def __init__(self,db):
        dsn=cx_Oracle.makedsn('10.8.0.165','1521','quan')
        usr=''
        pwd=''
        
        if db == 'qa2':
            usr='qa2'
            pwd='qa2'
        elif db =='wind' :
            usr='wind'
            pwd='wind'
        elif db=='fof':
            usr='fof'
            pwd='fof'
        
        self.__connection=cx_Oracle.connect(usr,pwd,dsn) 
        
    def __del__(self):  
        self.__connection.close()

    
    '''根据sql来返回一个 DataFrame'''  
    def exec_sql_byResult(self,sql):
        try:
            __df_result = pd.read_sql(sql, con=self.__connection)
            __df_result.columns = map(str.lower, __df_result.columns)            
            return __df_result
        except Exception as e:            
            raise e

    '''根据sql来返回一个Vector'''
    def exec_sql_bysingle(self,sql):
        try:        
            __cursor = self.__connection.cursor()
            __r= __cursor.execute(sql)
            __result = __r.fetchall()
            __lst = []
            for i in __result:
                __lst.append(i[0])
            return __lst        
        except Exception as e:
            raise e       
            
    '''执行一个SQL，不返回结果'''
    def exec_sql(self,sql):
        try:        
            __cursor = self.__connection.cursor()
            __cursor.execute(sql)
            self.__connection.commit()
        except Exception as e:
            raise e               