# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 11:07:18 2017

@author: wangming
"""

#mfactor: multi factors analysis based on alphalens

import pandas as pd
import numpy as np
import alphalens as al
from scipy import stats
import statsmodels.api as sm

#import sqlalchemy as sa
#import numba


class MFactor:
    
    def __init__ (self, data_df, factor_code, clean_data=False):
        """
        define a multi-factor DataFrame with factors name in columns and [date,
        asset] in index
        ------------------
        Must have: [price, industry, size]
        ------------------
        Parameters:
        data_df: factor data loaded from Oracle db
        factor_code_df: factor code list
        """
        
        self.factor_code = factor_code
        
        #raw_data
        #self.raw_data = data_df.copy()
        #data can be changed in the follwing process
        self.data = data_df.copy()
        if 'mktcap' not in data_df.columns:
            self.data['mktcap'] = np.exp(self.data.loc[:,'size'])
        if 'ret' not in data_df.columns:
            self.data['ret'] = self.data.price.unstack().pct_change().shift(-1).stack()
        self.data.dropna(subset=['ret'], inplace=True)
        
        if clean_data:
            self.fillna()
            self.apply_winsorize()
            self.apply_simple_normalize()
        
        
    
    def fillna(self, target_factor=None, method='mean', by_industry=False):
        """fill the missing value
        
        Parameters
        ----------
        target_factor: list of factor code wish to fill nan value, default None
        method: {'mean', 'weighted_mean'} default 'mean'
        by_industry: default False, if True, apply fillna mehod within each industry
        
        Return
        ------
        None, replace self.data with filled value
        """
        
        
        def fillna_with_mean(series):
            return series.fillna(np.mean(series))
        
        def fillna_with_weighted_mean(df, factor):
            return df[factor].fillna(np.sum(df[factor]*df.loc[:,'mktcap'])/\
                     np.sum(df.loc[:,'mktcap']))
        if target_factor is None:
            with_nan_factors = pd.isnull(self.data[self.factor_code]).any()[
                pd.isnull(self.data[self.factor_code]).any()==True].index.tolist()
            target_factor = with_nan_factors
        
        _grouper = ['date']
        if by_industry:
            _grouper.append('industry')
        
        if len(target_factor) == 0:
            print ('No nan value found!')
        else:
            for factor in target_factor:
                if method == 'mean':
                    self.data[factor] = self.data[factor].groupby(_grouper,
                             group_keys=False).apply(fillna_with_mean)
                elif method == 'weighted_mean':
                    self.data[factor] = self.data.groupby(_grouper,
                             group_keys=False).apply(fillna_with_mean, factor)
                else:
                    raise AttributeError('Please provide a valid fill nan method...')


    def apply_winsorize(self, target_factor=None, method='mstd', by_industry=False):
        """winsorize the outlier of a factor
        
        Parameters
        ----------
        target_factor: list of target factor
        method: {'mstd', 'qcut'} default 'mstd'
            'mstd': median standard deviation winsorize
            'pcut': percentile tails cutting winsorize
        
        Return
        ------
        None, replace self.data with winsorized value
        """

        def apply_winsorize_pcut(series):
            series[series>=np.percentile(series, 97.5)] = np.percentile(series, 97.5)
            series[series<=np.percentile(series, 2.5)] = np.percentile(series, 2.5)
            return series
        def apply_winsorize_mstd(series):
            up = np.median(series) + 3*np.std(series)
            down = np.median(series) - 3*np.std(series)
            series[series>=up] = up
            series[series<=down] = down
            return series
        
        if target_factor is None:
            target_factor = self.factor_code
        
        _grouper = ['date']
        if by_industry:
            _grouper.append('industry')
            
        for factor in target_factor:
            if method == 'mstd':
                self.data[factor] = self.data[factor].groupby(_grouper,
                         group_keys=False).apply(apply_winsorize_mstd)
            elif method == 'pcut':
                self.data[factor] = self.data[factor].groupby(_grouper,
                         group_keys=False).apply(apply_winsorize_pcut)
            
            
    def apply_mktcap_weighted_normalize(self, target_factor=None, by_industry=False):
        """
        capital weighted demean normalized
        """
        def _apply_normalized(df, factor):
            weighted_mean = np.sum(df[factor]*df['mktcap'])/np.sum(df['mktcap'])
            std = np.std(df[factor])
            return (df[factor]-weighted_mean)/std
        
        if target_factor==None:
            target_factor = self.factor_code
        _grouper = ['date']
        if by_industry:
            _grouper.append('industry')
        
        for factor in target_factor:
            self.data[factor] = self.data.groupby(_grouper, 
                     group_keys=False).apply(_apply_normalized, factor)
            
        self.apply_winsorize(target_factor=target_factor)

    def apply_simple_normalize(self, target_factor=None, by_industry=False):
        """
        capital simple normalized
        """
        def _apply_normalized(df, factor):
            mean = np.mean(df[factor])
            std = np.std(df[factor])
            return (df[factor]-mean)/std
        
        if target_factor==None:
            target_factor = self.factor_code
            
        _grouper = ['date']
        if by_industry:
            _grouper.append('industry')        
            
        for factor in target_factor:
            self.data[factor] = self.data.groupby(_grouper, 
                     group_keys=False).apply(_apply_normalized, factor)
            
        self.apply_winsorize(target_factor=target_factor)           
        
            
    def regress(self, regress_to, target_factor=None):
        """
        target factor orthogonlize to industry and size or other factors
        Use normalized zscore to regress is better
        
        Parameters:
        -----------
        regress_to: list, northogonlize to
        target_factor: list, default None
        
        Return:
        -------
        Orthogonlized multi-factors data
        """
        if target_factor is None:
            target_factor = self.factor_code
            
        def regression(df, factor, regress):
            if regress == 'industry':
                tmp_regress_to = pd.get_dummies(df['industry']).values
            else:
                tmp_regress_to = df.loc[:,regress].values
            #reg = pd.ols(x=tmp_regress_to, y=df[factor])
            reg = sm.OLS(endog=df[factor].values, 
                         exog=sm.add_constant(tmp_regress_to)).fit()
            resid = reg.resid
            return (resid-resid.mean())/resid.std()

        for factor in target_factor:
            self.data.loc[:,factor] = self.data.groupby('date', 
                         group_keys=False).apply(regression,factor=factor, 
                                         regress=regress_to)
            
        self.apply_simple_normalize(target_factor=target_factor)
        self.neutralized = True
    

    def ic(self, target_factor=None, by_group=None):
        """
        Calculate the information coefficient for target factors
        
        Parameters
        ----------
        
        Returns
        -------
        pd.DataFrame for ic times series
        """
        if target_factor is None:
            target_factor = self.factor_code
        
        _grouper = ['date']
        if by_group is not None:
            _grouper.append(by_group)
        
        _ic = []
        for factor in target_factor:
            tmp_data = self.data.dropna(subset=[factor]).copy()
            tmp_ic = tmp_data.groupby(_grouper).apply(lambda x: stats.spearmanr(a=x[factor],
                                      b=x['ret'])[0])
            _ic.append(tmp_ic)
            
        _ic = pd.concat(_ic, axis=1)
        _ic.columns = target_factor
        return _ic
    
    def quantile_return(self, target_factor=None, quantile=10, by_group=None):
        """
        Calculate the quantile return for target factors
        
        Parameters
        ----------
        target_factor
        quantile: int, default 10
        by_group: calcuate quantile return within each group
        
        Return:
        -------
        pd.DataFrame for factor return (top-bottom group) times series
        """
        if target_factor is None:
            target_factor = self.factor_code
        
        def _calc_ret(df, factor):
            q_cut = pd.qcut(df[factor],quantile,labels=False)+1
            _mean_ret = df.groupby(q_cut)['ret'].mean()
            return _mean_ret
        
        _grouper = ['date']
        if by_group is not None:
            _grouper.append(by_group)
            
        _return = []
        for factor in target_factor:
            tmp_ret = self.data.groupby(_grouper).apply(_calc_ret, factor)
            tmp_ret = tmp_ret.stack()
            tmp_ret.index.rename(['date', 'quantile'], inplace=True)
            _return.append(tmp_ret)
            
        _return = pd.concat(_return, axis=1)
        _return.columns = target_factor
        return _return
    
    def factor_return(self, target_factor=None, by_group=None):
        """
        Calculate the factor value weighted factor return
        
        Parameters
        ----------
        
        Return
        ------
        Factor value weighted factor return series
        """
        if target_factor is None:
            target_factor = self.factor_code        
        def to_weights(group):
            return group/group.abs().sum()
        
        _grouper = ['date']
        if by_group is not None:
            _grouper.append(by_group)
            
        _factor_return = []
        for factor in target_factor:
            weights = self.data.groupby(_grouper)[factor].apply(to_weights)
            weighted_returns = self.data['ret'].multiply(weights)
            _factor_return.append(weighted_returns.groupby('date').sum())
        _factor_return = pd.concat(_factor_return, axis=1)
        _factor_return.columns = target_factor
        return _factor_return        
    
    
    def factor_turnover(self, target_factor=None):
        """calculate the factor value autocorrelation
        
        Return
        ------
        factor autocorrelation time series
        """
        if target_factor is None:
            target_factor = self.factor_code
        
        #_grouper = ['date']
        _autocorr = []
        for factor in target_factor:
            #rank = self.data.groupby(_grouper)[factor].rank()
            tmp_factor = self.data[factor]
            asset_rank = tmp_factor.reset_index().pivot(index='date',
                                                  columns='asset',
                                                  values= factor)
            #asset_rank.fillna(0.0, inplace=True)
            #TODO how to deal with time series nan value?
            _autocorr.append(asset_rank.corrwith(asset_rank.shift(1),axis=1))
        _autocorr = pd.concat(_autocorr, axis=1)
        _autocorr.columns = target_factor
        return _autocorr 