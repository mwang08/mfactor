# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 13:11:32 2017

@author: wangming

Methods in Alpha Model

"""

import pandas as pd
import numpy as np
import alphalens as al
from scipy import stats
import statsmodels.api as sm


def max_icir_weight(ic_ts):
    """Calculate the factor weight (Max ICIR)
    
    Parameters
    ----------
    ic_ts, factors ic time series DataFrame
    
    Return
    ------
    factor weight, pd.Series
    """
    cols = ic_ts.columns.tolist()
    cov = ic_ts.cov()
    weight = np.dot(np.linalg.inv(cov), ic_ts.mean())
    return pd.Series(weight/np.sum(np.abs(weight)), index=cols)