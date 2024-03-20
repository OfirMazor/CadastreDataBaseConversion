import pyodbc
import numpy as np
import pandas as pd
import datetime as dt
import geopandas as gpd



def timepost():
    '''Prints the current time'''
    current_time = str(dt.datetime.now().strftime("%H:%M:%S"))
    print(current_time)

    
def timestamp():
    '''Returns the current time'''
    current_time = str(dt.datetime.now().strftime("%H:%M:%S"))
    
    return current_time

    
def time_difference(t1, t2 = timestamp()):
    '''Returns time delta in minutes'''
    t1 = dt.datetime.strptime(t1, "%H:%M:%S")
    t2 = dt.datetime.strptime(t2, "%H:%M:%S")
    time_difference = round(((t2 - t1).total_seconds())/60, 3)
    
    return time_difference


def read_sde(query : str, Server: str , Database: str , Username: str , Password: str):
    '''
    Return DataFrame of selected data from MNCDB  database.
    query example: 
            'SELECT Field1, Field2, Field3 FROM PF.CadasterProcessBorders'
    '''
    conn = pyodbc.connect(f'DRIVER=SQL Server;' +\
                                           f'SERVER={Server};' +\
                                           f'DATABASE={Database};' +\
                                           f'UID={Username};' +\
                                           f'PWD={Password}')
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df
         
                
def assign_ProcessName(df : pd.DataFrame|gpd.GeoDataFrame, num : str, year : str, drop : bool = True):
    ''' Assign ProcessName column to the given DataFrame.  '''
    
    df = df[(df[num] != 0) | (df[num].notna()) | (df[year] != 0) | (df[year].notna())]
    df[[num, year]] = df[[num, year]].astype(int)
    df['ProcessName'] = df[num].astype(str) + '/' + df[year].astype(str)
    
    if drop: df = df.drop(columns = [num, year])
    else:  pass
   
    return df


def assign_BlockName(df : pd.DataFrame|gpd.GeoDataFrame,
                                     BlockNumber : str,
                                     SubBlockNumber : str,
                                     drop : bool = True):
    
    '''Assign BlockName column to the given DataFrame.'''
    
    df[BlockNumber] = np.where((df[BlockNumber].isna()) | (df[BlockNumber] == 0), None, df[BlockNumber] )
    
    df[SubBlockNumber] = df[SubBlockNumber].fillna(0)
    
    df[[BlockNumber, SubBlockNumber]] = df[[BlockNumber, SubBlockNumber]].astype('Int64')
    df['BlockName'] = df[BlockNumber].astype(str) + '/' + df[SubBlockNumber].astype(str)
    df['BlockName']  = np.where(df['BlockName'].str.startswith("<"), None, df['BlockName'] )
    
    if drop: df = df.drop(columns = [BlockNumber, SubBlockNumber])
    else:  pass
    
    return df


def assign_ParcelName(df : pd.DataFrame|gpd.GeoDataFrame,
                                       ParcelNumber : str, 
                                       BlockNumber : str,
                                       SubBlockNumber : str,
                                       drop : bool = True):
    
    ''' Assign BlockName column to the given DataFrame. '''
    
    df = assign_BlockName(df, BlockNumber , SubBlockNumber, drop)
    df['ParcelName'] = df[ParcelNumber].astype(str) + '/' + df['BlockName']
    
    return df


def drop_bad_ProcessNames(df : pd.DataFrame|gpd.GeoDataFrame):
    '''Drop rows where the process name contains invalid or missing values '''
    
    df = df[(df['TALAR_NUM']  != 0) | (df['TALAR_YEAR'] != 0)]
    df = df[(df['TALAR_NUM'].notna()) | (df['TALAR_YEAR'].notna())]
    df = df.sort_values(['TALAR_NUM', 'TALAR_YEAR'])
    df = df.drop_duplicates(keep = 'last')
    
    
    return df



def handleRemoveReadonly(func, path, exc):
    '''
     Providing acces to files that are set to Read Only mode
     '''
    excvalue = exc[1]
    if func in (os.rmdir, os.remove) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)
        func(path)
    else: raise 