#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  1 19:30:03 2021

@author: wgout
"""

import tkinter as tk
from tkinter import filedialog
import zipfile
import pandas as pd
import datetime, re
import pandas_bokeh
#pd.options.plotting.backend = "plotly"

#pd.set_option('plotting.backend', 'pandas_bokeh')

def readData():
    wnd = tk.Tk()
    wnd.withdraw()
    archive =  filedialog.askopenfilename(title = "Select VAERS archive",
                                          filetypes=[("archives", "*.zip")])
    data = symptoms = vax = None
    zip_ref = zipfile.ZipFile(archive, 'r') 
    files = zip_ref.namelist()
    assert(3 == len(files))
    for file in files:
        if file.endswith('VAERSDATA.csv'):
            data = pd.read_csv(zip_ref.open(file), encoding='Windows-1252', low_memory=False)
            data.index.name = 'idx'
        elif file.endswith('VAERSSYMPTOMS.csv'):
            symptoms = pd.read_csv(zip_ref.open(file), encoding='Windows-1252', low_memory=False)
            symptoms.index.name = 'idx'
        elif file.endswith('VAERSVAX.csv'):
            vax = pd.read_csv(zip_ref.open(file), encoding='Windows-1252', low_memory=False)
            vax.index.name = 'idx'

    #df = pd.merge(data, symptoms, on='VAERS_ID', how='outer')
    #df = pd.merge(df, vax, on='VAERS_ID', how='outer')
    return data, symptoms, vax

def deaths(data, symptoms, vax):
    deaths = (data[data['DIED'] == 'Y']).copy() # filter entries to contain only deaths
    for col in list(filter(lambda s: re.search("DATE", s), deaths.columns.to_list())):
        # convert data type of date columns 
        deaths.loc[:, col] = pd.to_datetime(deaths.loc[:, col], format="%m/%d/%Y", errors='coerce')
    
    unknown_deathdate = deaths[deaths.DATEDIED.isnull()]
    dates_need_fix = False
    if 0 < unknown_deathdate.shape[0]:
        dates_need_fix = True    
        print(f"{unknown_deathdate.shape[0]} deaths with issues in date of death found.")
        # filling missing date of death by date form completed at VAERS
        deaths.at[unknown_deathdate.index, 'DATEDIED'] = unknown_deathdate['TODAYS_DATE']
        unknown_deathdate = deaths[deaths.DATEDIED.isnull()]

    if 0 < unknown_deathdate.shape[0]:
        # filling missing date of death by vaccinatin date
        deaths.at[unknown_deathdate.index, 'DATEDIED'] = unknown_deathdate['VAX_DATE']
        unknown_deathdate = deaths[deaths.DATEDIED.isnull()]
    
    if 0 < unknown_deathdate.shape[0]:
        # filling missing date of death by date report was received at VAERS
        deaths.at[unknown_deathdate.index, 'DATEDIED'] = unknown_deathdate['RECVDATE']
        unknown_deathdate = deaths[deaths.DATEDIED.isnull()]
        
    if 0 < unknown_deathdate.shape[0]:
        print(f"{unknown_deathdate.shape[0]} deaths in data which cannot be mapped to a date")
    else:
        if dates_need_fix:
            print("all deaths contain valid death dates now")
    
    wrong_year = deaths.loc[deaths.DATEDIED.dt.year != 2021, 'DATEDIED']
    deaths.loc[wrong_year.index, 'DATEDIED'] = pd.to_datetime(wrong_year.dt.strftime("2021-%m-%d"),
                                                              format="%Y-%m-%d")
    deaths=pd.merge(deaths, pd.get_dummies(deaths, columns=["SEX"]), how='inner')
    
    #deaths = deaths.set_index('DATEDIED', append=True) 
    print(deaths.pivot_table(index=['DATEDIED'], aggfunc={
                                                    'SEX_F': 'sum',
                                                    'SEX_M': 'sum',
                                                    'SEX_U': 'sum',
                                                    'VAERS_ID':'count'}).rename(
                                                        columns={'SEX_F':'females',
                                                                 'SEX_M':'males',
                                                                 'SEX_U':'unknown sex',
                                                                 'VAERS_ID':'deaths'}))
                                                        
    merged = pd.merge(deaths, vax[['VAERS_ID', 'VAX_NAME']], on='VAERS_ID', how='inner')

    vax_count_tbl = merged.pivot_table(index=['VAERS_ID'], 
                                       aggfunc={'VAX_NAME':'count'}
                                       ).rename(columns={
                                           'VAERS_ID':'VAERS_ID',
                                           'VAX_NAME': 'vaccine_count'})
    vax_new = vax.set_index('VAERS_ID')
    vax_data = vax_new.loc[vax_count_tbl[vax_count_tbl['vaccine_count'] == 1].index, ['VAX_TYPE', 'VAX_NAME']].copy()
    vax_data['vaccine_count'] = 1
    vax_data['VAERS_ID'] = vax_data.index
    vax_data.index.name='index'
    vax_data['new_index'] = [i for i in range(vax_data.shape[0])]
    vax_data = vax_data.set_index('new_index')

    for vaers_id in vax_count_tbl[vax_count_tbl['vaccine_count'] > 1].index:
        vaccine_names = '; '.join(vax_new.loc[vaers_id].VAX_NAME.values)
        count         = vax_count_tbl.loc[vaers_id].vaccine_count
        vax_type      = '; '.join(vax_new.loc[vaers_id, 'VAX_TYPE'])
        vax_data = vax_data.append({'VAX_TYPE' : vax_type,
                                    'VAX_NAME' : vaccine_names,
                                    'vaccine_count' : count,
                                    'VAERS_ID' : vaers_id}, 
                                   ignore_index=True)
    vax_data = vax_data.set_index('VAERS_ID')
    
    deaths = pd.merge(deaths, vax_data, on='VAERS_ID', how='inner')
    deaths.index.name = 'index'
    deaths.to_csv('vaccination_deaths.csv', sep=';')

    return deaths

def main():
    data, symptoms, vax = readData()
    df = deaths(data, symptoms, vax) 
    return df

#%%
if __name__ == "main":
    deaths = main()


