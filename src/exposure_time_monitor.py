#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 14:53:37 2024
-Created to generate monthly plots for CMD_EXPT and MEAS_EXP for all filters.
-Discrepany was seen in the data for few days. So this was created to see the
history.
@author: janmejoyarch
"""
import os, glob
import pandas as pd
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor

def generator(ftr_name):
    '''
    Takes Filter Name as input.
    Gives Plots as output.
    'file' is the excel file fed by global variable.
    Plots CMD_EXPT and MEAS_EXP for a month for the mentioned filter.
    '''
    print(f"Running for {ftr_name}\t")
    cmd_expt_all, meas_exp_all, time_all = pd.Series(), pd.Series(), pd.Series()
    for file in files:
        try:
            df= pd.read_excel(file)
            cmd_expt= df.loc[df['FTR_NAME']==ftr_name, 'CMD_EXPT']
            meas_exp= df.loc[df['FTR_NAME']==ftr_name, 'MEAS_EXP']
            time= pd.to_datetime(df.loc[df['FTR_NAME']==ftr_name,'DHOBT_DT'])
            cmd_expt_all= pd.concat([cmd_expt_all, cmd_expt], ignore_index=True)
            meas_exp_all= pd.concat([meas_exp_all, meas_exp], ignore_index=True)
            time_all= pd.concat([time_all, time], ignore_index=True)
        except KeyError:
            print("Skipped", file)
    #plotting
    plt.figure()
    plt.plot(time_all, cmd_expt_all, '.', label="CMD_EXPT")
    plt.plot(time_all, meas_exp_all, '.', label="MEAS_EXPT")
    plt.title(f"Filter: {ftr_name} | Date: {year}_{month}")
    plt.yscale("log")
    plt.ylabel("Exp Time (ms)")
    plt.legend()
    plt.grid()
    plt.xticks(rotation=30)
    if SAVE: plt.savefig(os.path.join(project_path, f"products/{ftr_name}_{year}_{month}.pdf"))
    print(f"products/{ftr_name}_{year}_{month}.pdf Saved!")
    plt.show()
    
if __name__=="__main__":
    ftr_list=["NB01","NB02","NB03","NB04","NB05","NB06","NB07","NB08","BB01", "BB02", "BB03"]
    SAVE=True
    project_path= os.path.realpath("..")
    for year_dir in glob.glob(os.path.join(project_path, "data/raw/*")):
    #List of years in the folder.
        year=os.path.basename(year_dir)
        print(year)
        for month_dir in glob.glob(os.path.join(year_dir,"*")):
        #List of months in a year's directory.
            if len(os.path.basename(month_dir)) < 3: 
            #To pick folders only.
            #erroneous excel files to be excluded
                month= os.path.basename(month_dir) 
            print(year, month)
            files= glob.glob(os.path.join(month_dir, "*/*.xlsx")) #list xlsx files
            with ProcessPoolExecutor() as executor: 
            #One processes runs for one filter for one month data.
                executor.map(generator, ftr_list)

