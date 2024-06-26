#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 14:53:37 2024

@author: janmejoyarch
"""
import os, glob
import pandas as pd
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor

def generator(ftr_name):
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
    plt.ylabel("Exp Time (ms")
    plt.legend()
    plt.grid()
    plt.xticks(rotation=30)
    if SAVE: plt.savefig(os.path.join(project_path, f"products/{ftr_name}_{year}_{month}.pdf"))
    print(f"products/{ftr_name}_{year}_{month}.pdf Saved!")
    plt.show()
    
    
if __name__=="__main__":
    ftr_list=["NB01","NB02","NB03","NB04","NB05","NB06","NB07","NB08","BB01", "BB02", "BB03", "BP02", "BP03", "BP04"]
    SAVE=True
    project_path= os.path.realpath("..")
    
    for year_dir in glob.glob(os.path.join(project_path, "data/raw/*")):
        year=os.path.basename(year_dir)
        print(year)
        for month_dir in glob.glob(os.path.join(year_dir,"*")):
            if len(os.path.basename(month_dir)) < 3:
                month= os.path.basename(month_dir) 
            print(year, month)
            files= glob.glob(os.path.join(month_dir, "*/*.xlsx"))
            with ProcessPoolExecutor() as executor:
                executor.map(generator, ftr_list)

