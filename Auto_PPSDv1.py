# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 09:09:09 2020

@author: EMMANUEL DAVID CASTILLO TABORDA
"""

import os
import warnings
import datetime
import pandas as pd
import numpy as np
import concurrent.futures
import matplotlib.pyplot as plt
from obspy.clients.filesystem.sds import Client
from datetime import timedelta 
from obspy import UTCDateTime
from obspy.io.xseed import Parser
from obspy.signal import PPSD

class Auto_PPSD(object):
    
    def __init__(self,client, parser, startdate, enddate, st_restrictions=None, fix_interval_hour=False, initial_hour="130000" , delta_hours=5):
        
        #Inicializamos la clase con: cliente, dataless, tiempo de inicio y tiempo final
             
        self.client= client
        self.parser = parser
        self.startdate_str= startdate
        self.enddate_str= enddate
        self.st_restrictions= st_restrictions
        
        if len(startdate)==8 and len(enddate)==8:
            try:
                self.startdate= UTCDateTime(startdate)
                self.enddate= UTCDateTime(enddate)
            except Exception as e:
                    msg = (f"can't convert startdate or enddate in UTCDateTime. Check these dates. {startdate} , {enddate} \n"
                           "%s: %s \n")
                    msg = msg % (e.__class__.__name__, str(e))
                    raise TypeError(msg)
            if self.enddate < self.startdate:
                msg = (f"enddate can't be less than startdate. Check these dates. {startdate} , {enddate} \n")
                raise TypeError(msg)
        else:
            msg= f"The length of the startime or enddate strings must be 8, corresponding to YYYYMMDD. Check these dates {startdate} , {enddate} "    
            raise TypeError(msg)
        
        if fix_interval_hour==True: 

            if len(initial_hour)==6:
                self.ini_IH= timedelta(hours=int(initial_hour[0:2]),minutes=int(initial_hour[2:4]),seconds=int(initial_hour[4:6]))
            else:
                msg = (f"The length of the initial_hour string must be 6, corresponding to hhmmss. Check this time {initial_hour} ")
                raise TypeError(msg)
            
            if isinstance(delta_hours, int):   
                if delta_hours<= 23:
                    self.delta= timedelta(hours=delta_hours)
                else:
                    msg = ("The length of the delta_hours  must be less than 24")
                    raise TypeError(msg)
            else:
                msg = ("delta_hours must be an integer")
                raise TypeError(msg)
        else:
            self.ini_IH= timedelta(hours=0,minutes=0,seconds=0)
            self.delta= timedelta(hours=24)

    @property
    def info_partition(self):
        
        #Devuelve un dataframe donde se da la información de la partición de cada npz
        
        starttime= self.startdate + self.ini_IH
        Endtime= self.enddate+ timedelta(hours=0)
        times_to_npz=[]
        while starttime < Endtime:
            endtime= starttime+self.delta
            times_to_npz.append([starttime, endtime])
            starttime= starttime + timedelta(days=1)
        df_times= pd.DataFrame(times_to_npz, columns=["starttime","endtime"])
        info_partition=df_times
        return info_partition

    @property
    def info_inventory(self):
        info_inv= pd.DataFrame(self.parser.get_inventory()["channels"]) # dataless en formato de dataframe
        return info_inv

    @property
    def info_by_channel(self):
        
        #Devuelve un dataframe donde se da la información del parser por canal.
    
        inv_df= self.info_inventory
        if self.st_restrictions != None:
            for restriction in self.st_restrictions:
                inv_df_bolean= inv_df["channel_id"].str.contains(restriction).rename("restriction").to_frame()
                inv_df.drop( inv_df_bolean[inv_df_bolean.restriction == True].index, inplace=True  )
            
            inv_df=inv_df.reset_index(drop=True)
        info_by_channel= inv_df
        return info_by_channel

if __name__ == '__main__':
    client_RSSB = Client("\\\\168.176.35.177\\archive")
    parser_RSSB = Parser('D:\EDCT\SNA_RSSB\Auto_PPSD\BT_UNALv5.dataless') 
    SNA= Auto_PPSD(client_RSSB, parser_RSSB, "20170101", "20200508")
    print(SNA.info_by_channel)
    