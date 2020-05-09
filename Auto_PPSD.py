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


class Seismic_Noise_Analysis(object):
    
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
            
        self._info_partition()    
        self._info_by_channel()
        
    def _info_partition(self):
        
        #Devuelve un dataframe donde se da la información de la partición de cada npz
        
        starttime= self.startdate + self.ini_IH
        Endtime= self.enddate+ timedelta(hours=0)
        times_to_npz=[]
        while starttime < Endtime:
            endtime= starttime+self.delta
            times_to_npz.append([starttime, endtime])
            starttime= starttime + timedelta(days=1)
        df_times= pd.DataFrame(times_to_npz, columns=["starttime","endtime"])
        self.info_partition=df_times
        return self.info_partition
        
    def _info_by_channel(self):
        
        #Devuelve un dataframe donde se da la información del parser por canal.
    
        inv_df= pd.DataFrame(self.parser.get_inventory()["channels"]) # dataless en formato de dataframe
        if self.st_restrictions!=None:
            for restriction in self.st_restrictions:
                inv_df_bolean= inv_df["channel_id"].str.contains(restriction).rename("restriction").to_frame()
                inv_df.drop( inv_df_bolean[inv_df_bolean.restriction == True].index, inplace=True  )
            
            inv_df=inv_df.reset_index(drop=True)
        self._info_by_channel= inv_df
        return self._info_by_channel
            
    
    def _create_data_base(self):
    
        """
        It tries to create the folder in a specified directory

        :type Directory: str, optional
        :param Directory: Directory of the folder that will be created
        """
        Initial_date= self.startdate_str[0:6]
        Final_date= self.enddate_str[0:6]
        Principal_directory= os.path.join(self.principal_directory, "SNA")
        try:    os.mkdir(Principal_directory)
        except: pass
        Data_base= self.data_base
        Stations= [x for x in self._info_by_channel["channel_id"]]
        Dir_data_base= os.path.join(Principal_directory, f'{Data_base}')
        # ~ self.__dir_Database= Dir_data_base
        try:    os.mkdir(Dir_data_base)
        except Exception as e:
            ensure_database="x"
            while ensure_database!= "Y" or ensure_database!= "N":
                ensure_database=input(f"{Dir_data_base} already exist!. Do you want to use this database? (Y or N)").upper()
                if ensure_database=="Y":    break
                if ensure_database=="N":
                    msg = (f"can't create a database with name {self.data_base}\n"
                           "%s: %s \n")
                    msg = msg % (e.__class__.__name__, str(e))
                    raise TypeError(msg)
            
        for station in Stations:
            Dir_stations= os.path.join(Dir_data_base, f'{station}')
            try:    os.mkdir(Dir_stations)
            except: pass
            Dir_ppsd= os.path.join(Dir_stations, 'ppsd')
            Dir_st= os.path.join(Dir_stations, 'st')
            try:    os.mkdir(Dir_ppsd)
            except: pass
            try:    os.mkdir(Dir_st)
            except: pass
            for _dir in [Dir_ppsd,Dir_st]:
                for year in range(int(Initial_date[:4]),int(Final_date[:4])+1):
                    Dir_year= os.path.join(_dir, f'{year}')
                    try:    os.mkdir(Dir_year)
                    except FileExistsError: pass
                    if year == int(Final_date[:4]):   initial_month, final_month= 1 , int(Final_date[4:])
                    if year == int(Initial_date[:4]):   initial_month, final_month= int(Initial_date[4:]) , 12
                    
                    for month in range(initial_month,final_month+1):
                        monthf= "{:02d}".format(month)
                        Dir_month= os.path.join(Dir_year, f'{monthf}')
                        try:    os.mkdir(Dir_month)
                        except FileExistsError: pass
        return Dir_data_base
        
    def execute(self,directory,database_name):
        self.principal_directory= directory
        self.data_base=database_name
        
        self.__dir_Database= self._create_data_base()
        for No,(starttime,endtime) in enumerate (zip(self.info_partition.iloc[:,0],self.info_partition.iloc[:,1])):
            # ~ print(No,(starttime,endtime))
            self.__name_npz= No
            st_S= self._get_streams(starttime, endtime)
            ppsd_S= self._get_ppsdS(st_S)

            # ~ print(get_st_S)
            # ~ st_S = [x[0] for x in get_st_S]
            # ~ dir_st_S = [os.path.join(dir_data_base,x[1],f"st_{No}.npz") for x in get_st_S] ###REVISAR
            # ~ print(st_S)
            # ~ print(st_S_name)
            # ~ dir_st= os.path.join(dir_data_base,st_S_name)
            # ~ print(dir_st)
            
            # ~ print(ppsd_S)
            # ~ for ppsd in ppsd_S:
                # ~ if ppsd != None:
                    # ~ ppsd.save_npz(
    def _get_waveform(self, parameters): 
        
        #Devuelve la forma de onda, dado los parametros del client get_waveforms
        try:
            channel= '.'.join([parameters[0], parameters[1], parameters[2], parameters[3]]) 
            dir_ppsd_channel= os.path.join(self.__dir_Database,channel,'ppsd',str(parameters[4])[0:4],str(parameters[4])[5:7],f"{self.__name_npz}.npz")
            dir_st_channel= os.path.join(self.__dir_Database,channel,'st',str(parameters[4])[0:4],str(parameters[4])[5:7],f"{self.__name_npz}.png")
            st = self.client.get_waveforms(parameters[0], parameters[1], parameters[2], parameters[3], parameters[4], parameters[5])
            st.plot(outfile=dir_st_channel)
            return [st,dir_st_channel,dir_ppsd_channel]
            
        except Exception as e:
            msg = (f"Error getting stream from: {parameters} for the next reason:\n"
                   "%s: %s\n"
                   "Skipping this stream.")
            msg = msg % (e.__class__.__name__, str(e))
            warnings.warn(msg)
            pass
        
        
    def _get_ppsd(self, st_info):
        
        #Devuelve la ppsd del stream st dado en el parametro de la función
        
        try: 
            st= st_info[0]
            ppsd_dir= st_info[2]
            print("HOLA ", ppsd_dir)
            tr=st[0]
            ppsd = PPSD(tr.stats, metadata=self.parser, skip_on_gaps=True, overlapping=0.5)
            ppsd.add(st)
            ppsd.save_npz(ppsd_dir)
            return [ppsd,ppsd_dir]
        except Exception as e:
            msg = (f"Error getting ppsd from: {st_info[1]} for the next reason:\n"
                   "%s: %s\n"
                   "Skipping this stream.")
            msg = msg % (e.__class__.__name__, str(e))
            warnings.warn(msg)
            pass
    
    
    def _get_streams(self, starttime, endtime):
        
        #Devuelve todas las trazas de todas las estaciones de la respuesta instrumental dado en el init, excepto las que estan en la lista restricciones
        #En las restricciones funciona el formato *, por ejemplo BT.BUPI.00.* restringe todos los canales de BUPI con locID 00
        
        inv_df= self._info_by_channel
        parameters=[]
        
        for channel_id in inv_df["channel_id"]:
            parameter=channel_id.split(".")
            parameter += [starttime,endtime]
            parameters.append(parameter)

        with concurrent.futures.ProcessPoolExecutor() as executor:
            streams= executor.map(self._get_waveform,parameters)

        return list(streams)
                
    def _get_ppsdS(self,streams_info):
        
        ##Devuelve todos los psds dado una lista de strams
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            ppsdS= executor.map(self._get_ppsd,streams_info)
        # ~ return list(ppsdS)
        
        

if __name__ == '__main__':
    client_RSSB = Client("\\\\168.176.35.177\\archive")
    parser_RSSB = Parser(os.path.join(os.getcwd(), 'BT_UNALv4.dataless')) 
    SNA=Seismic_Noise_Analysis(client_RSSB, parser_RSSB, "20170101", "20200508")
    # ~ print(SNA.info_by_channel)
    DIR=os.getcwd()
    SNA.execute(DIR,"RSSBv1")
    # ~ SNA.execute()
    # ~ print(SNA.info)
    # ~ SNA.get_streams(network="BT",station="BUPI",location="00",channel="BHN")
    # ~ SNA.get_streams(UTCDateTime("20171224"), UTCDateTime("20171225"),["BT.BUPI.00.BH*","BT.ZIPA.*"])
    # ~ streams=SNA.get_streams(["BT.BUPI.00.*"])
    # ~ print(SNA.get_ppsdS(streams))
