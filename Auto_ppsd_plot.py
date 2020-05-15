# Emmanuel David Castillo Taborda - ecastillot@unal.edu.co

import glob
import os
import concurrent.futures
from obspy.imaging.cm import pqlx
from obspy.signal import PPSD


ppsd_path= 'D:\\EDCT\\SNA_RSSB\\SNA\\RSSBv1\\BT.BUPI.00.BHE\\ppsd'
dataless_path= 'D:\EDCT\SNA_RSSB\Auto_PPSD\BT_UNALv5.dataless'
database_path= 'D:\\EDCT\\SNA_RSSB\\SNA\\RSSBv1'

def get_npz_paths(ppsd_path):
    npz_paths= []
    for path, subdirs, files in os.walk(ppsd_path):
        for name in files:
            npz_paths.append(os.path.join(path, name))
    return npz_paths

# def 
def save_NPZ(ppsd_NPZ):
    NPZ_name= f'{ppsd_NPZ.network}.{ppsd_NPZ.station}.{ppsd_NPZ.location}.{ppsd_NPZ.channel}.npz'
    NPZ= ppsd_NPZ.save_npz(NPZ_name)
    print(f'in {NPZ_name} ')

def ppsd_NPZ(npz_paths):
    npz_0= PPSD.load_npz(npz_paths[0])
    print(f'{npz_0.network}.{npz_0.station}.{npz_0.location}.{npz_0.channel}')
    
    for npz in npz_paths[1:]:
        npz_0.add_npz(npz)
        # try:    npz_0.add_npz(npz)
        # except AssertionError as e: print(str(e))
    print('saving')
    save_NPZ(npz_0)
    return npz_0


def get_NPZ( directory_path, directory_type):

    if directory_type == 'database':
        channel_paths= glob.glob(os.path.join(directory_path,'*'))
        ppsd_path_generator= lambda x: os.path.join(x,'ppsd')
        ppsd_paths= list(map(ppsd_path_generator,channel_paths ))

        with concurrent.futures.ProcessPoolExecutor() as executor:
            npz_paths= list(executor.map(get_npz_paths,ppsd_paths))
            ppsd_NPZs= executor.map(ppsd_NPZ,npz_paths)
            # save_NPZs= list(executor.map(save_NPZ,ppsd_NPZs))
    return ppsd_NPZs

def plot_by_path(NPZ_path):
    NPZ= PPSD.load_npz(NPZ_path)
    NPZ_name= f'{NPZ.network}.{NPZ.station}.{NPZ.location}.{NPZ.channel}.png'
    print(f'graficando {NPZ_name}')
    NPZ.plot(NPZ_name,cmap=pqlx, show_histogram=True, show_percentiles=True, percentiles=[90,10], period_lim=(0.1, 179))
    return NPZ
        
if __name__ == '__main__':  
    # print(get_NPZ(database_path,directory_type='database' ))


    
    NPZ_path='D:\\EDCT\\SNA_RSSB\\SNA\\NPZ_ppsds'
    
    NPZ_paths= glob.glob(os.path.join(NPZ_path,'*'))
    with concurrent.futures.ProcessPoolExecutor() as executor:
        NPZ= list(executor.map(plot_by_path,NPZ_paths))


    