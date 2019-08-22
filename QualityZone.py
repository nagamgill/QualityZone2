import pecos
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import plotly.plotly as py
import plotly.graph_objs as go
import pandas as pd
from plotly import tools
import plotly
import webbrowser
import dropbox
import config

dbx = dropbox.Dropbox(config.dropbox_api)
dbx.users_get_current_account()


# assumes index is col 0
# na_values defines ADDITIONAL na strings, not ONLY na strings
def download_master(master_path):
    print("downloading master CSV file from dropbox...")
    _, res = dbx.files_download(master_path)
    df_master=pd.read_csv(res.raw,
        index_col=0,
        )
    df_master.index = pd.to_datetime(df_master.index)
    return df_master

def download_new_data(new_path, newcols):
    print("downloading new data .dat from dropbox...")
    _, res = dbx.files_download(new_path)
    df_new=pd.read_csv(res.raw,
        header=1,
        skiprows=[2,3],
        index_col=0,
        na_values='NAN'
    )
    print("translating datalogger headers...")
    df_new.rename(columns=newcols, inplace=True)
    df_new.index = pd.to_datetime(df_new.index)
    return df_new

# this function requres the new dataframe index to begin sometime after the old dataframe index
def append_non_duplicates(a, b, col=None):
    print("Appending new data onto master dataframe...")
    if ((a is not None and type(a) is not pd.core.frame.DataFrame) or (b is not None and type(b) is not pd.core.frame.DataFrame)):
        raise ValueError('a and b must be of type pandas.core.frame.DataFrame.')
    if (a is None):
        return(b)
    if (b is None):
        return(a)
    if(col is not None):
        aind = a.iloc[:,col].values
        bind = b.iloc[:,col].values
    else:
        aind = a.index.values
        bind = b.index.values
    take_rows = list(set(bind)-set(aind))
    take_rows = [i in take_rows for i in bind]
    return(a.append( b.iloc[take_rows,:], sort=True))

# this will overwrite the previously existing file
def df_to_dropbox(dataframe, upload_path):
    df_string = dataframe.to_csv(index_label='TIMESTAMP') #na_rep='NaN')
    db_bytes = bytes(df_string, 'utf8')
    dbx.files_upload(
        f=db_bytes,
        path=upload_path,
        mode=dropbox.files.WriteMode.overwrite
    )


def qc_results_to_dropbox(qc_dir):
    print("Attempting to upload...")
    # walk return first the current folder that it walk, then tuples of dirs and files not "subdir, dirs, files"
    for dir, dirs, files in os.walk(qc_dir):
        for file in files:
            try:
                file_path = os.path.join(dir, file)
                dest_path = os.path.join(
                    '/CZO/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/Results/testing/',
                    file)
                print('Uploading %s to %s' % (file_path, dest_path))
                with open(file_path, 'rb') as f:
                    dbx.files_upload(f.read(),
                                     dest_path,
                                     mode=dropbox.files.WriteMode.overwrite,
                                     mute=True)
            except Exception as err:
                print("Failed to upload %s\n%s" % (file, err))

    print("Finished upload.")



def dbx_csv_folder_download(dbx_folder, outpath):
    entries = dbx.files_list_folder(dbx_folder).entries
    print("Downloading .csv files from dbx_folder")
    for entry in entries:
        if isinstance(entry, dropbox.files.FileMetadata) and entry.path_lower.endswith('.csv'):
            md, res = dbx.files_download(entry.path_lower)
            # 'wb' or "write binary" may cause Mac / PC compatibility issues - needs testing
            with open (os.path.join(outpath + entry.name), 'wb') as out:
                out.write(res.content)
                print("Saving .csv files to outpath")



