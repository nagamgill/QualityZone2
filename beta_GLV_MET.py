import QualityZone
import os
import pandas as pd
from io import StringIO
import tempfile
import glob
import pecos
import matplotlib as plt
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
from plotly import tools
import webbrowser
import click
import shutil



print("Starting QualityZone")
print("Checking Dropbox API")
print(QualityZone.dbx.users_get_current_account())

system_name = 'GLV_MET'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/GordonGulch'
distribute_file = '/Data/GordonG'
raw_folder = os.path.join(dropbox_base + '/ToughBook_Share/GLV/GL4_Met/raw_data/')
master_path = os.path.join(dropbox_base + master_file)
#new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)
local_raw = tempfile.mkdtemp()


QualityZone.dbx_dat_folder_download(raw_folder, '/Users/dillon/Downloads/dbx_download_test/')


def concat_dat(dat_path):
    df = pd.concat([pd.read_csv(f, skiprows=13, parse_dates=[['Date', 'Time']]) for f in glob.glob(os.path.join(raw_folder + '*.dat'))],
                   ignore_index=True)
    df.drop_duplicates(inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)
    return df


concat_dat('/Users/dillon/Downloads/dbx_download_test/')
