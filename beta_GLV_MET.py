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

# need to write some kind of tempfile cleanup
local_raw = tempfile.mkdtemp()


QualityZone.dbx_dat_folder_download(raw_folder, local_raw)


def concat_dat(dat_path):
    df = pd.concat([pd.read_csv(
        f,
        header=1,
        skiprows=[2,3],
        index_col=0,
        na_values='NAN')
        for f in glob.glob(os.path.join(local_raw + '/*.dat'))], sort=True)
    df.drop_duplicates(inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)
    return df


df = concat_dat('/Users/dillon/Downloads/dbx_download_test/')
