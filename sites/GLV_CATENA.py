"""
            ----QUALITY ZONE-------

Command line interface for collecting data from networked dataloggers, compiling it into an excel file,
running QAQC checks, and plotting. Powered by Pandas, Pecos, and Plotly.

Written by Dillon Ragar
March 27th 2018



"""

import pecos
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
from campbellsciparser import cr
import plotly.plotly as py
import plotly.graph_objs as go
import pandas as pd
from plotly import tools
import plotly

site_name = 'GLV_Catena'
index_name = 'Date plus time (0000 midnight notation)'

print('Starting QualityZone')

"""Append_Function...Don't Touch This..."""


def append_non_duplicates(a, b, col=None):
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
    return(a.append( b.iloc[take_rows,:] ))


os.chdir(
    'D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\'
    'BcCZO\\Personnel_Folders\\Dillon_Ragar\\Python_dev')

"""Load up the master .xlxs"""
df_master = pd.read_excel(
    'D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\'
    'BcCZO\\Data\\Betasso\\Betasso_Soil\\BT_Gully\\BT_Gully_WY2018\\BT_Gully_CR10x_ExcelandMeta\\'
    'BT_Gully_CR10x_Master_WY2018.xlsx', sheet_name='BT_Gully_Master_WY2018')

df_master.index = df_master[index_name]
del df_master[index_name]

print('Load Master Successful')

"""Load up the new .dat file"""

folder_path = ('D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\'
                'BcCZO\\ToughBook_Share\\Betasso\\Betasso_Soil\\BT_Gully\\data')
folder = os.listdir(folder_path)
filecount = len(folder)
if filecount > 1:
    raise Exception('QualityZone Error: There is more than one file in the Toughbook Share folder, '
                    'move old files to data folder to run QualityZone')

for file in folder:
    filepath = folder_path + "\\" + file



replacement_columns = [
    '211',
    'Year',
    'Day',
    'Hour_Minute',
    'BattVolt',
    'ProgSig',
    'EC_1_VWC',
    'EC_2_VWC',
    'EC_3_VWC',
    'WP_1_mV',
    'WP_1_kPa',
    'WP_2_mV',
    'WP_2_kPa',
    'WP_3_mV',
    'WP_3_kPa',
    'BTSD_1Tem',
    'BTSD_1Tim',
    'BTSD_1Dis',
    'BTSD_1Ret',
    'BTSD_2Tem',
    'BTSD_2Tim',
    'BTSD_2Dis',
    'BTSD_2Ret',
    'BTSD_3Tem',
    'BTSD_3Tim',
    'BTSD_3Dis',
    'BTSD_3Ret',
    'BTSD_4Tem',
    'BTSD_4Tim',
    'BTSD_4Dis',
    'BTSD_4Ret',
    'BTSD_5Tem',
    'BTSD_5Tim',
    'BTSD_5Dis',
    'BTSD_5Ret',
]


df_new = pd.read_csv(filepath, names=replacement_columns)

print('Dataframe created from download file %s'%file)

df_new['Year']+['Day']

df_new = df_new.rename(columns=replacement_columns, inplace=True)

df_new.index = df_new['TIMESTAMP']
del df_new['TIMESTAMP']

# df_new['GGL_NF_Met_Time of Wind Speed Max'] = pd.to_datetime(df['GGL_NF_Met_Time of Wind Speed Max']

newcols = {
    'RECORD':'Record Number',
    'BattVolt':'Battery Voltage, DC Volts',
    'ProgSig':'Program Signature (datalogger value)',
    'EC_1_VWC':'Volumetric Water Content at 40 cm depth',
    'EC_2_VWC':'Volumetric Water Content at 70 cm depth',
    'EC_3_VWC':'Volumetric Water Content at 110 cm depth',
    'WP_1_mV':'Water Potential at 40 cm depth (mV)',
    'WP_1_kPa':'Water Potential at 40 cm depth (kPa)',
    'WP_2_mV':'Water Potential at 70 cm depth (mV)',
    'WP_2_kPa':'Water Potential at 70 cm depth ( kPa)',
    'WP_3_mV':'Water Potential at 110 cm depth (mV)',
    'WP_3_kPa':'Water Potential at 110 cm depth (kpa)',
}

df_new.rename(columns=newcols, inplace=True)

df_new.index.names = [index_name]

df_new.index = pd.to_datetime(df_new.index)
print('Load .dat and translate columns successful')

# Create new dataframe updated with most recent data
print('Appending new data...')
updated_frame = append_non_duplicates(df_master, df_new)
print('Sorting new data frame...')
updated_frame = updated_frame.sort_index()

updated_frame.to_csv(
    'D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\'
    'BcCZO\\Personnel_Folders\\Dillon_Ragar\\QualityZone\\QualityZone_working_data.csv')

print('Create working file successful')

pecos.logger.initialize()

# Create a Pecos PerformanceMonitoring data object
pm = pecos.monitoring.PerformanceMonitoring()

# Populate the object with a dataframe and translation dictionary
system_name = site_name
data_file = ''
df = updated_frame

trans = dict(zip(df.columns, [[col] for col in df.columns]))
pm.add_translation_dictionary(trans, system_name)

pm.add_dataframe(df, system_name)

# Check timestamp
pm.check_timestamp(600)

# Check missing
pm.check_missing(min_failures=1)

# Check corrupt
pm.check_corrupt([-6999, 'NAN'])

# Check range
pm.check_range([12, 15.1], 'Battery Voltage, DC Volts')
# pm.check_range([-200, 1000], 'GGL_SF_Met Net Radiation Corrected for Windspeed W/m^2 Average')
# pm.check_range([-1, 1], 'Wave')
# pm.check_range([None, 0.25], 'Wave Absolute Error')

# Check increment
pm.check_increment([None, 0.02], 'Volumetric Water Content at 110 cm depth')
# pm.check_increment([0.0001, None], 'Random')
# pm.check_increment([0, 0.5], 'Volumetric Water Content at 70 cm depth')

# Compute metrics
mask = pm.get_test_results_mask()
QCI = pecos.metrics.qci(mask, pm.tfilter)

# Define output file names and directories
results_directory = 'D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\' \
                    'BcCZO\\Personnel_Folders\\Dillon_Ragar\\QualityZone\\Results'
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
graphics_file_rootname = os.path.join(results_directory, 'test_results')
custom_graphics_file = os.path.abspath(os.path.join(results_directory, 'custom.png'))
metrics_file = os.path.join(results_directory, system_name + '_metrics.csv')
test_results_file = os.path.join(results_directory, system_name + '_test_results.csv')
report_file = os.path.join(results_directory, system_name + '.html')

# Generate graphics
test_results_graphics = pecos.graphics.plot_test_results(graphics_file_rootname, pm)
plt.figure(figsize=(7.0, 3.5))
ax = plt.gca()
df.plot(ax=ax, ylim=[-1.5, 1.5])
plt.savefig(custom_graphics_file, format='png', dpi=500)

# Write metrics, test results, and report files
pecos.io.write_metrics(metrics_file, QCI)
pecos.io.write_test_results(test_results_file, pm.test_results)
pecos.io.write_monitoring_report(report_file, pm, test_results_graphics, QCI)
# pecos.io.write_monitoring_report(report_file, pm, test_results_graphics, [custom_graphics_file], QCI)

print('QAQC checks successful')
# -------GGL_SF_SP_6---------------------

plotly_df = updated_frame
plotly_df.reset_index(level=0, inplace=True)