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
#print(QualityZone.dbx.users_get_current_account())

system_name = 'GL4_MET'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/GLV/GL4_Met/GL4_Met_Master_Meta/Gl4_Met_Master_WY2019.csv'
distribute_file = '/Data/GordonG'
raw_folder = os.path.join(dropbox_base + '/ToughBook_Share/GLV/GL4_Met/raw_data/')
master_path = os.path.join(dropbox_base + master_file)
#new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

# need to write some kind of tempfile cleanup function
local_raw = tempfile.mkdtemp()


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'ArrayID_TenMinute',
        'Batt_Volt_Min',
        'PTemp_C',
        'RECORD',
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        'GGL_SF_Met tipping bucket rain gage, total, mm': "RAIN GAGE(MM)",
        'GGL_SF_Met BP_mbar_Avg': "BAROMETRIC PRESS(MBAR)",
        'GGL_SF_Met Volumetric Water Content, Average fractional 22 cm': "SOIL VOL WATER CONTENT(%)-22CM",
        'GGL_SF_Met Soil Temperature Average 22 cm': "SOIL TEMP(C)-22CM",
        'GGL_SF_Met Wind Speed Average m/sec': "WINDSPEED(m/s)-2.5M(AVG)",
        'GGL_SF_Met  Wind Speed Max': "WINDSPEED(m/s)-2.5M(MAX)",
        'GGL_SF_Met  Time of Wind Speed Max': "WINDSPEED-2.5M(MAX) TIME DATE MM/DD/YYYY HH24:MI",
        'GGL_SF_Met Wind Speed m/sec Minimum': "WINDSPEED(m/s)-2.5M(MIN)",
        'GGL_SF_Met Net Radiation W/m^2 Average': "NET RAD(W/m^2)-2.5M",
        'GGL_SF_Met Net Radiation Corrected for Windspeed W/m^2 Average': "NET RAD CORR(W/m^2)-2.5M",
        'GGL_SF_Met Incoming Shortwave Radiation W/m^2': "IN SW RAD(W/m^2)-2.5M(AVG)",
        'GGL_SF_Met Incoming Shortwave Radiation MJ/m^2': "IN SW RAD(MJ/m^2)-2.5M(TOTAL)",
        'GGL_SF_Met Air Temperature Average, Degrees C 2.6 meters': "AIRTEMP(C)-2.5M(AVG)",
        'RH (%)': "RH (%)",
        'RH_Max': "RH (MAX)",
        'RH_TMx': "RH (MAX) TIME DATE MM/DD/YYYY HH24:MI",
        'RH_Min': "RH (MIN)",
        'RH_TMn': "RH (MIN) TIME DATE MM/DD/YYYY HH24:MI",


    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    #dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)
    dfd.iloc[:, 8] = dfd.iloc[:, 8].round(3)
    dfd.iloc[:, 9] = dfd.iloc[:, 9].round(3)
    dfd.iloc[:, 10] = dfd.iloc[:, 10].round(3)
    dfd.iloc[:, 11] = dfd.iloc[:, 11].round(3)
    dfd.iloc[:, 12] = dfd.iloc[:, 12].round(3)
    dfd.iloc[:, 13] = dfd.iloc[:, 13].round(3)
    dfd.iloc[:, 14] = dfd.iloc[:, 14].round(3)
    #dfd.iloc[:, 15] = dfd.iloc[:, 15].round(3)
    dfd.iloc[:, 16] = dfd.iloc[:, 16].round(3)
    #dfd.iloc[:, 17] = dfd.iloc[:, 17].round(3)
    dfd.iloc[:, 18] = dfd.iloc[:, 18].round(3)
    dfd.iloc[:, 19] = dfd.iloc[:, 19].round(3)


    return dfd




df_master = QualityZone.download_master(master_path)
QualityZone.dbx_dat_folder_download(raw_folder, local_raw)
df_new = QualityZone.concat_dat(local_raw)

df_updated = QualityZone.append_non_duplicates(df_master, df_new)

# working file access
working_file_path = os.path.join(dropbox_base + '/Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv')
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=1)
pm.check_corrupt([-7999, 'NAN'])
pm.check_range([12, 15.1], 'Batt_Volt_Min')


mask = pm.get_test_results_mask()
QCI = pecos.metrics.qci(mask, pm.tfilter)


dirpath = tempfile.mkdtemp()

results_directory = dirpath
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
graphics_file_rootname = os.path.join(results_directory, 'test_results')
custom_graphics_file = os.path.abspath(os.path.join(results_directory, 'custom.png'))
metrics_file = os.path.join(results_directory, system_name + '_metrics.csv')
test_results_file = os.path.join(results_directory, system_name + '_test_results.csv')
report_file =  os.path.join(results_directory, system_name + '.html')

# Generate graphics
test_results_graphics = pecos.graphics.plot_test_results(graphics_file_rootname, pm)
plt.figure(figsize = (7.0,3.5))
ax = plt.gca()
df.plot(ax=ax, ylim=[-1.5,1.5])
plt.savefig(custom_graphics_file, format='png', dpi=500)

# Write metrics, test results, and report files
pecos.io.write_metrics(metrics_file, QCI)
pecos.io.write_test_results(test_results_file, pm.test_results)
pecos.io.write_monitoring_report(report_file, pm, test_results_graphics, QCI)
#pecos.io.write_monitoring_report(report_file, pm, test_results_graphics, [custom_graphics_file], QCI)
QualityZone.qc_results_to_dropbox(dirpath)
url = os.path.join(dirpath + '/' + system_name + '.html')
webbrowser.open('file://' + url, new=2)


