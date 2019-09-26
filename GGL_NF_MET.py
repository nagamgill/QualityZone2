import QualityZone
import os
from io import StringIO
import tempfile
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

system_name = 'GGL_NF_MET'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_NF_Met/GGL_NF_Met_ExcelandMeta/GGL_NF_Met_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_NF_Met_Raw/GGL_NF_MET_CR1000_GGL_NF_Met.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_NF_Met/GGL_NF_Met_ExcelandMeta/GGL_NF_Met_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'RECORD': 'GGL_NF_Met_Record Number',
    'BattV_GGL_NF_Met_Avg': 'GGL_NF_Met_Battery Voltage Average',
    'Rain_mm_GGL_NF_Met_Tot': 'GGL_NF_Met_tipping bucket rain gage, total, mm',
    'BP_mbar_GGL_NF_Met_Avg': 'GGL_NF_Met_BP_Mbar',
    'VW_22cm_GGL_NF_Met_Avg': 'GGL_NF_Met_Volumetric Water Content, Average fractional 22 cm',
    'PA_uS_22cm_GGL_NF_Met_Avg': 'GGL_NF_Met_Volumetric Water Content, PA_uS_Avg 22 cm',
    'SoilT_22cm_T107_C_GGL_NF_Met_Avg': 'GGL_NF_Met_Soil Temperature Average 22 cm ',
    'WS_ms_250cm_GGL_NF_Met_Avg':'GGL_NF_Met_Wind Speed Average m/sec',
    'WS_ms_250cm_GGL_NF_Met_Max':'GGL_NF_Met_Wind Speed Max m/sec',
    'WS_ms_250cm_GGL_NF_Met_TMx':'GGL_NF_Met_Time of Wind Speed Max',
    'WS_ms_250cm_GGL_NF_Met_Min':'GGL_NF_Met_WindSpeed Min m/sec',
    'NR_Wm2_250cm_GGL_NF_Met_Avg':'GGL_NF_Met_Net Radiation W/m^2 Average',
    'CNR_Wm2_250cm_GGL_NF_Met_Avg':'GGL_NF_Met_Net Radiation Corrected for Windspeed W/m^2 Average',
    'SlrW_250cm_GGL_NF_Met_Avg':'GGL_NF_Met_Incoming Shortwave Radiation W/m^2',
    'SlrMJ_250cm_GGL_NF_Met_Tot':'GGL_NF_Met_Incoming Shortwave Radiation MJ/m^2, Total',
    'AirTC_250cm_GGL_NF_Met_Avg':'GGL_NF_Met_Air Temperature Average, Degrees C 2.6 meters',
    'AirTC_250cm_GGL_NF_Met_Min':'AirTC_250cm_GGL_NF_Met_Min',
    'AirTC_250cm_GGL_NF_Met_Max':'AirTC_250cm_GGL_NF_Met_Max',
    'RH_GGL_NF_Met_Max':'RH_GGL_NF_Met_Max',
    'RH_GGL_NF_Met_Min':'RH_GGL_NF_Met_Min',
    'RH_GGL_NF_Met':'RH_GGL_NF_Met'
}

def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'GGL_NF_Met_Record Number',
        'GGL_NF_Met_Battery Voltage Average',
        'GGL_NF_Met_Volumetric Water Content, PA_uS_Avg 22 cm',
        'AirTC_250cm_GGL_NF_Met_Min',
        'AirTC_250cm_GGL_NF_Met_Max'
        ], axis=1)


    dist_columns = {
        'GGL_NF_Met_tipping bucket rain gage, total, mm':'RAIN GAGE(MM)',
        'GGL_NF_Met_BP_Mbar':'BAROMETRIC PRESS(MBAR)',
        'GGL_NF_Met_Volumetric Water Content, Average fractional 22 cm':'SOIL VOL WATER CONTENT(%)-22CM',
        'GGL_NF_Met_Soil Temperature Average 22 cm ':'SOIL TEMP(C)-22CM',
        'GGL_NF_Met_Wind Speed Average m/sec':'WINDSPEED(m/s)-2.5M(AVG)',
        'GGL_NF_Met_Wind Speed Max m/sec':'WINDSPEED(m/s)-2.5M(MAX)',
        'GGL_NF_Met_Time of Wind Speed Max':'WINDSPEED-2.5M(MAX) TIME',
        'GGL_NF_Met_WindSpeed Min m/sec':'WINDSPEED(m/s)-2.5M(MIN)',
        'GGL_NF_Met_Net Radiation W/m^2 Average':'NET RAD(W/m^2)-2.5M',
        'GGL_NF_Met_Net Radiation Corrected for Windspeed W/m^2 Average':'NET RAD CORR(W/m^2)-2.5M',
        'GGL_NF_Met_Incoming Shortwave Radiation W/m^2':'IN SW RAD(W/m^2)-2.5M(AVG)',
        'GGL_NF_Met_Incoming Shortwave Radiation MJ/m^2, Total':'IN SW RAD(MJ/m^2)-2.5M(TOTAL)',
        'GGL_NF_Met_Air Temperature Average, Degrees C 2.6 meters':'AIRTEMP(C)-2.5M(AVG)',
        'RH_GGL_NF_Met_Max':'RH (MAX)',
        'RH_GGL_NF_Met_Min':'RH (MIN)',
        'RH_GGL_NF_Met':'RH (%)'
    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    #dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)
    dfd.iloc[:, 8] = dfd.iloc[:, 8].round(3)
    dfd.iloc[:, 9] = dfd.iloc[:, 9].round(3)
    dfd.iloc[:, 10] = dfd.iloc[:, 10].round(3)
    dfd.iloc[:, 11] = dfd.iloc[:, 11].round(3)
    dfd.iloc[:, 12] = dfd.iloc[:, 12].round(3)
    dfd.iloc[:, 13] = dfd.iloc[:, 13].round(3)
    dfd.iloc[:, 14] = dfd.iloc[:, 14].round(3)
    dfd.iloc[:, 15] = dfd.iloc[:, 15].round(3)

    dfd = dfd.fillna('')
    return dfd

df_master = QualityZone.download_master(master_path)
df_new = QualityZone.download_new_data(new_path, newcols)
df_updated = QualityZone.append_non_duplicates(df_master, df_new)

working_file_path = os.path.join(dropbox_base + '/Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv')
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=1)
pm.check_corrupt([-7999, 'NAN'])
pm.check_range([12, 15.1], 'GGL_NF_Met_Battery Voltage Average')

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



plotly_df = df_updated.copy()

# Radiation
trace1 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Incoming Shortwave Radiation W/m^2'],
    mode='lines',
    name='Shortwave Radiation w/m^2',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Net Radiation Corrected for Windspeed W/m^2 Average'],
    mode='lines',
    name='Corrected Net Radiation w/m^2',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Net Radiation W/m^2 Average'],
    mode='lines',
    name='NR Average w/m^2',
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Air Temperature Average, Degrees C 2.6 meters'],
    mode='lines',
    name='Air T Deg C',
)
# ------------Wind--Battv---------------------------
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Wind Speed Max m/sec'],
    mode='lines',
    name='Wind Speed Max',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Wind Speed Average m/sec'],
    mode='lines',
    name='Wind Speed Avg',
)

trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_WindSpeed Min m/sec'],
    mode='lines',
    name='Wind Speed Min',
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Battery Voltage Average'],
    mode='lines',
    name='Batt V DC',
)
# ------------Soil--TIPPING----BP-----------------------
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_tipping bucket rain gage, total, mm'],
    mode='lines',
    name='Precipitation (mm)',
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['RH_GGL_NF_Met'],
    mode='lines',
    name='RH %',
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_BP_Mbar'],
    mode='lines',
    name='BP (mb)',
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Volumetric Water Content, Average fractional 22 cm'],
    mode='lines',
    name='22cm soil VWC',
)
trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_NF_Met_Soil Temperature Average 22 cm '],
    mode='lines',
    name='22cm soil T',
)

# -------------------------------------------------------------------------
fig = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 2, 1)
fig.append_trace(trace3, 2, 1)
fig.append_trace(trace4, 3, 1)

# axis titles
fig['layout']['yaxis1'].update(title='W/M^2')
fig['layout']['yaxis2'].update(title='W/M^2')
fig['layout']['yaxis3'].update(title='Deg C')
# Plot Title
fig['layout'].update(title='GGL_NF_Met_Rad_AirT')


plot_url = py.plot(fig, filename='GGL_NF_Met_Rad_AirT')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_Met\\GGL_NF_Met_PythonScripts\\1.html')
# -----------------------------------------------------------------------
fig2 = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                           shared_xaxes=True,
                           vertical_spacing=0.001)
fig2.append_trace(trace5, 1, 1)
fig2.append_trace(trace6, 1, 1)
fig2.append_trace(trace7, 1, 1)
fig2.append_trace(trace8, 2, 1)

# axis titles
fig2['layout']['yaxis1'].update(title='M/S')
fig2['layout']['yaxis2'].update(title='Volts DC')
# Plot Title
fig2['layout'].update(title='GGL_NF_Met_Wind_Batt')

plot_url = py.plot(fig2, filename='GGL_NF_Met_Wind_Batt')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_Met\\GGL_NF_Met_PythonScripts\\2.html')
# --------------------------------------------------------------------------
fig3 = tools.make_subplots(rows=5, cols=1, specs=[[{}], [{}], [{}], [{}], [{}]],
                           shared_xaxes=True,
                           vertical_spacing=0.001)

fig3.append_trace(trace9, 1, 1)
fig3.append_trace(trace10, 2, 1)
fig3.append_trace(trace11, 3, 1)
fig3.append_trace(trace12, 4, 1)
fig3.append_trace(trace13, 5, 1)
# axis titles
fig3['layout']['yaxis1'].update(title='mm')
fig3['layout']['yaxis2'].update(title='%')
fig3['layout']['yaxis3'].update(title='mB')
fig3['layout']['yaxis4'].update(title='VWC')
fig3['layout']['yaxis5'].update(title='Deg C')
# Plot Title
fig3['layout'].update(title='GGL_NF_Met_Precip_Soil_')

#plotly.offline.plot(fig3, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_Met\\GGL_NF_Met_PythonScripts\\3.html')
plot_url = py.plot(fig3, filename='GGL_NF_Met_Precip_Soil')



if click.confirm('Did you make changes to the data via the "QualityZone_working_data.csv" file?', default=False):
    df_updated = QualityZone.download_master(working_file_path)
    #print(updated_frame.dtypes)
    print('dataframe replaced with updated data from Quality_Zone_working_data.csv')

if click.confirm('Save updated dataset to master .csv?', default=False):
    QualityZone.df_to_dropbox(df_updated, master_path)
    print("Master .csv uploaded to dropbox")

if click.confirm('Save updated dataset to distribute .csv?', default=False):
    dist_df = format_for_dist(df_updated)
    QualityZone.df_to_dropbox(dist_df, distribute_path)
    print("distribute .csv formatted for distribution and uploaded to dropbox")

