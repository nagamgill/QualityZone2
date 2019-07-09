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

system_name = 'GGL_SF_MET'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_SF_Met/GGL_SF_Met_ExcelandMeta/GGL_SF_Met_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_SF_Met_Raw/GGL_SF_Met_CR1000_ten_min.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_SF_Met/GGL_SF_Met_ExcelandMeta/GGL_SF_Met_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

newcols = {
    'RECORD': 'GGL_SF_Met Record Number',
    'BattV_Min': 'GGL_SF_Met Battery Voltage Minimum',
    'PTemp_C_Avg': 'PTemp_C_Avg',
    'Rain_mm_Tot': 'GGL_SF_Met tipping bucket rain gage, total, mm',
    'BP_mbar_Avg': 'GGL_SF_Met BP_mbar_Avg',
    'VW_Avg': 'GGL_SF_Met Volumetric Water Content, Average fractional 22 cm',
    'PA_uS_Avg': 'GGL_SF_Met Volumetric Water Content, PA_uS_Avg 22 cm',
    'T107_C_Avg': 'GGL_SF_Met Soil Temperature Average 22 cm',
    'WS_ms_Avg': 'GGL_SF_Met Wind Speed Average m/sec',
    'WS_ms_Max': 'GGL_SF_Met  Wind Speed Max',
    'WS_ms_TMx': 'GGL_SF_Met  Time of Wind Speed Max',
    'WS_ms_Min': 'GGL_SF_Met Wind Speed m/sec Minimum',
    'WS_ms_TMn': 'WS_ms_TMn',
    'NR_Wm2_Avg': 'GGL_SF_Met Net Radiation W/m^2 Average',
    'CNR_Wm2_Avg': 'GGL_SF_Met Net Radiation Corrected for Windspeed W/m^2 Average',
    'SlrW_Avg': 'GGL_SF_Met Incoming Shortwave Radiation W/m^2',
    'SlrW_Max': 'SlrW_Max W/m^2',
    'SlrW_TMx': 'SlrW_TMx W/m^2',
    'SlrW_Min': 'SlrW_Min W/m2',
    'SlrW_TMn': 'SlrW_TMn',
    'SlrMJ_Tot': 'GGL_SF_Met Incoming Shortwave Radiation MJ/m^2',
    'AirTC_Avg': 'GGL_SF_Met Air Temperature Average, Degrees C 2.6 meters',
    'RH': 'RH (%)',
    'RH_Max': 'RH_Max',
    'RH_TMx': 'RH_TMx',
    'RH_Min': 'RH_Min',
    'RH_TMn': 'RH_TMn',
    'ETos': 'Etos Deg C',
    'Rso': 'Rso Deg C',

}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'GGL_SF_Met Record Number',
        'GGL_SF_Met Battery Voltage Minimum',
        'PTemp_C_Avg',
        'GGL_SF_Met Volumetric Water Content, PA_uS_Avg 22 cm',
        'WS_ms_TMn',
        'SlrW_Max W/m^2',
        'SlrW_TMx W/m^2',
        'SlrW_Min W/m2',
        'SlrW_TMn',
        'Etos Deg C',
        'Rso Deg C'
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
    '''dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
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
    dfd.iloc[:, 19] = dfd.iloc[:, 19].round(3)'''


    return dfd

df_master = QualityZone.download_master(master_path)
df_new = QualityZone.download_new_data(new_path, newcols)
df_updated = QualityZone.append_non_duplicates(df_master, df_new)

working_file_path = '/Boulder Creek CZO Team Folder/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv'
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=1)
pm.check_corrupt([-6999, 'NAN'])
pm.check_range([12, 15.1], 'GGL_SF_Met Battery Voltage Minimum')

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

trace1 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_SF_Met Incoming Shortwave Radiation W/m^2'],
    mode='lines',
    name='Shortwave Radiation w/m^2',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_SF_Met Net Radiation W/m^2 Average'],
    mode='lines',
    name='Corrected Net Radiation w/m^2',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGL_SF_Met Net Radiation Corrected for Windspeed W/m^2 Average'],
    mode='lines',
    name='NR Average w/m^2',
)
trace4 = go.Scatter(
    x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Air Temperature Average, Degrees C 2.6 meters'],
    mode='lines',
    name='Air T Deg C',
)
# wind
trace5 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Wind Speed m/sec Minimum'],
    mode='lines',
    name='Wind Min',
)
trace6 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met  Wind Speed Max'],
    mode='lines',
    name='Wind Max',
)
trace7 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Wind Speed Average m/sec'],
    mode='lines',
    name='Wind Avg',
)
# battery
trace8 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Battery Voltage Minimum'],
    mode='lines',
    name='Battery Voltage',
)
# tipping & rh
trace9 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met tipping bucket rain gage, total, mm'],
    mode='lines',
    name='Precipitation (mm)',
)
trace10 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['RH (%)'],
    mode='lines',
    name='RH %',
)
trace11 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met BP_mbar_Avg'],
    mode='lines',
    name='BP (mb)',
)
trace12 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Volumetric Water Content, Average fractional 22 cm'],
    mode='lines',
    name='22cm soil VWC',
)
trace13 = go.Scatter(
x = plotly_df.index,
    y=plotly_df['GGL_SF_Met Soil Temperature Average 22 cm'],
    mode='lines',
    name='22cm soil T',
)

# -------------------------------------------------------------------
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
fig['layout'].update(title='GGL_SF_Met_Rad_AirT')

plot_url = py.plot(fig, filename='GGL_SF_Met_Air_Rad')
#plotly.offline.plot(fig,
                    #filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_SF_Met\\GGL_SF_Met_PythonScripts\\1.html')

# -------AIR T BATTV-------------------------------------------------------------
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
fig2['layout'].update(title='GGL_SF_Met_Wind_BattV')

#plotly.offline.plot(fig2,
                    #filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_SF_Met\\GGL_SF_Met_PythonScripts\\2.html')
plot_url = py.plot(fig2, filename='GGL_SF_Met_Wind_Battv')

# --------------------------------------------------------------------

fig3 = tools.make_subplots(rows=5, cols=1, specs=[[{}], [{}], [{}], [{}], [{}]],
                           shared_xaxes=True,
                           vertical_spacing=0.001)

fig3.append_trace(trace9, 1, 1)
fig3.append_trace(trace10, 2, 1)
fig3.append_trace(trace11, 3, 1)
fig3.append_trace(trace12, 4, 1)
fig3.append_trace(trace13, 5, 1)
# axis titles
fig3['layout']['yaxis1'].update(title='MM')
fig3['layout']['yaxis2'].update(title='%')
fig3['layout']['yaxis3'].update(title='mB')
fig3['layout']['yaxis4'].update(title='%')
fig3['layout']['yaxis5'].update(title='C')
# Plot Title
fig3['layout'].update(title='GGL_SF_Met')

plot_url = py.plot(fig3, filename='GGL_SF_Met_3')
#plotly.offline.plot(fig3,
                    #filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_SF_Met\\GGL_SF_Met_PythonScripts\\3.html')




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

