from sites import QualityZone
import os
import tempfile
import pecos
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
from plotly import tools
import webbrowser
import click

print("Starting QualityZone")
print("Checking Dropbox API")
#print(QualityZone.dbx.users_get_current_account())

system_name = 'B1_MET'
dropbox_base = '/CZO/BcCZO/'
master_file = 'Data/B1/B1_Met/B1_Met_QAQC/B1_Met_ExcelandMeta/B1_Met_Master_WY2020.csv'
distribute_file = 'Data/B1/B1_Met/B1_Met_QAQC/B1_Met_ExcelandMeta/B1_Met_Distribute_WY2020.csv'
raw_folder = os.path.join(dropbox_base + '/ToughBook_Share/B1/Data/B1_Met/B1_Met_WY2020/')
master_path = os.path.join(dropbox_base + master_file)
#new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

# need to write some kind of tempfile cleanup function
local_raw = tempfile.mkdtemp()



newcols = {
    'RECORD': 'Record_Number',
    'BattV_Min': 'Battv_min',
    'WS_ms_Min': 'Wind_Speed_Min_MS',
    'WS_ms_Max':'Wind_Speed_Max_MS',
    'WS_ms_Avg':'Wind_Speed_Avg_MS',
    'WindDir':'Wind_Direction',
    'Rain_mm_Tot':'Rain_mm_total',
    'NR_Wm2_Avg':'NR_Wm2_Avg',
    'CNR_Wm2_Avg':'CNR_Wm2_Avg',
    'AirTC_Avg':'AirT_C_Avg',
    'AirTC_Max':'Air_T_C_Max',
    'AirTC_Min':'Air_T_C_Min',
    'RH_Min':'RH_Min',
    'RH_Max':'RH_Max',
    'RH':'RH',
    'DT_Avg':'Distance (m)',
    'TCDT_Avg':'Temperature Compensated Distance (m)',
    'Eb':'Eb',
    'Temp':'Soil_Temp',
    'EC':'EC',
    'VWC':'VWC'

}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record_Number',
        'Battv_min',
        'Distance (m)',
        'Eb',
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        'Wind_Speed_Min_MS':'WINDSPEED(m/s)-2.5M(MIN)',
        'Wind_Speed_Max_MS':'WINDSPEED(m/s)-2.5M(MAX)',
        'Wind_Speed_Avg_MS':'WINDSPEED(m/s)-2.5M(AVG)',
        'Wind_Direction':'WINDDIR-1.5M(DEGREES)',
        'Rain_mm_total':'RAIN GAGE(MM)',
        'NR_Wm2_Avg':'NET RAD(W/m^2)-1.5M(AVG)',
        'CNR_Wm2_Avg':'NET RAD CORR(W/m^2)-1.5M(AVG)',
        'AirT_C_Avg':'AIRTEMP(C)-1.5M(AVG)',
        'Air_T_C_Max':'AIRTEMP(C)-1.5M(MAX)',
        'Air_T_C_Min':'AIRTEMP(C)-1.5M(MIN)',
        'RH_Min':'RH (MIN)',
        'RH_Max':'RH (MAX)',
        'RH':'RH (%)',
        'Soil_Temp':'SOIL TEMP(C)-25CM',
        'EC':'SOIL TEMP(C)-25CM',
        'VWC':'SOIL VOL WATER CONTENT(%)-25CM',
        'Temperature Compensated Distance (m)':'DISTANCE(M)'
    }


    dfd.rename(columns=dist_columns, inplace=True)
    '''dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)
    dfd.iloc[:, 8] = dfd.iloc[:, 8].round(3)
    dfd.iloc[:, 9] = dfd.iloc[:, 9].round(3)
    dfd.iloc[:, 10] = dfd.iloc[:, 10].round(3)
    dfd.iloc[:, 11] = dfd.iloc[:, 11].round(3)
    dfd.iloc[:, 12] = dfd.iloc[:, 12].round(3)
    dfd.iloc[:, 13] = dfd.iloc[:, 13].round(3)
    dfd.iloc[:, 14] = dfd.iloc[:, 14].round(3)
    dfd.iloc[:, 15] = dfd.iloc[:, 15].round(3)
    dfd.iloc[:, 16] = dfd.iloc[:, 16].round(3)'''
    return dfd




df_master = QualityZone.download_master(master_path)
QualityZone.dbx_dat_folder_download(raw_folder, local_raw)
df_new = QualityZone.concat_dat(local_raw, start_date='2019-10-01')
df_new.rename(columns=newcols, inplace=True)
'''df_new.columns = ['Record_Number',
                  'Battv_min',
                  'Wind_Speed_Min_MS',
                  'Wind_Speed_Max_MS',
                  'Wind_Speed_Avg_MS',
                  'Wind_Direction',
                  'Rain_mm_total',
                  'NR_Wm2_Avg',
                  'CNR_Wm2_Avg',
                  'AirT_C_Avg',
                  'Air_T_C_Max',
                  'Air_T_C_Min',
                  'RH_Min',
                  'RH_Max',
                  'RH',
                  'Distance (m)',
                  'Temperature Compensated Distance (m)',
                  'Eb',
                  'Soil_Temp',
                  'EC',
                  'VWC'
                  ]'''

df_updated = QualityZone.append_non_duplicates(df_master, df_new)

# working file access
working_file_path = os.path.join(dropbox_base + 'Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv')
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=1)
pm.check_corrupt([-7999, 'NAN'])
pm.check_range([12, 15.1], 'Battv_min')


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

trace1 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Temperature Compensated Distance (m)'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='T compensated distance'
)
trace2 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Battv_min'],
    mode='lines',
    name='Battv min',
)

fig = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 2, 1)

plot_url = py.plot(fig, filename='B1_Met_Depth_Battv')


trace3 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Rain_mm_total'],
    mode='lines',
    name='Precipitation'
)
trace4 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Eb'],
    mode='lines',
    name='Soil Eb',
)
trace5 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['EC'],
    mode='lines',
    name='Soil EC',
)
trace6 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Soil_Temp'],
    mode='lines',
    name='Soil T',
)
trace7 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['VWC'],
    mode='lines',
    name='Soil VWC',
)

fig2 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig2.append_trace(trace3, 2, 1)
fig2.append_trace(trace4, 1, 1)
fig2.append_trace(trace5, 3, 1)
fig2.append_trace(trace6, 1, 1)
fig2.append_trace(trace7, 3, 1)


#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig2, filename='B1_Met_Soil')



trace8 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind_Direction'],
    mode='lines',
    name='Wind Direction'
)
trace9 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind_Speed_Avg_MS'],
    mode='lines',
    name='Wind Speed Average',
)
trace10 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind_Speed_Max_MS'],
    mode='lines',
    name='Wind Speed Max',
)
trace11 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind_Speed_Min_MS'],
    mode='lines',
    name='Wind Speed Min',
)


fig3 = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig3.append_trace(trace8, 2, 1)
fig3.append_trace(trace9, 1, 1)
fig3.append_trace(trace10, 1, 1)
fig3.append_trace(trace11, 1, 1)


#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig3, filename='B1_Met_Wind')



trace12 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['NR_Wm2_Avg'],
    mode='lines',
    name='Net Radiation AVG'
)
trace13 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['CNR_Wm2_Avg'],
    mode='lines',
    name='Corrected Net Radiation AVG',
)
trace14 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Air_T_C_Min'],
    mode='lines',
    name='Air T Min',
)
trace15 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Air_T_C_Max'],
    mode='lines',
    name='Air T Max',
)
trace16 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['AirT_C_Avg'],
    mode='lines',
    name='Air T AVG',
)
trace17 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['RH'],
    mode='lines',
    name='RH %',
)

fig4 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig4.append_trace(trace1, 1, 1)
fig4.append_trace(trace2, 1, 1)
fig4.append_trace(trace3, 2, 1)
fig4.append_trace(trace4, 2, 1)
fig4.append_trace(trace5, 2, 1)
fig4.append_trace(trace6, 3, 1)



plot_url = py.plot(fig4, filename='B1_Met_AirT_Radiaiton')






if click.confirm('Did you make changes to the data via the "QualityZone_working_data.csv" file?', default=False):
    df_updated = QualityZone.download_master(working_file_path)
    #print(updated_frame.dtypes)
    print('dataframe replaced with updated data from Quality_Zone_working_data.csv')

if click.confirm('Save updated dataset as master .csv?', default=False):
    QualityZone.df_to_dropbox(df_updated, master_path)
    print("Master .csv uploaded to dropbox")

if click.confirm('Save updated dataset to distribute .csv?', default=False):
    dist_df = format_for_dist(df_updated)
    QualityZone.df_to_dropbox(dist_df, distribute_path)
    print("distribute .csv formatted for distribution and uploaded to dropbox")


