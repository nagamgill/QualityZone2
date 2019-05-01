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
import pandas as pd
import numpy as np



print("Starting QualityZone")
print("Checking Dropbox API")
print(QualityZone.dbx.users_get_current_account())

system_name = 'GGL_SF_SP10'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_SF_SP10/GGL_SF_SP10_ExcelandMeta/GGL_SF_SP10_WY2019_Master.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_SF_SP10_Raw/GGL_SF_SP10_CR1000.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_SF_SP10/GGL_SF_SP10_ExcelandMeta/GGL_SF_SP10_WY2019_Distribute.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'RECORD':'Record Number (datalogger value)',
    'BattV':'Battery Voltage, Minimum, DC Volts',
    'WP_Temp_C':'Wiring Panel Temperature, Average, Deg C',
    'T107_C_1':'Simulated Rain, 5 cm, Temperature, degree C',
    'VW_1':'Simulated Rain, 5 cm, Volumetric Water Content, fraction',
    'PA_uS_1':'Simulated Rain, 5 cm, CS616 period average, u sec',
    'T107_C_2':'Simulated Rain, 25 cm, Temperature, degree C',
    'VW_2':'Simulated Rain, 25 cm, Volumetric Water Content, fraction',
    'PA_uS_2':'Simulated Rain, 25 cm, CS616 period average, u sec',
    'T107_C_3':'Snow Melt, 5 cm, Temperature, degree C',
    'VW_3':'Snow Melt, 5 cm, Volumetric Water Content, fraction',
    'PA_uS_3':'Snow Melt, 5 cm, CS616 period average, u sec',
    'T107_C_4':'Snow Melt, 25 cm, Temperature, degree C',
    'VW_4':'Snow Melt, 25 cm, Volumetric Water Content, fraction',
    'PA_uS_4':'Snow Melt, 25 cm, CS616 period average, u sec',
    'TempC_GGSD_14':'GGSD_14, Temperature, deg C',
    'Time_GGSD_14':'GGSD_14, Travel Time, 10*us',
    'Depth_GGSD_14':'GGSD_14, Distance, cm',
    'Retries_GGSD_14':'GGSD_14, Retries, number',
    'TempC_GGSD_16':'GGSD_16, Temperature, deg C',
    'Time_GGSD_16':'GGSD_16, Travel Time, 10*us',
    'Depth_GGSD_16':'GGSD_16, Distance, cm',
    'Retries_GGSD_16':'GGSD_16, Retries, number',
    'TempC_GGSD_15':'GGSD_15, Temperature, deg C',
    'Time_GGSD_15':'GGSD_15, Travel Time, 10*us',
    'Depth_GGSD_15':'GGSD_15, Distance, cm',
    'Retries_GGSD_15':'GGSD_15, Retries, number',
    'TempC_GGSD_13':'GGSD_13, Temperature, deg C',
    'Time_GGSD_13':'GGSD_13, Travel Time, 10*us',
    'Depth_GGSD_13':'GGSD_13, Distance, cm',
    'Retries_GGSD_13':'GGSD_13, Retries, number',
    'TempC_GGSD_12':'GGSD_12, Temperature, deg C',
    'Time_GGSD_12':'GGSD_12, Travel Time, 10*us',
    'Depth_GGSD_12':'GGSD_12, Distance, cm',
    'Retries_GGSD_12':'GGSD_12, Retries, number',
    'TempC_GGSD_9':'GGSD_9, Temperature, deg C',
    'Time_GGSD_9':'GGSD_9,Travel Time, 10*us',
    'Depth_GGSD_9':'GGSD_9, Distance, cm',
    'Retries_GGSD_9':'GGSD_9, Retries, number',
    'TempC_GGSD_10':'GGSD_10, Temperature, deg C',
    'Time_GGSD_10':'GGSD_10, Travel Time, 10*us',
    'Depth_GGSD_10':'GGSD_10, Distance, cm',
    'Retries_GGSD_10':'GGSD_10, Retries, number',
    'TempC_GGSD_11':'GGSD_11, Temperature, deg C',
    'Time_GGSD_11':'GGSD_11, Travel Time, 10*us',
    'Depth_GGSD_11':'GGSD_11, Distance, cm',
    'Retries_GGSD_11':'GGSD_11, Retries, number'

}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
    'GGSD_14, Temperature, deg C',
    'GGSD_14, Travel Time, 10*us',
    'GGSD_14, Distance, cm',
    'GGSD_14, Retries, number',
    'GGSD_16, Temperature, deg C',
    'GGSD_16, Travel Time, 10*us',
    'GGSD_16, Distance, cm',
    'GGSD_16, Retries, number',
    'GGSD_15, Temperature, deg C',
    'GGSD_15, Travel Time, 10*us',
    'GGSD_15, Distance, cm',
    'GGSD_15, Retries, number',
    'GGSD_13, Temperature, deg C',
    'GGSD_13, Travel Time, 10*us',
    'GGSD_13, Distance, cm',
    'GGSD_13, Retries, number',
    'GGSD_12, Temperature, deg C',
    'GGSD_12, Travel Time, 10*us',
    'GGSD_12, Distance, cm',
    'GGSD_12, Retries, number',
    'GGSD_9, Temperature, deg C',
    'GGSD_9,Travel Time, 10*us',
    'GGSD_9, Distance, cm',
    'GGSD_9, Retries, number',
    'GGSD_10, Temperature, deg C',
    'GGSD_10, Travel Time, 10*us',
    'GGSD_10, Distance, cm',
    'GGSD_10, Retries, number',
    'GGSD_11, Temperature, deg C',
    'GGSD_11, Travel Time, 10*us',
    'GGSD_11, Distance, cm',
    'GGSD_11, Retries, number',
    'Record Number (datalogger value)',
    'Battery Voltage, Minimum, DC Volts',
    'Wiring Panel Temperature, Average, Deg C'

    ], axis=1)

    dist_columns = {
    'Simulated Rain, 5 cm, Temperature, degree C': 'SR-TEMP(C)-5CM',
    'Simulated Rain, 5 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-5CM',
    'Simulated Rain, 25 cm, Temperature, degree C':'SR-TEMP(C)-25CM',
    'Simulated Rain, 25 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-25CM',
    'Snow Melt, 5 cm, Temperature, degree C':'SM-TEMP(C)-5CM',
    'Snow Melt, 5 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-5CM',
    'Snow Melt, 25 cm, Temperature, degree C':'SM-TEMP(C)-25CM',
    'Snow Melt, 25 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-25CM'
    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(2)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(2)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(2)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(2)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)

    dfd = dfd.fillna('')
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
pm.check_corrupt(['NaN', 'Na', 'na', 'np.nan', 'nan'])
pm.check_corrupt(corrupt_values=['nan','NAN'])
pm.check_range([12, 15.1], 'Battery Voltage, Minimum, DC Volts')
pm.check_outlier([None, 5], 'GGSD_13, Distance, cm', window=168*3600)

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
# -------------------------------------------------------------------
trace1 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SR 5cm T107'
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 5cm VWC',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 5cm CS616',
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, Temperature, degree C'],
    mode='lines',
    name='SR 25cm T107'
)
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 25cm VWC'
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 25cm CS616'
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SM 5cm T107'
)
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 5cm VWC'
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SM 5cm CS616'
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 25 cm, Temperature, degree C'],
    mode='lines',
    name='SM 25cm T107'
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 25 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 25cm VWC'
)
trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 25 cm, CS616 period average, u sec'],
    mode='lines',
    name='SM 25cm CS616'
)
trace14 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_16, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_16'
)
trace15 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_15, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_15'
)
trace16 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_13, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_13'
)
trace17 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_12, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_12'
)
trace18 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_9, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_9'
)
trace19 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_10, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_10'
)
trace20 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['GGSD_11, Distance, cm'],
    mode = 'markers',
    marker = dict(
        size = 2,
    ),
    name='GGSD_11'
)

trace21 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, Minimum, DC Volts'],
    mode='lines',
    name='batt_v'
)


fig1 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                            shared_xaxes=True,
                            vertical_spacing=0.001)
fig1.append_trace(trace1, 1, 1)
fig1.append_trace(trace4, 1, 1)
fig1.append_trace(trace8, 1, 1)
fig1.append_trace(trace11, 1, 1)

fig1.append_trace(trace2, 2, 1)
fig1.append_trace(trace5, 2, 1)
fig1.append_trace(trace9, 2, 1)
fig1.append_trace(trace12, 2, 1)

fig1.append_trace(trace3, 3, 1)
fig1.append_trace(trace6, 3, 1)
fig1.append_trace(trace10, 3, 1)
fig1.append_trace(trace13, 3, 1)

fig1['layout'].update(title='GGL_SF_SP10')
plot_url = py.plot(fig1, filename='GGL_SF_SP10', auto_open=True)
# --------------------------------------------------------------------------------------
data = [trace21]
layout = go.Layout(
    title="GGl_SF_SP10"
)
battv = go.Figure(data=data, layout=layout)

plot_url = py.iplot(battv, filename='GGL_SF_SP10_Battery', auto_open=True)

# -------------------------------------------------------------------------------
fig2 = tools.make_subplots(rows=7, cols=1, specs=[[{}], [{}], [{}], [{}], [{}], [{}], [{}]],
                            shared_xaxes=True,
                            vertical_spacing=0.001)

fig2.append_trace(trace14, 1, 1)
fig2.append_trace(trace15, 2, 1)
fig2.append_trace(trace16, 3, 1)
fig2.append_trace(trace17, 4, 1)
fig2.append_trace(trace18, 5, 1)
fig2.append_trace(trace19, 6, 1)
fig2.append_trace(trace20, 7, 1)

fig2['layout'].update(title='GGL_SF_SP10_Snow_Depth')

plot_url = py.iplot(fig2, filename='GGL_SF_SP10_Snow_Depth', auto_open=True)

#--------------Snow Plot-----------------------------------------------------------------

data3 = [trace14, trace15, trace16, trace17, trace18, trace19, trace20]
layout3 = go.Layout(
    title="GGL_SF_SP10_Snow_Depth_Alternate"
)

fig3 = go.Figure(data=data3, layout=layout3)
plot_url = py.plot(fig3, filename='GGL_SF_SP10_Snow_Depth_Alternate')


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


