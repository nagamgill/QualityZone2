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

system_name = 'BT_GULLY'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/Betasso/Betasso_Soil/BT_Gully/BT_Gully_ExcelandMeta/BT_Gully_CR1000X_Master_WY2019.csv'
new_file = '/Toughbook_Share/Betasso/Betasso_Soil/BT_Gully/data/BT_Gully_CR1000X_BT_Gully_10min_2018_08_14_10_15_49.dat'
distribute_file = '/Data/Betasso/Betasso_Soil/BT_Gully/BT_Gully_ExcelandMeta/BT_Gully_CR1000X_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'TIMESTAMP': 'Date plus time (0000 midnight notation)',
    'RECORD': 'Record Number',
    'batt_volt_Min':'Battery Voltage, DC Volts',
    'PTemp': 'Panel Temp',
    'VW_Avg': 'Volumetric Water Content 1',
    'VW_2_Avg': 'Volumetric Water Content 2',
    'VW_3_Avg': 'Volumetric Water Content 3',
    'WP_mV_Avg': 'Water Potential 1 (mV)',
    'WP_mV_2_Avg': 'Water Potential 2 (mV)',
    'WP_mV_3_Avg': 'Water Potential 3 (mV)',
    'WP_kPa_Avg': 'Water Potential 1 (kPa)',
    'WP_kPa_2_Avg': 'Water Potential 2 (kPa)',
    'WP_kPa_3_Avg': 'Water Potential 3 (kpa)',
    'Retries(1)':'BTSD_1, Retries, number',
    'Retries(2)':'BTSD_2, Retries, number',
    'Retries(3)':'BTSD_3, Retries, number',
    'Retries(4)':'BTSD_4, Retries, number',
    'Retries(5)':'BTSD_5, Retries, number',
    'Air_TempC(1)':'BTSD_1, Temperature, deg C',
    'Air_TempC(2)':'BTSD_2, Temperature, deg C',
    'Air_TempC(3)':'BTSD_3, Temperature, deg C',
    'Air_TempC(4)':'BTSD_4, Temperature, deg C',
    'Air_TempC(5)':'BTSD_5, Temperature, deg C',
    'Distance(1)':'BTSD_1, Distance, cm',
    'Distance(2)':'BTSD_2, Distance, cm',
    'Distance(3)':'BTSD_3, Distance, cm',
    'Distance(4)':'BTSD_4, Distance, cm',
    'Distance(5)':'BTSD_5, Distance, cm',

}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Panel Temp',
        'Record Number',
        'Battery Voltage, DC Volts',
        'BTSD_1, Retries, number',
        'BTSD_2, Retries, number',
        'BTSD_3, Retries, number',
        'BTSD_4, Retries, number',
        'BTSD_5, Retries, number',
        'BTSD_1, Temperature, deg C',
        'BTSD_2, Temperature, deg C',
        'BTSD_3, Temperature, deg C',
        'BTSD_4, Temperature, deg C',
        'BTSD_5, Temperature, deg C',
        'BTSD_1, Distance, cm',
        'BTSD_2, Distance, cm',
        'BTSD_3, Distance, cm',
        'BTSD_4, Distance, cm',
        'BTSD_5, Distance, cm',

        ], axis=1)

    dist_columns = {
        'Volumetric Water Content 1':"VOL WATER CONTENT-15CM",
        'Volumetric Water Content 2':"VOL WATER CONTENT-40CM",
        'Volumetric Water Content 3':"VOL WATER CONTENT-70CM",
        'Water Potential 1 (mV)':"WATER POTENTIAL-15CM(mV)",
        'Water Potential 1 (kPa)': "WATER POTENTIAL-15CM(kPa)",
        'Water Potential 2 (mV)':"WATER POTENTIAL-40CM(mV)",
        'Water Potential 2 (kPa)':"WATER POTENTIAL-40CM(kPa)",
        'Water Potential 3 (mV)':"WATER POTENTIAL-70CM(mV)",
        'Water Potential 3 (kpa)':"WATER POTENTIAL-70CM(kPa)"

    }

    dfd.rename(columns=dist_columns, inplace=True)
    dfd = dfd[["VOL WATER CONTENT-15CM",
               "VOL WATER CONTENT-40CM",
               "VOL WATER CONTENT-70CM",
               "WATER POTENTIAL-15CM(mV)",
               "WATER POTENTIAL-15CM(kPa)",
               "WATER POTENTIAL-40CM(mV)",
               "WATER POTENTIAL-40CM(kPa)",
               "WATER POTENTIAL-70CM(mV)",
               "WATER POTENTIAL-70CM(kPa)"
               ]]

    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)
    dfd.iloc[:, 8] = dfd.iloc[:, 8].round(3)

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
pm.check_range([12, 15.1], 'Battery Voltage, DC Volts')
pm.check_increment([None,200], 'Water Potential 1 (kPa)')

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
    y=plotly_df['BTSD_1, Distance, cm'],
    mode='markers',
    name='SD_1'
)
trace2 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['BTSD_2, Distance, cm'],
    mode='markers',
    name='SD_2',
)
trace3 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['BTSD_3, Distance, cm'],
    mode='markers',
    name='SD_3',
)
trace4 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['BTSD_4, Distance, cm'],
    mode='markers',
    name='SD_4',
)
trace5 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['BTSD_5, Distance, cm'],
    mode='markers',
    name='SD_5',
)
trace6 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, DC Volts'],
    mode='lines',
    name='Batt v',
)

fig = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 1)
fig.append_trace(trace3, 1, 1)
fig.append_trace(trace4, 1, 1)
fig.append_trace(trace5, 1, 1)
fig.append_trace(trace6, 2, 1)

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig, filename='BT_Gully_Depth_Battv')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\Betasso_Soil\\BT_Gully\\BT_Gully_Plots_WY2019\\1.html')

#---------------------------------------------------------------------------------------------------

trace7 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Volumetric Water Content 1'],
    mode='lines',
    name='WWC 40cm'
)
trace8 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Volumetric Water Content 2'],
    mode='lines',
    name='VWC 70cm',
)
trace9 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Volumetric Water Content 3'],
    mode='lines',
    name='VWC 110cm',
)
trace10 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 1 (mV)'],
    mode='lines',
    name='WP 40cm (mV)',
)
trace11 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 2 (mV)'],
    mode='lines',
    name='WP 70cm (mV)',
)
trace12 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 3 (mV)'],
    mode='lines',
    name='WP 110cm (mV)',
)


trace20 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 1 (kPa)'],
    mode='lines',
    name='WP (kPa) 15cm'
)
trace21 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 2 (kPa)'],
    mode='lines',
    name='WP (kPa) 40cm'
)
trace22 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Water Potential 3 (kpa)'],
    mode='lines',
    name='WP (kPa) 70cm'
)




fig2 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig2.append_trace(trace7, 1, 1)
fig2.append_trace(trace8, 1, 1)
fig2.append_trace(trace9, 1, 1)
fig2.append_trace(trace10, 2, 1)
fig2.append_trace(trace11, 2, 1)
fig2.append_trace(trace12, 2, 1)
fig2.append_trace(trace20, 3, 1)
fig2.append_trace(trace21, 3, 1)
fig2.append_trace(trace22, 3, 1)

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig2, filename='BT_Gully_Soil')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\Betasso_Soil\\BT_Gully\\BT_Gully_WY2019\\BT_Gully_Plots_WY2019\\2.html')

# open Pecos results
url = "D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Personnel_Folders\\Dillon_Ragar\\QualityZone\\Results\\BT_GULLY.html"
webbrowser.open(url, new=2)



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



