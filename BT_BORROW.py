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

system_name = 'BT_BORROW'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/Betasso/Betasso_Soil/BT_Borrow/BT_Borrow_Excelandmeta/BT_Borrow_Master_WY2020.csv'
new_file = '/Toughbook_Share/Betasso/Betasso_Soil/BT_Borrow/data/BT_Borrow_CR10xPB.dat'
distribute_file = '/Data/Betasso/Betasso_Soil/BT_Borrow/BT_Borrow_Excelandmeta/BT_Borrow_Distribute_WY2020.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

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


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record Number',
        'Battery Voltage, DC Volts',
        'Program Signature (datalogger value)'
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        'Volumetric Water Content at 40 cm depth': 'VOL WATER CONTENT-40CM',
        'Volumetric Water Content at 70 cm depth':'VOL WATER CONTENT-70CM',
        'Volumetric Water Content at 110 cm depth':'VOL WATER CONTENT-110CM',
        'Water Potential at 40 cm depth (mV)':'WATER POTENTIAL-40CM(mV)',
        'Water Potential at 40 cm depth (kPa)':'WATER POTENTIAL-40CM(kPa)',
        'Water Potential at 70 cm depth (mV)':'WATER POTENTIAL-70CM(mV)',
        'Water Potential at 70 cm depth ( kPa)':'WATER POTENTIAL-70CM(kPa)',
        'Water Potential at 110 cm depth (mV)':'WATER POTENTIAL-110CM(mV)',
        'Water Potential at 110 cm depth (kpa)':'WATER POTENTIAL-110CM(kPa)',
    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)


    return dfd

df_master = QualityZone.download_master(master_path)
df_new = QualityZone.download_new_data(new_path, newcols, start_date='2019-10-01')
df_updated = QualityZone.append_non_duplicates(df_master, df_new)

working_file_path = os.path.join(dropbox_base + '/Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv')
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=1)
pm.check_corrupt([-6999, -7999, 'NAN'])
pm.check_range([12, 15.1], 'Battery Voltage, DC Volts')

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
    y=plotly_df['Volumetric Water Content at 40 cm depth'],
    mode='lines',
    name='WWC 40cm'
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Volumetric Water Content at 70 cm depth'],
    mode='lines',
    name='VWC 70cm',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Volumetric Water Content at 110 cm depth'],
    mode='lines',
    name='VWC 110cm',
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Water Potential at 40 cm depth (mV)'],
    mode='lines',
    name='WP 40cm (mV)',
)
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Water Potential at 70 cm depth (mV)'],
    mode='lines',
    name='WP 70cm (mV)',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Water Potential at 110 cm depth (mV)'],
    mode='lines',
    name='WP 110cm (mV)',
)

fig = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 1)
fig.append_trace(trace3, 1, 1)
fig.append_trace(trace4, 2, 1)
fig.append_trace(trace5, 2, 1)
fig.append_trace(trace6, 2, 1)

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig, filename='BT_Borrow_Soil')

trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, DC Volts'],
    mode='lines',
    name='BattV'
)

data = [trace7]

plot_url = py.plot(data, filename='BT_Borrow_Battv')

url = "D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Personnel_Folders\\Dillon_Ragar\\QualityZone\\Results\\BT_Borrow.html"
webbrowser.open(url,new=2)



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

