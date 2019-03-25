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

dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_SF_SP6/GGL_SF_SP6_ExcelandMeta/GGL_SF_SP6_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_SF_SP6_Raw/GGL_SF_SP6_CR1000_Soil.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_SF_SP6/GGL_SF_SP6_ExcelandMeta/GGL_SF_SP6_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)
# not sure if this is neccesary with the .CSV system, needs testing
master_index = ''

newcols = {
    'RECORD': 'Record Number',
    'cm5_VW_Avg': 'Soill Moisture, Volumetric Water Content, 5 cm, fraction',
    'PA_uS_Avg': 'Soil Moisture, CS616 period average, 5 cm, u sec',
    'cm5_T107_C_Avg': 'Soil Temperature, 5 cm, degree C',
    'VW_2_Avg': 'Soill Moisture, Volumetric Water Content, 25 cm, fraction',
    'PA_uS_2_Avg': 'Soil Moisture, CS616 period average, 25 cm, u sec',
    'cm25_T107_C_Avg': 'Soil Temperature, 25 cm, degree C',
    'BattV_Min':'Battery Voltage, DC Volts',
}

def format_for_dist(dataframe):
    df = dataframe.copy()
    df = df.drop(['Record Number',
                           'Battery Voltage, DC Volts',
                           'Soil Moisture, CS616 period average, 5 cm, u sec',
                           'Soil Moisture, CS616 period average, 25 cm, u sec'
                           ], axis=1)
    df = df.fillna('')
    dist_columns = {
        'Soill Moisture, Volumetric Water Content, 5 cm, fraction':'Vol Water Content-5cm',
        'Soil Temperature, 5 cm, degree C':'TEMP(C)-5cm',
        'Soill Moisture, Volumetric Water Content, 25 cm, fraction':'Vol Water Content-25cm',
        'Soil Temperature, 25 cm, degree C':'TEMP(C)-25cm'
    }
    df.rename(columns=dist_columns, inplace=True)
    df['Vol Water Content-5cm'] = df['Vol Water Content-5cm'].round(3)
    df['TEMP(C)-5cm'] = df['TEMP(C)-5cm'].round(2)
    df['Vol Water Content-25cm'] = df['Vol Water Content-25cm'].round(3)
    df['TEMP(C)-25cm'] = df['TEMP(C)-25cm'].round(2)
    return df

df_master = QualityZone.download_master(master_path)
df_new = QualityZone.download_new_data(new_path, newcols)
df_updated = QualityZone.append_non_duplicates(df_master, df_new)

working_file_path = '/Boulder Creek CZO Team Folder/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/QZ_working_file.csv'
QualityZone.df_to_dropbox(df_updated, working_file_path)

pecos.logger.initialize()
pm = pecos.monitoring.PerformanceMonitoring()
system_name = 'GGL_SF_SP6'
df = df_updated.copy()
pm.add_dataframe(df)
pm.check_timestamp(600)
pm.check_missing(min_failures=5)
pm.check_corrupt([-6999, 'NAN'])
pm.check_range([12, 15.1], 'Battery Voltage, DC Volts')

mask = pm.get_test_results_mask()
QCI = pecos.metrics.qci(mask, pm.tfilter)

dirpath = tempfile.mkdtemp()

results_directory = 'dirpath'
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
QualityZone.qc_results_to_dropbox('dirpath')


plotly_df = df_updated.copy()

trace1 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Soill Moisture, Volumetric Water Content, 25 cm, fraction'],
    mode='lines',
    name='25cm VWC',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Soill Moisture, Volumetric Water Content, 5 cm, fraction'],
    mode='lines',
    name='5cm VWC',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Soil Temperature, 5 cm, degree C'],
    mode='lines',
    name='5cm T107'
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Soil Temperature, 25 cm, degree C'],
    mode='lines',
    name='25cm T107'
)


fig = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 1)
fig.append_trace(trace3, 2, 1)
fig.append_trace(trace4, 2, 1)

# axis titles
fig['layout']['yaxis1'].update(title='VWC Fraction')
fig['layout']['yaxis2'].update(title='Degree C')
# Plot Title
fig['layout'].update(title='GGL_SF_SP6_Soil')


plot_url = py.plot(fig, filename='QualityZone/GGL_SF_SP6/GGL_SF_SP6_soil')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_SF_SP6\\GGL_SF_SP6_PythonScripts\\Soil.html')


trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Soil Temperature, 5 cm, degree C'],
    mode='lines',
    name='5cm T',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, DC Volts'],
    mode='lines',
    name='Battv',
)


fig2 = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig2.append_trace(trace5, 1, 1)
fig2.append_trace(trace6, 2, 1)


# axis titles
fig2['layout']['yaxis1'].update(title='Deg C')
fig2['layout']['yaxis2'].update(title='Volts')
# Plot Title
fig2['layout'].update(title='GGL_SF_SP6_Battery')


plot_url = py.plot(fig2, filename='QualityZone/GGL_SF_SP6/GGL_SF_SP6_Battery')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_SF_SP6\\GGL_SF_SP6_PythonScripts\\Battery.html')

url = "/dirpath/GGL_SF_SP6.html"
webbrowser.open(url, new=2)


if click.confirm('Did you make changes to the data via the "QualityZone_working_data.csv" file?', default=False):
    df_updated = QualityZone.download_master(working_file_path)
    print(updated_frame.dtypes)
    print('dataframe replaced with updated data from Quality_Zone_working_data.csv')

if click.confirm('Save updated dataset to master .csv?', default=False):
    QualityZone.df_to_dropbox(df_updated, master_path)
    print("Master .csv uploaded to dropbox")

if click.confirm('Save updated dataset to distribute .csv?', default=False):
    dist_df = format_for_dist(df_updated)
    QualityZone.df_to_dropbox(dist_df, distribute_path)
    print("distribute .csv formatted for distribution and uploaded to dropbox")

if click.confirm('Delete temporary qc results directory?', default=False):
    shutil.rmtree('dirpath/')