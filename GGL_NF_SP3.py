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

system_name = 'GGL_NF_SP3'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_NF_SP3/GGL_NF_SP3__CR10x_Excelandmeta/GGL_NF_SP3_CR10x_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_NF_SP3_Raw/GGL_NF_SP3_CR10xPB_15.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_NF_SP3/GGL_NF_SP3__CR10x_Excelandmeta/GGL_NF_SP3_CR10x_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)
# not sure if this is neccesary with the .CSV system, needs testing
master_index = ''

newcols = {
    'RECORD': 'Record Number',
    'BattVolt': 'Battery Voltage, DC Volts',
    'SM_PA5cm': 'Snow Melt, 5 cm, CS616 period average, u sec',
    'SM_PA25cm': 'Snow Melt, 25 cm, CS616 period average, u sec',
    'SM_VWC5cm': 'Snow Melt, 5 cm, Volumetric Water Content, fraction',
    'SM_VWC25c': 'Snow Melt, 25 cm, Volumetric Water Content, fraction',
    'SM_T_5cm': 'Snow Melt, 5 cm, Temperature, degree C',
    'SM_T_20cm': 'Snow Melt, 25 cm, Temperature, degree C',
    'SR_PA5cm': 'Simulated Rain, 5 cm, CS616 period average, u sec',
    'SR_PA19c': 'Simulated Rain, 19 cm, CS616 period average, u sec',
    'SR_VWC5cm': 'Simulated Rain, 5 cm, Volumetric Water Content, fraction',
    'SR_VWC19c': 'Simulated Rain, 19 cm, Volumetric Water Content, fraction',
    'SR_T_5cm': 'Simulated Rain, 5 cm, Temperature, degree C',
    'SR_T_19cm': 'Simulated Rain, 19 cm, Temperature, degree C',
    'ProgSig': 'Program Signature, (datalogger value)',

    'GGSD3_TempC': 'GGSD_3, Temperature, deg C',
    'GGSD3_Time': 'GGSD_3, Travel Time, 10*us',
    'GGSD3_Dist_cm': 'GGSD_3, Distance, cm',
    'GGSD3_Retry': 'GGSD_3, Retries, number',

    'GGSD2_TempC': 'GGSD_2, Temperature, deg C',
    'GGSD2_Time': 'GGSD_2, Travel Time, 10*us',
    'GGSD2_Dist_cm': 'GGSD_2, Distance, cm',
    'GGSD2_Retry': 'GGSD_2, Retries, number',

    'GGSD1_TempC': 'GGSD_1, Temperature, deg C',
    'GGSD1_Time': 'GGSD_1, Travel Time, 10*us',
    'GGSD1_Dist_cm': 'GGSD_1, Distance, cm',
    'GGSD1_Retry': 'GGSD_1, Retries, number',

    'GGSD4_TempC': 'GGSD_4, Temperature, deg C',
    'GGSD4_Time': 'GGSD_4, Travel Time, 10*us',
    'GGSD4_Dist_cm': 'GGSD_4, Distance, cm',
    'GGSD4_Retry': 'GGSD_4, Retries, number',
}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record Number',
        'Battery Voltage, DC Volts',
        'Snow Melt, 5 cm, CS616 period average, u sec',
        'Snow Melt, 25 cm, CS616 period average, u sec',
        'Simulated Rain, 5 cm, CS616 period average, u sec',
        'Simulated Rain, 19 cm, CS616 period average, u sec',
        'Program Signature, (datalogger value)',

        'GGSD_3, Temperature, deg C',
        'GGSD_3, Travel Time, 10*us',
        'GGSD_3, Distance, cm',
        'GGSD_3, Retries, number',

        'GGSD_2, Temperature, deg C',
        'GGSD_2, Travel Time, 10*us',
        'GGSD_2, Distance, cm',
        'GGSD_2, Retries, number',

        'GGSD_1, Temperature, deg C',
        'GGSD_1, Travel Time, 10*us',
        'GGSD_1, Distance, cm',
        'GGSD_1, Retries, number',

        'GGSD_4, Temperature, deg C',
        'GGSD_4, Travel Time, 10*us',
        'GGSD_4, Distance, cm',
        'GGSD_4, Retries, number',
        ], axis=1)

    dist_columns = {
        'Snow Melt, 5 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-5CM',
        'Snow Melt, 25 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-25CM',
        'Snow Melt, 5 cm, Temperature, degree C':'SM-TEMP(C)-5CM',
        'Snow Melt, 25 cm, Temperature, degree C':'SM-TEMP(C)-25CM',
        'Simulated Rain, 5 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-5CM',
        'Simulated Rain, 19 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-19CM',
        'Simulated Rain, 5 cm, Temperature, degree C':'SR-TEMP(C)-5CM',
        'Simulated Rain, 19 cm, Temperature, degree C':'SR-TEMP(C)-19CM'
    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(2)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(2)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(2)
    dfd.iloc[:, 7] = dfd.iloc[:, 7].round(2)

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
pm.check_range([12, 15.1], 'Battery Voltage, DC Volts')

mask = pm.get_test_results_mask()
QCI = pecos.metrics.qci(mask, pm.tfilter)




dirpath = tempfile.mkdtemp()
#with tempfile.TemporaryDirectory() as dirpath:

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
    y=plotly_df['Snow Melt, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 5cm VWC',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 25 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 25cm VWC',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SM 5cm T107',
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 25 cm, Temperature, degree C'],
    mode='lines',
    name='SM 25cm T107',
)
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 5cm VWC',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SR 5cm T107',
)
trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 19 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 19cm VWC',
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 19 cm, Temperature, degree C'],
    mode='lines',
    name='SR 19cm VWC',
)
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_1, Distance, cm'],
    mode='lines',
    name='GGSD 1',
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_2, Distance, cm'],
    mode='lines',
    name='GGSD 2',
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_3, Distance, cm'],
    mode='lines',
    name='GGSD 3',
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_4, Distance, cm'],
    mode='lines',
    name='GGSD 4',
)



fig = tools.make_subplots(rows=4, cols=1, specs=[[{}], [{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 1)
fig.append_trace(trace3, 2, 1)
fig.append_trace(trace4, 2, 1)
fig.append_trace(trace5, 3, 1)
fig.append_trace(trace6, 4, 1)
fig.append_trace(trace7, 3, 1)
fig.append_trace(trace8, 4, 1)



# axis titles
fig['layout']['yaxis1'].update(title='VWC Fraction')
fig['layout']['yaxis2'].update(title='Deg C')
# Plot Title
fig['layout'].update(title='GGU_NF_SP3_Soil')


plot_url = py.plot(fig, filename='GGU_NF_SP3_Soil')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP3\\GGL_NF_SP3_PythonScripts\\Soil.html')

#--------------Snow Plot-----------------------------------------------------------------

data = [trace9, trace10, trace11, trace12]

plot_url = py.plot(data, filename='Snow')
#plotly.offline.plot(data, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP3\\GGL_NF_SP3_PythonScripts\\Snow.html')


#----------------------------------------------------------------------------------


if click.confirm('Did you make changes to the data via the "QualityZone_working_data.csv" file?', default=False):
    df_updated = QualityZone.download_master(working_file_path)
    #print(working_file_path.dtypes)
    print('dataframe replaced with updated data from Quality_Zone_working_data.csv')

if click.confirm('Save updated dataset to master .csv?', default=False):
    QualityZone.df_to_dropbox(df_updated, master_path)
    print("Master .csv uploaded to dropbox")

if click.confirm('Save updated dataset to distribute .csv?', default=False):
    dist_df = format_for_dist(df_updated)
    QualityZone.df_to_dropbox(dist_df, distribute_path)
    print("distribute .csv formatted for distribution and uploaded to dropbox")

