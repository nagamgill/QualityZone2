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

system_name = 'GGL_NF_SP4'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_NF_SP4/GGL_NF_SP4_CR1000_ExcelandMeta/GGL_NF_SP4_CR1000_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_NF_SP4_Raw/GGL_NF_SP4_GGL_NF_SP4_10min.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_NF_SP4/GGL_NF_SP4_CR1000_ExcelandMeta/GGL_NF_SP4_CR1000_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'RECORD': 'Record Number (datalogger value)',
    'Batt_Volt': 'Battery Voltage, Minimum, DC Volts',
    'WP_Temp_C': 'Wiring Panel Temperature, Average, Deg C',
    'SR_SoilTemp_C_5cm': 'Simulated Rain, 5 cm, Temperature, degree C',
    'SR_VWC_CS616_5cm': 'Simulated Rain, 5 cm, Volumetric Water Content, fraction',
    'SR_PA_uS_5cm': 'Simulated Rain, 5 cm, CS616 period average, u sec',
    'SR_SoilTemp_C_25cm': 'Simulated Rain, 25 cm, Temperature, degree C',
    'SR_VWC_CS616_25cm': 'Simulated Rain, 25 cm, Volumetric Water Content, fraction',
    'SR_PA_uS_25cm': 'Simulated Rain, 25 cm, CS616 period average, u sec',
    'SM_SoilTemp_C_5cm': 'Snow Melt, 5 cm, Temperature, degree C',
    'SM_VWC_CS616_5cm': 'Snow Melt, 5 cm, Volumetric Water Content, fraction',
    'SM_PA_uS_5cm': 'Snow Melt, 5 cm, CS616 period average, u sec',
    'SM_SoilTemp_C_20cm': 'Snow Melt, 20 cm, Temperature, degree C',
    'SM_VWC_CS616_20cm': 'Snow Melt, 20 cm, Volumetric Water Content, fraction',
    'SM_PA_uS_20cm': 'Snow Melt, 20 cm, CS616 period average, u sec',

    'TempC_GGSD_6': 'GGSD_6, Temperature, deg C',
    'Time_GGSD_6': 'GGSD_6, Travel Time, 10*us',
    'Depth_GGSD_6': 'GGSD_6, Distance, cm',
    'Retries_GGSD_6': 'GGSD_6, Retries, number',

    'TempC_GGSD_7': 'GGSD_7, Temperature, deg C',
    'Time_GGSD_7': 'GGSD_7 Travel Time, 10*us',
    'Depth_GGSD_7': 'GGSD_7, Distance, cm',
    'Retries_GGSD_7': 'GGSD_7, Retries, number',

    'TempC_GGSD_5': 'GGSD_5, Temperature, deg C',
    'Time_GGSD_5': 'GGSD_5, Travel Time, 10*us',
    'Depth_GGSD_5': 'GGSD_5, Distance, cm',
    'Retries_GGSD_5': 'GGSD_5, Retries, number',

}


def format_for_dist(dataframe):
    df = dataframe.copy()
    df = df.drop(['Record Number (datalogger value)',
                            'Battery Voltage, Minimum, DC Volts',
                            'Wiring Panel Temperature, Average, Deg C',
                            'Simulated Rain, 5 cm, CS616 period average, u sec',
                            'Simulated Rain, 25 cm, CS616 period average, u sec',
                            'Snow Melt, 5 cm, CS616 period average, u sec',
                            'Snow Melt, 20 cm, CS616 period average, u sec',
                            'GGSD_6, Temperature, deg C',
                            'GGSD_6, Travel Time, 10*us',
                            'GGSD_6, Distance, cm',
                            'GGSD_6, Retries, number',
                            'GGSD_7, Temperature, deg C',
                            'GGSD_7 Travel Time, 10*us',
                            'GGSD_7, Distance, cm',
                            'GGSD_7, Retries, number',
                            'GGSD_5, Temperature, deg C',
                            'GGSD_5, Travel Time, 10*us',
                            'GGSD_5, Distance, cm',
                            'GGSD_5, Retries, number',
                           ], axis=1)

    dist_columns = {
        'Simulated Rain, 5 cm, Temperature, degree C':'SR-TEMP(C)-5CM',
        'Simulated Rain, 5 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-5CM',
        'Simulated Rain, 25 cm, Temperature, degree C':'SR-TEMP(C)-25CM',
        'Simulated Rain, 25 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-25CM',
        'Snow Melt, 5 cm, Temperature, degree C':'SM-TEMP(C)-5CM',
        'Snow Melt, 5 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-5CM',
        'Snow Melt, 20 cm, Temperature, degree C':'SM-TEMP(C)-20CM',
        'Snow Melt, 20 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-20CM'
    }
    df.rename(columns=dist_columns, inplace=True)
    df.iloc[:, 0] = df.iloc[:, 0].round(2)
    df.iloc[:, 1] = df.iloc[:, 1].round(3)
    df.iloc[:, 2] = df.iloc[:, 2].round(2)
    df.iloc[:, 3] = df.iloc[:, 3].round(3)
    df.iloc[:, 4] = df.iloc[:, 4].round(2)
    df.iloc[:, 5] = df.iloc[:, 5].round(3)
    df.iloc[:, 6] = df.iloc[:, 6].round(2)
    df.iloc[:, 7] = df.iloc[:, 7].round(3)

    df = df.fillna('')
    return df

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
pm.check_range([12, 15.1], 'Battery Voltage, Minimum, DC Volts')

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
    y=plotly_df['Simulated Rain, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 5cm VWC',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 5cm CS616',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SR 5cm T',
)


trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 25cm VWC',
)
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 25cm CS616',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 25 cm, Temperature, degree C'],
    mode='lines',
    name='SR 25cm T',
)



trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 5cm VWC',
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SM 5cm CS616',
)
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SM 5cm T',
)



trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 20 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 20cm VWC',
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 20 cm, CS616 period average, u sec'],
    mode='lines',
    name='SM 20cm CS616',
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 20 cm, Temperature, degree C'],
    mode='lines',
    name='SM 20cm T',
)


trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_5, Distance, cm'],
    mode='lines',
    name='GGSD 5',
)
trace14 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_6, Distance, cm'],
    mode='lines',
    name='GGSD 6',
)
trace15 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['GGSD_7, Distance, cm'],
    mode='lines',
    name='GGSD_7',
)
trace16 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, Minimum, DC Volts'],
    mode='lines',
    name='Batt Volts',
)




fig = tools.make_subplots(rows=6, cols=1, specs=[[{}], [{}], [{}], [{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 2, 1)
fig.append_trace(trace3, 3, 1)
fig.append_trace(trace4, 1, 1)
fig.append_trace(trace5, 2, 1)
fig.append_trace(trace6, 3, 1)

fig.append_trace(trace7, 4, 1)
fig.append_trace(trace8, 5, 1)
fig.append_trace(trace9, 6, 1)
fig.append_trace(trace10, 4, 1)
fig.append_trace(trace11, 5, 1)
fig.append_trace(trace12, 6, 1)
# axis titles
fig['layout']['yaxis1'].update(title='VWC Fraction')
fig['layout']['yaxis2'].update(title='Deg C')
# Plot Title
fig['layout'].update(title='GGL_NF_SP4_Soil')


plot_url = py.plot(fig, filename='1')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP4\\GGL_NF_SP4_Python\\Soil.html')



fig2 = tools.make_subplots(rows=4, cols=1, specs=[[{}], [{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)

fig2.append_trace(trace13, 1, 1)
fig2.append_trace(trace14, 2, 1)
fig2.append_trace(trace15, 3, 1)
fig2.append_trace(trace16, 4, 1)


# Plot Title
fig2['layout'].update(title='GGU_NF_SP4_Snow')


plot_url = py.plot(fig2, filename='2')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP4\\GGL_NF_SP4_Python\\Snow_battery.html')



#----------------------------------------------------------------------------------



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