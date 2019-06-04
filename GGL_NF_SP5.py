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


print("Starting QualityZone")
print("Checking Dropbox API")
print(QualityZone.dbx.users_get_current_account())

system_name = 'GGL_NF_SP5'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_NF_SP5/GGL_NF_SP5_M5_CR1000_ExcelandMeta/GGL_NF_SP5_WY2019_Master.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_NF_SP5_Raw/GGL_NF_SP5_CR1000_LowerGG_Pole5_10min.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_NF_SP5/GGL_NF_SP5_M5_CR1000_ExcelandMeta/GGL_NF_SP5_WY2019_Distribute.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'RECORD': 'Record Number (datalogger value)',
    'BattV_Min':'Battery Voltage, DC Volts',
    'WP_Temp_C_Avg':'Wiring Panel Temperature, Dec C',
    'T107_SM_7cm_Avg':'Snow Melt, Soil Temperature, 7 cm, degree C',
    'T107_SM_20cm_Avg':'Snow Melt, Soil Temperature, 20 cm, degree C',
    'T107_SM_50cm_Avg':'Snow Melt, Soil Temperature, 50 cm, degree C',
    'T107_SM_120cm_Avg':'Snow Melt, Soil Temperature, 120 cm, degree C',
    'T107_SR_5cm_Avg':'Simulated Rain, Soil Temperature, 5 cm, degree C',
    'T107_SR_10cm_Avg':'Simulated Rain, Soil Temperature, 10 cm, degree C',
    'T107_SR_25cm_Avg':'Simulated Rain, Soil Temperature, 25 cm, degree C',
    'T107_SR_60cm_Avg':'Simulated Rain, Soil Temperature, 60 cm, degree C',
    'VW_SM_7cm_Avg':'Snow Melt, Volumetric Water Content, 7 cm, fraction',
    'VW_SM_20cm_Avg':'Snow Melt, Soil Moisture, Volumetric Water Content, 20 cm, fraction',
    'VW_SM_50cm_Avg':'Snow Melt, Soil Moisture, Volumetric Water Content, 50 cm, fraction',
    'VW_SM_120cm_Avg':'Snow Melt, Soil Moisture, Volumetric Water Content, 120 cm, fraction',
    'VW_SR_5cm_Avg':'Simulated Rain, Soil Moisture, Volumetric Water Content, 5 cm, fraction',
    'VW_SR_10cm_Avg':'Simulated Rain, Soil Moisture, Volumetric Water Content, 10 cm, fraction',
    'VW_SR_25cm_Avg':'Simulated Rain, Soil Moisture, Volumetric Water Content, 25 cm, fraction',
    'VW_SR_60cm_Avg':'Simulated Rain, Soil Moisture, Volumetric Water Content, 60 cm, fraction',
    'PA_uS_SM_7cm_Avg':'Snow Melt, Soil Moisture, CS616 period average,7 cm, u sec',
    'PA_uS_SM_20cm_Avg':'Snow Melt, Soil Moisture, CS616 period average,20 cm, u sec',
    'PA_uS_SM_50cm_Avg':'Snow Melt, Soil Moisture, CS616 period average, 50 cm, u sec',
    'PA_uS_SM_120cm_Avg':'Snow Melt, Soil Moisture, CS616 period average, 120 cm, u sec',
    'PA_uS_SR_5cm_Avg':'Simulated Rain, Soil Moisture, CS616 period average, 5 cm, u sec',
    'PA_uS_SR_10cm_Avg':'Simulated Rain, Soil Moisture, CS616 period average, 10 cm, u sec',
    'PA_uS_SR_25cm_Avg':'Simulated Rain, Soil Moisture, CS616 period average, 25 cm, u sec',
    'PA_uS_SR_60cm_Avg':'Simulated Rain, Soil Moisture, CS616 period average, 60 cm, u sec',

}

def format_for_dist(dataframe):
    df = dataframe.copy()
    df = df.drop(['Record Number (datalogger value)',
                    'Battery Voltage, DC Volts',
                    'Wiring Panel Temperature, Dec C',
                    'Snow Melt, Soil Moisture, CS616 period average,7 cm, u sec',
                    'Snow Melt, Soil Moisture, CS616 period average,20 cm, u sec',
                    'Snow Melt, Soil Moisture, CS616 period average, 50 cm, u sec',
                    'Snow Melt, Soil Moisture, CS616 period average, 120 cm, u sec',
                    'Simulated Rain, Soil Moisture, CS616 period average, 5 cm, u sec',
                    'Simulated Rain, Soil Moisture, CS616 period average, 10 cm, u sec',
                    'Simulated Rain, Soil Moisture, CS616 period average, 25 cm, u sec',
                    'Simulated Rain, Soil Moisture, CS616 period average, 60 cm, u sec'
                  ], axis=1)

    dist_columns = {
        'Snow Melt, Soil Temperature, 7 cm, degree C':'SM-TEMP(C)-7CM',
        'Snow Melt, Soil Temperature, 20 cm, degree C':'SM-TEMP(C)-20CM',
        'Snow Melt, Soil Temperature, 50 cm, degree C':'SM-TEMP(C)-50CM',
        'Snow Melt, Soil Temperature, 120 cm, degree C':'M-TEMP(C)-120CM',
        'Simulated Rain, Soil Temperature, 5 cm, degree C':'SR-TEMP(C)-5CM',
        'Simulated Rain, Soil Temperature, 10 cm, degree C':'SR-TEMP(C)-10CM',
        'Simulated Rain, Soil Temperature, 25 cm, degree C':'SR-TEMP(C)-25CM',
        'Simulated Rain, Soil Temperature, 60 cm, degree C':'SR-TEMP(C)-60CM',
        'Snow Melt, Volumetric Water Content, 7 cm, fraction':'SM-VOL WATER CONTENT-7CM',
        'Snow Melt, Soil Moisture, Volumetric Water Content, 20 cm, fraction':'SM-VOL WATER CONTENT-20CM',
        'Snow Melt, Soil Moisture, Volumetric Water Content, 50 cm, fraction':'SM-VOL WATER CONTENT-50CM',
        'Snow Melt, Soil Moisture, Volumetric Water Content, 120 cm, fraction':'SM-VOL WATER CONTENT-120CM',
        'Simulated Rain, Soil Moisture, Volumetric Water Content, 5 cm, fraction':'SR-VOL WATER CONTENT-5CM',
        'Simulated Rain, Soil Moisture, Volumetric Water Content, 10 cm, fraction':'SR-VOL WATER CONTENT-10CM',
        'Simulated Rain, Soil Moisture, Volumetric Water Content, 25 cm, fraction':'SR-VOL WATER CONTENT-25CM',
        'Simulated Rain, Soil Moisture, Volumetric Water Content, 60 cm, fraction':'SR-VOL WATER CONTENT-60CM',
    }
    df.rename(columns=dist_columns, inplace=True)
    df.iloc[:,0:8] = df.iloc[:,0:8].round(2)
    df.iloc[:,9:16] = df.iloc[:, 9:16].round(3)

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
    y=plotly_df['Simulated Rain, Soil Temperature, 5 cm, degree C'],
    mode='lines',
    name='SR 5cm T107',
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Temperature, 10 cm, degree C'],
    mode='lines',
    name='SR 10cm T107',
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Temperature, 25 cm, degree C'],
    mode='lines',
    name='SR 25cm T107',
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Temperature, 60 cm, degree C'],
    mode='lines',
    name='SR 60cm T107',
)



trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, Volumetric Water Content, 5 cm, fraction'],
    mode='lines',
    name='SR 5cm VWC',
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, Volumetric Water Content, 10 cm, fraction'],
    mode='lines',
    name='SR 10cm VWC',
)
trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, Volumetric Water Content, 25 cm, fraction'],
    mode='lines',
    name='SR 25cm VWC',
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, Volumetric Water Content, 60 cm, fraction'],
    mode='lines',
    name='SR 60cm VWC',
)




trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, CS616 period average, 5 cm, u sec'],
    mode='lines',
    name='SR 5cm CS616',
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, CS616 period average, 10 cm, u sec'],
    mode='lines',
    name='SR 10cm CS616',
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, CS616 period average, 25 cm, u sec'],
    mode='lines',
    name='SR 25cm CS616',
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, Soil Moisture, CS616 period average, 60 cm, u sec'],
    mode='lines',
    name='SR 60cm CS616',
)
trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Temperature, 7 cm, degree C'],
    mode='lines',
    name='SM 7cm T107',
)
trace14 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Temperature, 20 cm, degree C'],
    mode='lines',
    name='SM 20cm T107',
)
trace15 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Temperature, 50 cm, degree C'],
    mode='lines',
    name='SM 50cm T107',
)
trace16 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Temperature, 120 cm, degree C'],
    mode='lines',
    name='SM 120cm T107',
)



trace17 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Volumetric Water Content, 7 cm, fraction'],
    mode='lines',
    name='SM 7cm VWC',
)
trace18 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, Volumetric Water Content, 20 cm, fraction'],
    mode='lines',
    name='SM 20cm VWC',
)
trace19 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, Volumetric Water Content, 50 cm, fraction'],
    mode='lines',
    name='SM 50cm VWC',
)
trace20 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, Volumetric Water Content, 120 cm, fraction'],
    mode='lines',
    name='SM 120cm VWC',
)

trace21 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, CS616 period average,7 cm, u sec'],
    mode='lines',
    name='SM 7cm CS616',
)
trace22 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, CS616 period average,20 cm, u sec'],
    mode='lines',
    name='SM 20cm CS616',
)
trace23 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, CS616 period average, 50 cm, u sec'],
    mode='lines',
    name='SM 50cm CS616',
)
trace24 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, Soil Moisture, CS616 period average, 120 cm, u sec'],
    mode='lines',
    name='SM 120cm CS616',
)

fig = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)

fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 1, 1)
fig.append_trace(trace3, 1, 1)
fig.append_trace(trace4, 1, 1)

fig.append_trace(trace5, 2, 1)
fig.append_trace(trace6, 2, 1)
fig.append_trace(trace7, 2, 1)
fig.append_trace(trace8, 2, 1)

fig.append_trace(trace9, 3, 1)
fig.append_trace(trace10, 3, 1)
fig.append_trace(trace11, 3, 1)
fig.append_trace(trace12, 3, 1)


# axis titles
fig['layout']['yaxis1'].update(title='Deg C')
fig['layout']['yaxis2'].update(title='VWC')
fig['layout']['yaxis3'].update(title='CS616')
# Plot Title
fig['layout'].update(title='GGL_NF_SP5_Simulated_Rain')


plot_url = py.plot(fig, filename='QualityZone/GGL_NF_SP5/GGL_NF_SP5_sr')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP5\\GGL_NF_SP5_Python\\Soil_SR.html')



fig2 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)

fig2.append_trace(trace13, 1, 1)
fig2.append_trace(trace14, 1, 1)
fig2.append_trace(trace15, 1, 1)
fig2.append_trace(trace16, 1, 1)

fig2.append_trace(trace17, 2, 1)
fig2.append_trace(trace18, 2, 1)
fig2.append_trace(trace19, 2, 1)
fig2.append_trace(trace20, 2, 1)

fig2.append_trace(trace21, 3, 1)
fig2.append_trace(trace22, 3, 1)
fig2.append_trace(trace23, 3, 1)
fig2.append_trace(trace24, 3, 1)

# axis titles
fig2['layout']['yaxis1'].update(title='Deg C')
fig2['layout']['yaxis2'].update(title='VWC')
fig2['layout']['yaxis3'].update(title='CS616')
# Plot Title
fig2['layout'].update(title='GGL_NF_SP5_Snow_Melt')


plot_url = py.plot(fig2, filename='QualityZone/GGL_NF_SP5/GGL_NF_SP5_sm')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\GordonGulch\\GGL\\GGL_NF_SP5\\GGL_NF_SP5_Python\\Soil_SM.html')


trace25 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, DC Volts'],
    mode='lines',
    name='Battery Voltage',
)
battv = [trace25]
plot_url = py.plot(battv, filename = 'QualityZone/GGL_NF_SP5/Battery_Voltage_GGL_NF_SP5')




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
