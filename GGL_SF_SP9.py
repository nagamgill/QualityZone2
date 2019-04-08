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

system_name = 'GGL_SF_SP9'
dropbox_base = '/Boulder Creek CZO Team Folder/BcCZO'
master_file = '/Data/GordonGulch/GGL/GGL_SF_SP9/GGL_SF_SP9_ExcelandMeta/GGL_SF_SP9_Master_WY2019.csv'
new_file = '/Toughbook_Share/GordonGulch/GGL/Data/GGL_SF_SP9_Raw/GGL_SF_SP9_Tenmin.dat'
distribute_file = '/Data/GordonGulch/GGL/GGL_SF_SP9/GGL_SF_SP9_ExcelandMeta/GGL_SF_SP9_Distribute_WY2019.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)


newcols = {
    'RECORD':'Record Number',
    'BattV_Min':'Bat Volt (minimum)',
    'PTemp_C_Avg':'Panel temperature (average)',
    'VW_5cm_SR_Avg':'Simulated Rain, 5 cm, Volumetric Water Content, fraction',
    'PA_uS_Avg':'Simulated Rain, 5 cm, CS616 period average, u sec',
    'VW_22cm_SR_Avg':'Simulated Rain, 22 cm, Volumetric Water Content, fraction',
    'PA_uS_2_Avg':'Simulated Rain, 22 cm, CS616 period average, u sec',
    'T107_C_5cm_SR_Avg':'Simulated Rain, 5 cm, Temperature, degree C',
    'T107_C_22cm_SR_Avg':'Simulated Rain, 22 cm, Temperature, degree C',
    'VW_5cm_SM_Avg':'Snow Melt, 5 cm, Volumetric Water Content, fraction',
    'PA_uS_5cm_SM_Avg':'Snow Melt, 5 cm, CS616 period average, u sec',
    'VW_20cm_SM_Avg':'Snow Melt, 20 cm, Volumetric Water Content, fraction',
    'PA_uS_20cm_SM_Avg':'Snow Melt, 20 cm, CS616 period average, u sec',
    'T107_C_5cm_SM_Avg':'Snow Melt, 5 cm, Temperature, degree C',
    'T107_C_20cm_SM_Avg':'Snow Melt, 20 cm, Temperature, degree C',
}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record Number',
        'Bat Volt (minimum)',
        'Panel temperature (average)',
        'Simulated Rain, 5 cm, CS616 period average, u sec',
        'Simulated Rain, 22 cm, CS616 period average, u sec',
        'Snow Melt, 5 cm, CS616 period average, u sec',
        'Snow Melt, 20 cm, CS616 period average, u sec',
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        'Simulated Rain, 5 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-5CM',
        'Simulated Rain, 22 cm, Volumetric Water Content, fraction':'SR-VOL WATER CONTENT-22CM',
        'Simulated Rain, 5 cm, Temperature, degree C':'SR-TEMP(C)-5CM',
        'Simulated Rain, 22 cm, Temperature, degree C':'SR-TEMP(C)-22CM',
        'Snow Melt, 5 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-5CM',
        'Snow Melt, 20 cm, Volumetric Water Content, fraction':'SM-VOL WATER CONTENT-20CM',
        'Snow Melt, 5 cm, Temperature, degree C':'SM-TEMP(C)-5CM',
        'Snow Melt, 20 cm, Temperature, degree C':'SM-TEMP(C)-20CM'
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
pm.check_missing(min_failures=5)
pm.check_corrupt([-6999, 'NAN'])
pm.check_corrupt(['NaN', 'Na', 'na', 'np.nan', 'nan'])
pm.check_corrupt(corrupt_values=['nan','NAN'])
pm.check_range([12, 15.1], 'Bat Volt (minimum)')

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
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 5cm CS616 u sec'
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 5cm CS616 VWC %',
)
trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SR 5cm T107 T',
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 22 cm, CS616 period average, u sec'],
    mode='lines',
    name='SR 22cm CS616 u sec',
)
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Simulated Rain, 22 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SR 22cm CS616 VWC %'
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, CS616 period average, u sec'],
    mode='lines',
    name='SM 5cm CS616 u sec',
)
trace11 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Volumetric Water Content, fraction'],
    mode='lines',
    name='SM 5cm CS616 VWC %',
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Snow Melt, 5 cm, Temperature, degree C'],
    mode='lines',
    name='SM 5cm T107'
)
trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Bat Volt (minimum)'],
    mode='lines',
    name='Battery Voltage'
)
trace14 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['Bat Volt (minimum)'],
    mode='lines',
    name='Battery Voltage'
)

fig2 = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig2.append_trace(trace5, 1, 1)
fig2.append_trace(trace6, 3, 1)
fig2.append_trace(trace7, 2, 1)
fig2.append_trace(trace8, 1, 1)
fig2.append_trace(trace9, 3, 1)
fig2.append_trace(trace10, 1, 1)
fig2.append_trace(trace11, 3, 1)
fig2.append_trace(trace12, 2, 1)


#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig2, filename='GGL_SF_SP9')

# -------------------------------------------------------------------------------

fig = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace13, 1, 1)
fig.append_trace(trace14, 2, 1)


# axis titles
fig['layout']['yaxis1'].update(title='Deg C')
fig['layout']['yaxis2'].update(title='Volts')
# Plot Title
fig['layout'].update(title='GGL_SF_SP9 Battery')


plot_url = py.plot(fig, filename='GGL_SF_SP9_Battery')





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


