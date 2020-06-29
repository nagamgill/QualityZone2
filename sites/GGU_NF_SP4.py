from sites import QualityZone, config
import os
import tempfile
import pecos
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly.io
import webbrowser
import click
from plotly.subplots import make_subplots



print("Starting QualityZone")
print("Checking Dropbox API")
print(QualityZone.dbx.users_get_current_account())

system_name = 'GGU_NF_SP4'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/GordonGulch/GGU/GGU_NF_SP4/GGU_NF_SP4_ExcelandMeta/GGU_NF_SP4_Master_WY2020.csv'
new_file = '/Toughbook_Share/GordonGulch/GGU/GGU_NF_SP4/Data/GGU_NF_SP4_CR1000_10minute.dat'
distribute_file = '/Data/GordonGulch/GGU/GGU_NF_SP4/GGU_NF_SP4_ExcelandMeta/GGU_NF_SP4_Distribute_WY2020.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

newcols = {
    'RECORD':'Record Number',
    'BattV_Avg':'Battery Voltage, minimum, Volts',
    'T107_C5_Avg':'5cm depth, Temperature, degree C',
    'T107_C50_Avg':'50cm depth, Temperature, deg C',
    'T107_C100_Avg':'100cm depth, Temperature, deg C',
    'T107_C138_Avg':'138cm depth, Temperature, deg C',
    'VW_5_Avg':'5cm depth, Volumetric Water Content, fraction',
    'PA_uS_5_Avg':'5cm depth, CS616 period average, u sec',
    'VW_50_Avg':'50cm depth, Volumetric Water Content, fraction',
    'PA_uS_50_Avg':'50cm depth, CS616 period average, u sec',
    'VW_100_Avg':'100cm depth, Volumetric Water Content, fraction',
    'PA_uS_100_Avg':'100cm depth, CS616 period average, u sec',
    'VW_138_Avg':'138cm depth, Volumetric Water Content, fraction',
    'PA_uS_138_Avg':'138cm depth, CS616 period average, u sec',

}


def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record Number',
        'Battery Voltage, minimum, Volts',
        '5cm depth, CS616 period average, u sec',
        '50cm depth, CS616 period average, u sec',
        '100cm depth, CS616 period average, u sec',
        '138cm depth, CS616 period average, u sec',
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        '5cm depth, Temperature, degree C':"TEMP(C)-5cm",
        '50cm depth, Temperature, deg C':"TEMP(C)-50cm",
        '100cm depth, Temperature, deg C':"TEMP(C)-100cm",
        '138cm depth, Temperature, deg C':"TEMP(C)-138cm",
        '5cm depth, Volumetric Water Content, fraction':"Vol Water Content-5cm",
        '50cm depth, CS616 period average, u sec':"Vol Water Content-50cm",
        '100cm depth, Volumetric Water Content, fraction':"Vol Water Content-100cm",
        '138cm depth, Volumetric Water Content, fraction':"Vol Water Content-138cm"

    }
    dfd.rename(columns=dist_columns, inplace=True)
    dfd.iloc[:, 0] = dfd.iloc[:, 0].round(2)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(2)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(2)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(2)
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
pm.check_range([12, 15.1], 'Battery Voltage, minimum, Volts')

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
    y=plotly_df['Battery Voltage, minimum, Volts'],
    mode='lines',
    name='Batt_v'
)
trace2 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['5cm depth, Temperature, degree C'],
    mode='lines',
    name='5cm T107'
)
trace3 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['50cm depth, Temperature, deg C'],
    mode='lines',
    name='50cm T107'
)
trace4 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['100cm depth, Temperature, deg C'],
    mode='lines',
    name='100cm T107'
)
trace5 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['138cm depth, Temperature, deg C'],
    mode='lines',
    name='138cm T107'
)
trace6 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['5cm depth, Volumetric Water Content, fraction'],
    mode='lines',
    name='5cm VWC'
)
trace7 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['5cm depth, CS616 period average, u sec'],
    mode='lines',
    name='5cm CS616'
)
trace8 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['50cm depth, Volumetric Water Content, fraction'],
    mode='lines',
    name='50cm VWC'
)
trace9 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['50cm depth, CS616 period average, u sec'],
    mode='lines',
    name='50cm CS616'
)
trace10 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['100cm depth, Volumetric Water Content, fraction'],
    mode='lines',
    name='100cm VWC'
)
trace12 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['100cm depth, CS616 period average, u sec'],
    mode='lines',
    name='100cm CS616'
)
trace13 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['138cm depth, Volumetric Water Content, fraction'],
    mode='lines',
    name='138cm VWC'
)
trace14 = go.Scatter(
    x=plotly_df.index,
    y=plotly_df['138cm depth, CS616 period average, u sec'],
    mode='lines',
    name='128cm CS616'
)

data = [trace2, trace3, trace4, trace5]
fig = go.Figure(data=data)

plotly.io.write_html(
    fig,
    file=os.path.join(
        config.dropbox_local + '/CZO/BcCZO/Data/GordonGulch/GGU/GGU_NF_SP4/GGU_NF_SP4_PythonScripts/GGU_NF_SP4_Temp.html'),
    auto_open=True)


fig1 = make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                            shared_xaxes=True,
                            vertical_spacing=0.001)

fig1.add_trace(trace6, 1, 1)
fig1.add_trace(trace8, 1, 1)
fig1.add_trace(trace10, 1, 1)
fig1.add_trace(trace13, 1, 1)

fig1.add_trace(trace7, 2, 1)
fig1.add_trace(trace9, 2, 1)
fig1.add_trace(trace12, 2, 1)
fig1.add_trace(trace14, 2, 1)



plotly.io.write_html(
    fig1,
    file=os.path.join(
        config.dropbox_local + '/CZO/BcCZO/Data/GordonGulch/GGU/GGU_NF_SP4/GGU_NF_SP4_PythonScripts/GGU_NF_SP4_soil.html'),
    auto_open=True)

battv = [trace1]
fig3 = go.Figure(data=battv)

plotly.io.write_html(
    fig3,
    file=os.path.join(
        config.dropbox_local + '/CZO/BcCZO/Data/GordonGulch/GGU/GGU_NF_SP4/GGU_NF_SP4_PythonScripts/GGU_NF_SP4_batt.html'),
    auto_open=True)



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

