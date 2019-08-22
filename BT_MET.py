import QualityZone
import os
from io import StringIO
import tempfile
import pecos
import matplotlib as plt
import matplotlib.pyplot as plt
import plotly.plotly as py
import plotly.graph_objs as go
import plotly
from plotly import tools
import webbrowser
import click
import shutil



print("Starting QualityZone")
print("Checking Dropbox API")
print(QualityZone.dbx.users_get_current_account())

system_name = 'BT_MET'
dropbox_base = '/CZO/BcCZO'
master_file = '/Data/Betasso/BetassoMet/QA_QC/BT_Met_ExcelandMeta/BetMet_WY2019_Master.csv'
new_file = '/Toughbook_Share/Betasso/Bet_Met/BetMet_Data/Betasso_Remote_CR1000_BetMet10.dat'
distribute_file = '/Data/Betasso/BetassoMet/QA_QC/BT_Met_ExcelandMeta/BetMet_WY2019_Distribute.csv'
master_path = os.path.join(dropbox_base + master_file)
new_path = os.path.join(dropbox_base + new_file)
distribute_path = os.path.join(dropbox_base + distribute_file)

newcols = {
    'RECORD': 'Record Number (datalogger value)',
    'Batt_Volt_Min':'Battery Voltage, Minimum, DC Volts',
    'Batt_Volt_TMn':'Time of Battery Voltage, Minimum',
    'LoggerPanelTemp_Avg':'Wiring Panel Temperature, Average, Deg C',
    'BaroPress_Avg':'barometric pressure, uncorrected to msl, mbar',
    'Rain_mm_Tot':'tipping bucket rain gage, total, mm',
    'SoilTmp_15cm_Avg':'soil temperature, 15cm depth, average, Degrees C',
    'VWC_CS616_15cm_Avg':'soil moisture 15cm depth, average, fraction',
    'PA_CS616_15cm_Avg':'soil moisture, 15cm depth, average, period average',
    'SoilHeatFlux_Avg':'soil heat flux, average, 15cm depth, watts/m^2',
    'AirTemp_2m_Avg':'air temperature, average, 2m elevation, Degrees C',
    'AirTemp_2m_Min':'air temperature, minimum, 2m elevation, Degrees C',
    'AirTemp_2m_TMn':'time of air temperature minimum, 2m elevation, Degrees C',
    'AirTemp_2m_Max':'air temperature, maximum, 2m elevation, Degrees C',
    'AirTemp_2m_TMx':'time of air temperature, maximum, 2m elevation, Degrees C',
    'RH_2m_Avg':'relative humidity, average, 2m elevation, percent',
    'RH_2m_Min':'relative humidity, minimum, 2m elevation, percent',
    'RH_2m_TMn':'time of time of relative humidity minimum, 2m elevation, percent',
    'RH_2m_Max':'relative humidity, maximum, 2m elevation, percent',
    'RH_2m_TMx':'time of relative humidity maximum, 2m elevation, percent',
    'WindSpeed_2m_Min':'Wind Speed, minimum, 2m elevation, meters/sec',
    'WindSpeed_2m_TMn':'Time of wind speed minimum, 2m elevation',
    'WindSpeed_2m_Max':'Wind Speed, maximum, 2m elevation, meters/sec',
    'WindSpeed_2m_TMx':'Time of wind speed maximum, 2m elevation',
    'WindSpeed_2m_avg_WVT':'Wind Speed, Average, 2m elevation, meters/sec',
    'WindDir_2m_ave_WVT':'Wind Direction, average, 2m elevation, Degrees',
    'WindDir_2m_SD1_WVT':'Standard Deviation of Wind Direction, 2m elevation, Degrees ',
    'SlrRad_instant_Avg':'Incoming shortwave radiation, Average, 5m elevation, watts/m^2',
    'SlrRad_instant_Min':'Incoming shortwave radiation, minimum, 5m elevation, watts/m^2',
    'SlrRad_instant_TMn':'Time of incoming shortwave radiation minimum, 5m elevation',
    'SlrRad_instant_Max':'incoming shortwave radiation, maximum, 5m elevation, watts/m^2',
    'SlrRad_instant_TMx':'Time of incoming shortwave radiation maximum, 5m elevation',
    'SlrRad_flux_Tot':'Incoming shortwave radiation, total, 5m elevation, MJoule/m^2',
    'NetRad_Avg':'Net Radiation, average, 5m elevation, watts/m^2',
    'CorrectedNetRad_Avg':'Net Radiation, average, corrected to wind speed, 5m elevation, watts/m^2',
    'AirTemp_10m_Avg':'air temperature, average, 10m elevation, Degrees C',
    'AirTemp_10m_Min':'air temperature, minimum, 10m elevation, Degrees C',
    'AirTemp_10m_TMn':'time of air temperature minimum, 10m elevation, Degrees C',
    'AirTemp_10m_Max':'air temperature, maximum, 10m elevation, Degrees C',
    'AirTemp_10m_TMx':'time of air temperature, maximum, 10m elevation, Degrees C',
    'RH_10m_Avg':'relative humidity, average, 10m elevation, percent',
    'RH_10m_Min':'relative humidity, minimum, 10m elevation, percent',
    'RH_10m_TMn':'time of relative humidity minimum, 10m elevation, percent',
    'RH_10m_Max':'relative humidity, maximum, 10m elevation, percent',
    'RH_10m_TMx':'time of relative humidity maximum, 10m elevation, percent',
    'WindSpeed_10m_Min':'Wind Speed, minimum, 10m elevation, meters/sec',
    'WindSpeed_10m_TMn':'Time of wind speed minimum, 10m elevation',
    'WindSpeed_10m_Max':'Wind Speed, maximum, 10m elevation, meters/sec',
    'WindSpeed_10m_TMx':'Time of wind speed maximum, 10m elevation',
    'WindSpeed_10m_avg_WVT':'Wind Speed, Average, 10m elevation, meters/sec',
    'WindDir_10m_avg_WVT':'Wind Direction, average, 10m elevation, Degrees',
    'WindDir_10m_SD1_WVT':'Standard Deviation of Wind Direction, 10m elevation, Degrees ',
}



def format_for_dist(dataframe):
    dfd = dataframe.copy()
    dfd = dfd.drop([
        'Record Number (datalogger value)',
        'Battery Voltage, Minimum, DC Volts',
        'Time of Battery Voltage, Minimum',
        'Wiring Panel Temperature, Average, Deg C',
        'soil moisture, 15cm depth, average, period average'
        ], axis=1)

    dfd = dfd.fillna('')
    dist_columns = {
        'barometric pressure, uncorrected to msl, mbar':'BAROMETRIC PRESS(MBAR)',
        'tipping bucket rain gage, total, mm':'RAIN GAGE(MM)',
        'soil temperature, 15cm depth, average, Degrees C':'SOIL TEMP(C)-15CM',
        'soil moisture 15cm depth, average, fraction':'SOIL VOL WATER CONTENT(%)-15CM',
        'soil heat flux, average, 15cm depth, watts/m^2':'SOIL HEAT FLUX(W/m^2)-15CM',
        'air temperature, average, 2m elevation, Degrees C':'AIRTEMP(C)-2M(AVG)',
        'air temperature, minimum, 2m elevation, Degrees C':'AIRTEMP(C)-2M(MIN)',
        'time of air temperature minimum, 2m elevation, Degrees C':'AIRTEMP-2M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'air temperature, maximum, 2m elevation, Degrees C':'AIRTEMP(C)-2M(MAX)',
        'time of air temperature, maximum, 2m elevation, Degrees C':'AIRTEMP-2M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'relative humidity, average, 2m elevation, percent':'RH-2M',
        'relative humidity, minimum, 2m elevation, percent':'RH-2M(MIN)',
        'time of time of relative humidity minimum, 2m elevation, percent':'RH-2M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'relative humidity, maximum, 2m elevation, percent':'RH-2M(MAX)',
        'time of relative humidity maximum, 2m elevation, percent':'RH-2M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'Wind Speed, minimum, 2m elevation, meters/sec':'WINDSPEED(m/s)-2M(MIN)',
        'Time of wind speed minimum, 2m elevation':'WINDSPEED-2M(MIN) TIME DATE  MM/DD/YYYY HH24:MI',
        'Wind Speed, maximum, 2m elevation, meters/sec':'WINDSPEED(m/s)-2M(MAX)',
        'Time of wind speed maximum, 2m elevation':'WINDSPEED-2M(MAX) TIME DATE  MM/DD/YYYY HH24:MI',
        'Wind Speed, Average, 2m elevation, meters/sec':'WINDSPEED(m/s)-2M(AVG)',
        'Wind Direction, average, 2m elevation, Degrees':'WINDDIR-2M(DEGREES)',
        'Standard Deviation of Wind Direction, 2m elevation, Degrees ':'WINDDIR-2M STD DEV(DEGREES)',
        'Incoming shortwave radiation, Average, 5m elevation, watts/m^2':'IN SW RAD(W/m^2)-5M(AVG)',
        'Incoming shortwave radiation, minimum, 5m elevation, watts/m^2':'IN SW RAD(W/m^2)-5M(MIN)',
        'Time of incoming shortwave radiation minimum, 5m elevation':'IN SW RAD-5M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'incoming shortwave radiation, maximum, 5m elevation, watts/m^2':'IN SW RAD(W/m^2)-5M(MAX)',
        'Time of incoming shortwave radiation maximum, 5m elevation':'IN SW RAD-5M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'Incoming shortwave radiation, total, 5m elevation, MJoule/m^2':'IN SW RAD(MJ/m^2)-5M(TOTAL)',
        'Net Radiation, average, 5m elevation, watts/m^2':'NET RAD(W/m^2)-5M',
        'Net Radiation, average, corrected to wind speed, 5m elevation, watts/m^2':'NET RAD CORR(W/m^2)-5M',
        'air temperature, average, 10m elevation, Degrees C':'AIRTEMP(C)-10M(AVG)',
        'air temperature, minimum, 10m elevation, Degrees C':'AIRTEMP(C)-10M(MIN)',
        'time of air temperature minimum, 10m elevation, Degrees C':'AIRTEMP-10M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'air temperature, maximum, 10m elevation, Degrees C':'AIRTEMP(C)-10M(MAX)',
        'time of air temperature, maximum, 10m elevation, Degrees C':'AIRTEMP-10M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'relative humidity, average, 10m elevation, percent':'RH-10M',
        'relative humidity, minimum, 10m elevation, percent':'RH-10M(MIN)',
        'time of relative humidity minimum, 10m elevation, percent':'RH-10M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'relative humidity, maximum, 10m elevation, percent':'RH-10M(MAX)',
        'time of relative humidity maximum, 10m elevation, percent':'RH-10M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'Wind Speed, minimum, 10m elevation, meters/sec':'WINDSPEED(m/s)-10M(MIN)',
        'Time of wind speed minimum, 10m elevation':'WINDSPEED-10M(MIN) TIME DATE MM/DD/YYYY HH24:MI',
        'Wind Speed, maximum, 10m elevation, meters/sec':'WINDSPEED(m/s)-10M(MAX)',
        'Time of wind speed maximum, 10m elevation':'WINDSPEED-10M(MAX) TIME DATE MM/DD/YYYY HH24:MI',
        'Wind Speed, Average, 10m elevation, meters/sec':'WINDSPEED(m/s)-10M(AVG)',
        'Wind Direction, average, 10m elevation, Degrees':'WINDDIR-10M(DEGREES)',
        'Standard Deviation of Wind Direction, 10m elevation, Degrees ':'WINDDIR-10M STD DEV(DEGREES)'

    }
    dfd.rename(columns=dist_columns, inplace=True)
    '''dfd.iloc[:, 0] = dfd.iloc[:, 0].round(3)
    dfd.iloc[:, 1] = dfd.iloc[:, 1].round(3)
    dfd.iloc[:, 2] = dfd.iloc[:, 2].round(3)
    dfd.iloc[:, 3] = dfd.iloc[:, 3].round(3)
    dfd.iloc[:, 4] = dfd.iloc[:, 4].round(3)
    dfd.iloc[:, 5] = dfd.iloc[:, 5].round(3)
    dfd.iloc[:, 6] = dfd.iloc[:, 6].round(3)
    #dfd.iloc[:, 7] = dfd.iloc[:, 7].round(3)
    dfd.iloc[:, 8] = dfd.iloc[:, 8].round(3)
    #dfd.iloc[:, 9] = dfd.iloc[:, 9].round(3)
    dfd.iloc[:, 10] = dfd.iloc[:, 10].round(3)
    dfd.iloc[:, 11] = dfd.iloc[:, 11].round(3)
    #dfd.iloc[:, 12] = dfd.iloc[:, 12].round(3)
    dfd.iloc[:, 13] = dfd.iloc[:, 13].round(3)
    #dfd.iloc[:, 14] = dfd.iloc[:, 14].round(3)
    dfd.iloc[:, 15] = dfd.iloc[:, 15].round(3)
    #dfd.iloc[:, 16] = dfd.iloc[:, 16].round(3)
    dfd.iloc[:, 17] = dfd.iloc[:, 17].round(3)
    #dfd.iloc[:, 18] = dfd.iloc[:, 18].round(3)
    dfd.iloc[:, 19] = dfd.iloc[:, 19].round(3)
    dfd.iloc[:, 20] = dfd.iloc[:, 20].round(3)
    dfd.iloc[:, 21] = dfd.iloc[:, 21].round(3)
    dfd.iloc[:, 22] = dfd.iloc[:, 22].round(3)
    dfd.iloc[:, 23] = dfd.iloc[:, 23].round(3)
    dfd.iloc[:, 24] = dfd.iloc[:, 24].round(3)
    #dfd.iloc[:, 25] = dfd.iloc[:, 25].round(3)
    dfd.iloc[:, 26] = dfd.iloc[:, 26].round(3)
    #dfd.iloc[:, 27] = dfd.iloc[:, 27].round(3)
    dfd.iloc[:, 28] = dfd.iloc[:, 28].round(3)
    dfd.iloc[:, 29] = dfd.iloc[:, 29].round(3)
    dfd.iloc[:, 30] = dfd.iloc[:, 30].round(3)
    dfd.iloc[:, 31] = dfd.iloc[:, 31].round(3)
    dfd.iloc[:, 32] = dfd.iloc[:, 32].round(3)
    #dfd.iloc[:, 33] = dfd.iloc[:, 33].round(3)
    dfd.iloc[:, 34] = dfd.iloc[:, 34].round(3)
    #dfd.iloc[:, 35] = dfd.iloc[:, 35].round(3)
    dfd.iloc[:, 36] = dfd.iloc[:, 36].round(3)
    dfd.iloc[:, 37] = dfd.iloc[:, 37].round(3)
    #dfd.iloc[:, 38] = dfd.iloc[:, 38].round(3)
    dfd.iloc[:, 39] = dfd.iloc[:, 39].round(3)
    #dfd.iloc[:, 40] = dfd.iloc[:, 40].round(3)
    dfd.iloc[:, 41] = dfd.iloc[:, 41].round(3)
    #dfd.iloc[:, 42] = dfd.iloc[:, 42].round(3)
    dfd.iloc[:, 43] = dfd.iloc[:, 43].round(3)
    #dfd.iloc[:, 44] = dfd.iloc[:, 44].round(3)
    dfd.iloc[:, 45] = dfd.iloc[:, 45].round(3)
    dfd.iloc[:, 46] = dfd.iloc[:, 46].round(3)'''


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
pm.check_corrupt([-6999, 'NAN'])
pm.check_range([12, 15.1], 'Battery Voltage, Minimum, DC Volts')
pm.check_range([-20, 100], 'soil heat flux, average, 15cm depth, watts/m^2')
pm.check_increment([.001,None], 'Wind Speed, maximum, 2m elevation, meters/sec', absolute_value=True, min_failures=5)
pm.check_range([-200, 1000], 'Net Radiation, average, corrected to wind speed, 5m elevation, watts/m^2')
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

#-----------------------------------Precip----BP----RH--------------------------------------
trace1 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['barometric pressure, uncorrected to msl, mbar'],
    mode='lines',
    name='barometric pressure (mb)'
)
trace2 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['tipping bucket rain gage, total, mm'],
    mode='lines',
    name='precipitation (mm)',
)
trace3 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['relative humidity, average, 2m elevation, percent'],
    mode='lines',
    name='2m RH (%)',
)
trace4 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['relative humidity, average, 10m elevation, percent'],
    mode='lines',
    name='10m RH (%)'
)


fig = tools.make_subplots(rows=3, cols=1, specs=[[{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 2, 1)
fig.append_trace(trace3, 3, 1)
fig.append_trace(trace4, 3, 1)

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig, filename='BetMet_Met_AirT_RH_Precip')
#plotly.offline.plot(fig, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\BetassoMet\\QA_QC\\BT_Met_Python_Scripts\\1.html')

#-------------------------------Radiation----AirT------------------------------------------------------------------
trace5 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['incoming shortwave radiation, maximum, 5m elevation, watts/m^2'],
    mode='lines',
    name='incoming SW'
)
trace6 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Net Radiation, average, corrected to wind speed, 5m elevation, watts/m^2'],
    mode='lines',
    name='CNR',
)
trace7 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['air temperature, average, 2m elevation, Degrees C'],
    mode='lines',
    name='air T (2m)',
)
trace8 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['air temperature, average, 10m elevation, Degrees C'],
    mode='lines',
    name='Air T (10m)'
)



fig2 = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig2.append_trace(trace5, 1, 1)
fig2.append_trace(trace6, 1, 1)
fig2.append_trace(trace7, 2, 1)
fig2.append_trace(trace8, 2, 1)

# axis titles
fig2['layout']['yaxis1'].update(title='w/m^2')
fig2['layout']['yaxis2'].update(title='Degrees C')
# Plot Title
fig2['layout'].update(title='Betasso Met Tower Radiation and Air T')

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig2, filename='BetMet_Met_Radiation_Air')
#plotly.offline.plot(fig2, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\BetassoMet\\QA_QC\\BT_Met_Python_Scripts\\2.html')


#--------------------------------------------------Soil------------------

trace9 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['soil heat flux, average, 15cm depth, watts/m^2'],
    mode='lines',
    name='soil heat flux'
)
trace10 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['soil moisture 15cm depth, average, fraction'],
    mode='lines',
    name='soil moisture 15cm',
)
trace11 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['soil temperature, 15cm depth, average, Degrees C'],
    mode='lines',
    name='soil temp 15cm',
)
trace12 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['air temperature, average, 2m elevation, Degrees C'],
    mode='lines',
    name='Air T (2m)'
)



fig3 = tools.make_subplots(rows=4, cols=1, specs=[[{}], [{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig3.append_trace(trace9, 1, 1)
fig3.append_trace(trace10, 2, 1)
fig3.append_trace(trace11, 3, 1)
fig3.append_trace(trace12, 4, 1)
# Axis labels
fig3['layout']['yaxis1'].update(title='w/m^2')
fig3['layout']['yaxis2'].update(title='')
fig3['layout']['yaxis3'].update(title='Degrees C')
fig3['layout']['yaxis4'].update(title='Degrees C')
# Plot Title
fig3['layout'].update(title='Betasso Met Tower Soil')

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig3, filename='BetMet_Met_Soil')
#plotly.offline.plot(fig3, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\BetassoMet\\QA_QC\\BT_Met_Python_Scripts\\3.html')
#-----------------------------------------Precip & Wind-------------------------------------------------
trace13 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['tipping bucket rain gage, total, mm'],
    name='Tipping Bucket'
)
trace14 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, maximum, 2m elevation, meters/sec'],
    name='2m wind speed max',
)

fig4 = tools.make_subplots(rows=2, cols=1, specs=[[{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig4.append_trace(trace13, 1, 1)
fig4.append_trace(trace14, 2, 1)

# axis titles
fig4['layout']['yaxis1'].update(title='mm')
fig4['layout']['yaxis2'].update(title='m/s')
# Plot Title
fig4['layout'].update(title='Betasso Met Tower Tipping and Wind Speed')

#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig4, filename='multiple-axes-double')
#plotly.offline.plot(fig4, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\BetassoMet\\QA_QC\\BT_Met_Python_Scripts\\4.html')

#-------------------------------Wind--------------------------------------------------------------
trace15 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Direction, average, 10m elevation, Degrees'],
    mode='markers',
    name='2m Wind Direction',

)

trace16 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Direction, average, 10m elevation, Degrees'],
    mode='markers',
    name='10m Wind Direction',

)

trace17 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, minimum, 2m elevation, meters/sec'],
    mode='lines',
    name='2m wind speed min',
)
trace18 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, Average, 2m elevation, meters/sec'],
    mode='lines',
    name='2m wind speed AVG',
)
trace19 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, maximum, 2m elevation, meters/sec'],
    mode='lines',
    name='2m wind speed max'
)
trace20 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, minimum, 10m elevation, meters/sec'],
    mode='lines',
    name='10m wind speed min',
)
trace21 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, Average, 10m elevation, meters/sec'],
    mode='lines',
    name='10m wind speed AVG',
)
trace22 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Wind Speed, maximum, 10m elevation, meters/sec'],
    mode='lines',
    name='10m wind speed max'
)


fig5 = tools.make_subplots(rows=4, cols=1, specs=[[{}], [{}], [{}], [{}]],
                          shared_xaxes=True,
                          vertical_spacing=0.001)
fig5.append_trace(trace15, 3, 1)
fig5.append_trace(trace16, 4, 1)
fig5.append_trace(trace17, 1, 1)
fig5.append_trace(trace18, 1, 1)
fig5.append_trace(trace19, 1, 1)
fig5.append_trace(trace20, 2, 1)
fig5.append_trace(trace21, 2, 1)
fig5.append_trace(trace22, 2, 1)


#fig['layout'].update(height=600, width=600, title='Stacked Subplots with Shared X-Axes')

plot_url = py.plot(fig5, filename='BetMet_Met_Wind')
#plotly.offline.plot(fig5, filename='D:\\Dropbox (Boulder Creek CZO)\\Dropbox (Boulder Creek CZO)\\Boulder Creek CZO Team Folder\\BcCZO\\Data\\Betasso\\BetassoMet\\QA_QC\\BT_Met_Python_Scripts\\5.html')
#-----------------------------------------------------

trace23 = go.Scattergl(
    x=plotly_df.index,
    y=plotly_df['Battery Voltage, Minimum, DC Volts'],
    mode='lines',
    name='Battery Voltage'
)

data = [trace23]

plot_url = py.plot(data, filename='BetMet_Met_BattV')

data2 = [trace9, trace13]

plot_url = py.plot()




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





