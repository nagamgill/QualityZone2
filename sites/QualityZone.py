import glob
import os


import pandas as pd
import dropbox
import config

dbx = dropbox.Dropbox(config.dropbox_api)
dbx.users_get_current_account()


def download_master(master_path):
    """
    Download master CSV file from dbx. Assumes index is col 0.
    pd.na_values defines ADDITIONAL na strings, not only na strings.
    :param master_path: path to master file in dbx
    """
    print("downloading master CSV file from dropbox...")
    _, res = dbx.files_download(master_path)
    df_master = pd.read_csv(res.raw,
                            index_col=0,
                            )
    df_master.index = pd.to_datetime(df_master.index)
    return df_master


def download_new_data(new_path, newcols, start_date=None):
    """
    Download Campbell Scientific .dat file from dbx. Pandas read_csv params for CR-1000 TOA15 .dat file

    :param new_path: dbx path
    :param newcols: dictionary to rename headers from CR-1000 program to more human-readable.
    :param start_date: truncates data before this date, to properly append to Water Year of interest. Format: YYYY-MM-DD
    """
    print("downloading new data .dat from dropbox...")
    _, res = dbx.files_download(new_path)
    df_new = pd.read_csv(res.raw,
                         header=1,
                         skiprows=[2, 3],
                         index_col=0,
                         na_values='NAN'
                         )
    print("translating datalogger headers...")
    df_new.rename(columns=newcols, inplace=True)
    df_new.index = pd.to_datetime(df_new.index)
    if start_date is not None:
        df_new.sort_index(inplace=True)
        df_new = df_new.truncate(before=pd.Timestamp(start_date))
    return df_new


def append_non_duplicates(a, b, col=None):
    """
    Subtract index values from two DFs to append new data from data-logger.
    This func requires the new DF index to begin sometime after the old DF index.

    :param a: master or existing data.
    :param b: new data to append.
    :param col: column to determine new data (default is df.index)

    """
    print("Appending new data onto master dataframe...")
    if ((a is not None and type(a) is not pd.core.frame.DataFrame) or (
            b is not None and type(b) is not pd.core.frame.DataFrame)):
        raise ValueError('a and b must be of type pandas.core.frame.DataFrame.')
    if (a is None):
        return (b)
    if (b is None):
        return (a)
    if (col is not None):
        aind = a.iloc[:, col].values
        bind = b.iloc[:, col].values
    else:
        aind = a.index.values
        bind = b.index.values
    take_rows = list(set(bind) - set(aind))
    take_rows = [i in take_rows for i in bind]
    return (a.append(b.iloc[take_rows, :]))


def df_to_dropbox(dataframe, upload_path):
    """
    Upload DF to dbx as .CSV with UTF-8 encoding. This will overwrite the previously existing file.

    :param dataframe: df
    :param upload_path: dbx path to save file
    """
    print('Uploading dataframe to %s' % upload_path)
    df_string = dataframe.to_csv(index_label='TIMESTAMP')
    db_bytes = bytes(df_string, 'utf8')
    dbx.files_upload(
        f=db_bytes,
        path=upload_path,
        mode=dropbox.files.WriteMode.overwrite
    )


def qc_results_to_dropbox(qc_dir):
    """
    Upload Pecos QC results folder to dbx.

    :param qc_dir: dbx directory for test results.
    """
    print("Attempting to upload...")
    # walk return first the current folder that it walk, then tuples of dirs and files not "subdir, dirs, files"
    for dir, dirs, files in os.walk(qc_dir):
        for file in files:
            try:
                file_path = os.path.join(dir, file)
                dest_path = os.path.join(
                    '/CZO/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/Results/',
                    file)
                print('Uploading %s to %s' % (file_path, dest_path))
                with open(file_path, 'rb') as f:
                    dbx.files_upload(f.read(),
                                     dest_path,
                                     mode=dropbox.files.WriteMode.overwrite,
                                     mute=True)
            except Exception as err:
                print("Failed to upload %s\n%s" % (file, err))

    print("Finished upload.")


def dbx_csv_folder_download(dbx_folder, outpath):
    """
    download all .csv files from a dbx folder. This function is improved and expanded in ArchivalZone.dbx_pathlist_to_df.

    :param dbx_folder: location to pull CSV files from
    :param outpath: dbx location to save CSV files. In QZ2 this is temp folder.

    """
    entries = dbx.files_list_folder(dbx_folder).entries
    print("Downloading .csv files from dbx_folder")
    for entry in entries:
        if isinstance(entry, dropbox.files.FileMetadata) and entry.path_lower.endswith('.csv'):
            md, res = dbx.files_download(entry.path_lower)
            # 'wb' or "write binary" may cause Mac / PC compatibility issues - needs testing
            with open(os.path.join(outpath + '/' + entry.name), 'wb') as out:
                out.write(res.content)
                print("Saving %s to outpath" % (entry.name))


def dbx_dat_folder_download(dbx_folder, outpath):
    """
    Download .dat files from a specified dbx filder.
    """
    entries = dbx.files_list_folder(dbx_folder).entries
    print("Downloading .dat files from %s" % dbx_folder)
    for entry in entries:
        if isinstance(entry, dropbox.files.FileMetadata) and entry.path_lower.endswith('.dat'):
            md, res = dbx.files_download(entry.path_lower)
            # 'wb' or "write binary" may cause Mac / PC compatibility issues - needs testing
            with open(os.path.join(outpath + '/' + entry.name), 'wb') as out:
                out.write(res.content)
                print("Saving %s to outpath" % (entry.name))


def concat_dat(dat_path, start_date=None):
    """
    Concat the saved dat files in folder with pd.index_sort and pd.drop_duplicates.
    This func is also improved in ArchivalZone.dbx_pathlist_to_df, but still in use in QualityZone2

    :param dat_path: dbx path to .dat files.
    :param start_date: truncate data before this data. Format is YYYY-MM-DD
    """
    print('Concatenating .dat files in folder with index_sort and drop_duplicates')
    df = pd.concat([pd.read_csv(
        f,
        header=1,
        skiprows=[2, 3],
        index_col=0,
        na_values='NAN')
        for f in glob.glob(os.path.join(dat_path + '/*.dat'))], sort=False)
    df.drop_duplicates(inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)
    if start_date is not None:
        df = df.truncate(before=pd.Timestamp(start_date))

    return df


def drop_dup(df):
    """
    Alternative to Pandas drop_duplicates command, which ignores df index. This func only checks the index.
    Especially useful for data with slowly changing values, such as groundwell pressure and temp.
    """
    df = df.loc[~df.index.duplicated(keep='first')]
    return df