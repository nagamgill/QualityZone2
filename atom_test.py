
import QualityZone
import pandas as pd
import io
from io import StringIO
import config
import dropbox

dbx = dropbox.Dropbox(config.dropbox_api)
dbx.users_get_current_account()



df = pd.read_csv('/Users/dillon/downloads/GGL_NF_SP5_TEST.csv')

def to_dropbox(dataframe, path):
    df_string = dataframe.to_csv(index=False)
    db_bytes = bytes(df_string, 'utf8')
    dbx.files_upload(
        f=db_bytes,
        path=path,
        mode=dropbox.files.WriteMode.overwrite
    )


rootdir2='/Users/dillon/QualityZone_2/dirpath/'
rootdir='/dirpath'

print ("Attempting to upload...")
# walk return first the current folder that it walk, then tuples of dirs and files not "subdir, dirs, files"
for dir, dirs, files in os.walk(rootdir):
    for file in files:
        try:
            file_path = os.path.join(dir, file)
            dest_path = os.path.join('/Boulder Creek CZO Team Folder/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/Results/testing/', file)
            print('Uploading %s to %s' % (file_path, dest_path))
            with open(file_path, 'rb') as f:
                dbx.files_upload(f.read(), dest_path, mute=True)
        except Exception as err:
            print("Failed to upload %s\n%s" % (file, err))

print("Finished upload.")




with open(rootdir, 'rb') as f:
    dbx.files_upload(f.read(), dest_path)







def upload(dbx, fullname, folder, subfolder, name, overwrite=False):
    """Upload a file.
    Return the request response, or None in case of error.
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    mtime = os.path.getmtime(fullname)
    with open(fullname, 'rb') as f:
        data = f.read()
    with stopwatch('upload %d bytes' % len(data)):
        try:
            res = dbx.files_upload(
                data, path, mode,
                client_modified=datetime.datetime(*time.gmtime(mtime)[:6]),
                mute=True)
        except dropbox.exceptions.ApiError as err:
            print('*** API error', err)
            return None
    print('uploaded as', res.name.encode('utf8'))
    return res



file_from = '/Users/dillon/QualityZone_2/dirpath/GGL_SF_SP6.html'
file_to = '/Boulder Creek CZO Team Folder/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/Results/testing/test.html'
def upload_file(file_from, file_to):
    f = open(file_from, 'rb')
    dbx.files_upload(f.read(), file_to)
upload_file(file_from,file_to)

file_from = '/Users/dillon/QualityZone_2/dirpath/custom.png'
file_to = '/Boulder Creek CZO Team Folder/BcCZO/Personnel_Folders/Dillon_Ragar/QualityZone/Results/testing/test2.png'
def upload_file(file_from, file_to):
    f = open(file_from, 'rb')
    dbx.files_upload(f.read(), file_to)
upload_file(file_from,file_to)
