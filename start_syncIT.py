import datetime
import logging
import os
import shutil
import tempfile

import pysftp
from pandas import DataFrame, ExcelFile

logging.basicConfig(filename='sftp-to-sftp.log', level=logging.DEBUG, format='%(asctime)s %(message)s')


class syncIT:
    def __init__(self):
        self.dt = str(datetime.datetime.now())

    def get_single_sync(self, syncFileNames, syncSourcePath, syncSourceInfo, localDestination):
        with tempfile.TemporaryDirectory() as syncLocalTempPath:
            tempdir = os.path.join(syncLocalTempPath, syncFileNames)
            remote = os.path.join(syncSourcePath, syncFileNames)

            with pysftp.Connection(**syncSourceInfo) as gsftp:
                logging.info("connected")
                with gsftp.cd(syncSourcePath):  # temporarily chdir
                    if TimestampIt:
                        try:
                            renamed_filename = "{}.{}".format(self.dt, syncFileNames)
                            gsftp.get(remote, tempdir, preserve_mtime=True)  # get a remote file
                            moveto = os.path.join(localDestination, renamed_filename)
                            shutil.copyfile(tempdir, moveto, follow_symlinks=False)
                            logging.info("File {} copied from: {} to: {}".format(renamed_filename, syncSourcePath,
                                                                                 localDestination))
                        except Exception as e:
                            logging.warning(e)
                    else:
                        try:
                            gsftp.get(remote, tempdir, preserve_mtime=True)  # get a remote file
                            moveto = os.path.join(localDestination, syncFileNames)
                            shutil.copyfile(tempdir, moveto, follow_symlinks=False)
                            logging.info("File {} copied from: {} to: {}".format(syncFileNames, syncSourcePath,
                                                                                 localDestination))
                        except Exception as e:
                            logging.warning(e)

    def get_dual_sync(self, syncFileNames, syncSourcePath, syncSourceInfo, syncDestinationPath, syncDestinationInfo):
        with tempfile.TemporaryDirectory() as SyncLocalTempPath:
            tempdir = os.path.join(SyncLocalTempPath, syncFileNames)

            with pysftp.Connection(**syncSourceInfo) as gsftp:
                logging.info("connected")
                with gsftp.cd(syncSourcePath):  # temporarily chdir
                    try:
                        copyfrom = os.path.join(syncSourcePath, syncFileNames)
                        gsftp.get(copyfrom, tempdir, preserve_mtime=True)  # get a remote file
                        logging.info("File {} copied from: {} to: {}".format(syncFileNames, syncSourcePath,
                                                                             SyncLocalTempPath))
                    except Exception as e:
                        logging.warning(e)

            with pysftp.Connection(**syncDestinationInfo) as gsftp:
                logging.info("connected")
                with gsftp.cd(syncDestinationPath):  # temporarily chdir
                    if TimestampIt:
                        try:
                            renamed_filename = "{}.{}".format(self.dt, syncFileNames)
                            copyto = os.path.join(syncDestinationPath, renamed_filename)
                            gsftp.put(tempdir, copyto, preserve_mtime=True)  # get a remote file
                            logging.info("File {} copied from: {} to: {}".format(renamed_filename, SyncLocalTempPath,
                                                                                 syncDestinationPath))
                        except Exception as e:
                            logging.warning(e)
                    else:
                        try:
                            copyto = os.path.join(syncDestinationPath, syncFileNames)
                            gsftp.put(tempdir, copyto, preserve_mtime=True)  # get a remote file
                            logging.info("File {} copied from: {} to: {}".format(syncFileNames, SyncLocalTempPath,
                                                                                 syncDestinationPath))
                        except Exception as e:
                            logging.warning(e)


class gatherIT:
    def __init__(self):
        syncopts = './settings.xlsx'
        self.x1 = ExcelFile(syncopts)

    def getSettings(self):
        df1 = self.x1.parse('OtherSettings')
        df1a = DataFrame(df1)
        for index, row in df1a.iterrows():
            global TimestampIt, IsLocal
            TimestampIt = row["timestamp_it"]
            IsLocal = row["is_local"]

    def LocalSync(self):
        cnopts = pysftp.CnOpts()

        df2 = self.x1.parse('LocalSync')
        df2a = DataFrame(df2)
        for index, row in df2a.iterrows():
            si = syncIT()
            ghost = row["host"]
            gusername = row["username"]
            gpassword = row["private_key"]
            gport = row["port"]
            cnopts.hostkeys = None

            SyncFileNames = row["sync_file_names"]
            LocalDestination = row["local_destination"]
            SyncSourcePath = row["sync_source_path"]
            SyncSourceInfo = {'host': ghost,
                              'username': gusername,
                              'password': gpassword,
                              'port': gport,
                              'cnopts': cnopts}
            try:
                si.get_single_sync(SyncFileNames, SyncSourcePath, SyncSourceInfo, LocalDestination)
            except FileNotFoundError as e:
                logging.warning(e)
                continue

    def RemoteSync(self):
        cnopts = pysftp.CnOpts()

        df3 = self.x1.parse('RemoteSync')
        df3a = DataFrame(df3)
        for index, row in df3a.iterrows():
            si = syncIT()
            shost = row["source_host"]
            susername = row["source_username"]
            spassword = row["source_private_key"]
            sport = row["source_port"]
            cnopts.hostkeys = None

            SyncFileNames = row["sync_file_names"]
            SyncSourcePath = row["sync_source_path"]
            SyncSourceInfo = {'host': shost,
                              'username': susername,
                              'password': spassword,
                              'port': sport,
                              'cnopts': cnopts}

            dhost = row["destination_host"]
            dusername = row["destination_username"]
            dpassword = row["destination_private_key"]
            dport = row["destination_port"]
            cnopts.hostkeys = None

            SyncDestinationPath = row["sync_destination_path"]
            SyncDestinationInfo = {'host': dhost,
                                   'username': dusername,
                                   'password': dpassword,
                                   'port': dport,
                                   'cnopts': cnopts}

            try:
                si.get_dual_sync(SyncFileNames, SyncSourcePath, SyncSourceInfo, SyncDestinationPath,
                                 SyncDestinationInfo)
            except FileNotFoundError as e:
                logging.warning(e)
                continue


def main():
    logging.info("This script is intended to assist in the operation of copying file from one sftp to another")
    gi = gatherIT()
    gi.getSettings()
    if IsLocal:
        try:
            gi.LocalSync()
            logging.info("Done")
        except KeyboardInterrupt:
            logging.info("Sync stopped")
            exit(0)
    else:
        try:
            gi.RemoteSync()
            logging.info("Done")
        except KeyboardInterrupt:
            logging.info("Sync stopped")
            exit(0)


if __name__ == '__main__':
    main()
