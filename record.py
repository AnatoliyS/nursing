import datetime
import pandas as pd

class NurseSensorsRecord:
    def __init__(self, path):
        self.frame = pd.read_csv(path, header=0)
        self.path = path
        
        filename = self.path.split('/')[-1].split('.')[0]
        info = filename.split('_')
        date = info[1]
        start_time = info[2].split('-')[0]
        end_time = info[2].split('-')[1]
        self.nurseId = int(info[0][1:])
        self.startDate = datetime.datetime(
            int(date[0:4]),  # year
            int(date[4:6]),  # month
            int(date[6:8]),  # day
            int(start_time[0:2]),  # hour
            int(start_time[2:4]),  # minute
            int(start_time[4:6])  # second
        )
        self.endDate = datetime.datetime(
            int(date[0:4]),  # year
            int(date[4:6]),  # month
            int(date[6:8]),  # day
            int(end_time[0:2]),  # hour
            int(end_time[2:4]),  # minute
            int(end_time[4:6])  # second
        )
        print('Nurse sensors record created (file "' + path + '")')

class NurseLables:
    def __init__(self, path):
        self.frame = pd.read_csv(path, header=0)
        self.path = path

        self.filename = self.path.split('/')[-1].split('.')[0]
        info = self.filename.split('_')
        date = info[1]
        self.nurseId = int(info[0][1:])
        self.date = datetime.date(
            int(date[0:4]),  # year
            int(date[4:6]),  # month
            int(date[6:8])  # day
        )
        print('Labels record created (file "' + path + '")')

    def write(self, path='./data/processed/Labelled/sensors/'):
        self.frame.to_csv(path + self.filename + '.csv', sep=',', header=True,
                          index=False)
