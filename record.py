import datetime
import pandas as pd
import os

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
        self.date = datetime.datetime(
            int(date[0:4]),  # year
            int(date[4:6]),  # month
            int(date[6:8]),  # day
            0,
            0,
            0
        )
        print('Labels record created (file "' + path + '")')

    def write(self, path='./data/processed/Labelled/labels/'):
        self.frame.to_csv(path + self.filename + '.csv', sep=',', header=True,
                          index=False)


class TrainingSample:
    def __init__(self, id, action_id, sensors_data):
        self.frame = sensors_data
        self.action_id = action_id
        self.id = id

        print('Training sample created')

    def write(self, path='./data/processed/Labelled/sensors/'):
        path = path + str(self.action_id) + '/'
        if not os.path.exists(path):
            os.makedirs(path)
        self.frame.to_csv(path + str(self.id) + '.csv', sep=',', header=True,
                          index=False)