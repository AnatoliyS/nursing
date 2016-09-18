import datetime


class NurseRecord:
    import pandas as pd
    frame = pd.DataFrame
    path = ''
    nurseId = int
    startDate = datetime.datetime
    endDate = datetime.datetime

    def __init__(self, path=str):
        import pandas as pd
        self.frame = pd.read_csv(path, header=0)
        print('Nurse record created')
        self.path = path
        filename = self.path.split('/')[-1].split('.')[0]
        info = filename.split('_')
        self.nurseId = int(info[0][1:])
        date = info[1]
        start_time = info[2].split('-')[0]
        end_time = info[2].split('-')[1]
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