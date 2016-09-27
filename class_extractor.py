from record import NurseSensorsRecord, TrainingSample
from datetime import datetime, timedelta
from pandas import DataFrame


def sameDate(date1, date2):
    return (date1 - date2).days == 0


def inTimeRange(start_date, finish_date, date):
    return start_date < date < finish_date


def ExtractClasses(label_records, sensors_data):
    train_data = {}
    print('Extracting classes and preparing training data ')

    for nurse_lable_record in label_records:
        print('Processing file ' + nurse_lable_record.filename)
        label_date = nurse_lable_record.date

        for index, row in nurse_lable_record.frame.iloc[:, :].iterrows():
            action_id = int(row['action_id'])
            start_t = datetime.strptime(row['start_t'], '%Y-%m-%d %H:%M:%S')
            finish_t = datetime.strptime(row['finish_t'], '%Y-%m-%d %H:%M:%S')

            print('Finding sensors data for action' + str(action_id))

            for sensor_record in sensors_data:
                if nurse_lable_record.nurseId != sensor_record.nurseId:
                    continue
                if not sameDate(label_date, sensor_record.startDate):
                    continue

                frame = DataFrame
                for sensor_index, sensor_row in sensor_record.frame.iloc[:, :].iterrows():
                    current_time = sensor_record.startDate + \
                                   timedelta(milliseconds=int(sensor_row['time']) * 1000)
                    if inTimeRange(start_t, finish_t, current_time):
                        frame.append(sensor_row)

                if frame.shape[0] > 0:
                    print('Gathered ' + str(frame.shape[0]) + ' records')
                    train_data.setdefault(TrainingSample(action_id=action_id, sensors_data=frame))

    return train_data
