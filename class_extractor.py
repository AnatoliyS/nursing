import pandas as pd

from record import NurseSensorsRecord, TrainingSample
from datetime import datetime, timedelta
from pandas import DataFrame


def sameDate(date1, date2):
    return date1.day == date2.day and date1.month == date2.month and date1.year == date2.year


def inTimeRange(start_date, finish_date, date):
    return start_date < date < finish_date


def ExtractClasses(label_records, sensors_data):
    train_data = {}
    train_num = 0
    print('Extracting classes and preparing training data ')

    processed = 0
    total = 0
    for nurse_lable_record in label_records[:1]:
        print('Counting file ' + nurse_lable_record.filename)
        total += nurse_lable_record.frame.shape[0]
        
    total *= len(sensors_data)
    
    print('Total of {}'.format(total))

    for nurse_lable_record in label_records[:1]:
        print('Processing file ' + nurse_lable_record.filename)
        label_date = nurse_lable_record.date

        for index, row in nurse_lable_record.frame.iloc[:, :].iterrows():
            print(row['action_id'])
            action_id = int(row['action_id'])
            start_t = datetime.strptime(row['start_t'], '%Y-%m-%d %H:%M:%S')
            finish_t = datetime.strptime(row['finish_t'], '%Y-%m-%d %H:%M:%S')

            #print("Finding sensors data for actiond " + str(action_id))

            for sensor_record in sensors_data:
                processed += 1
                print('Processed {} out of {}'.format(processed, total))

                if nurse_lable_record.nurseId != sensor_record.nurseId:
                    continue
                if not sameDate(label_date, sensor_record.startDate):
                    continue

                #print("found records for this date")

                rows_list = []
                for _, sensor_row in sensor_record.frame.iloc[:, :].iterrows():
                    current_time = sensor_record.startDate + timedelta(milliseconds=int(sensor_row['time']) * 1000)
                    #print('checking time' + str(current_time))
                    if inTimeRange(start_t, finish_t, current_time):
                        #print('date is in range')
                        rows_list.append(sensor_row)

                if len(rows_list) > 0:
                    train_num += 1
                    frame = DataFrame(rows_list)
                    print('Gathered ' + str(frame.shape[0]) + ' records')
                    #train_data.setdefault(TrainingSample(action_id=action_id, sensors_data=frame))
                    sample = TrainingSample(id=train_num, action_id=action_id, sensors_data=frame)
                    sample.write()


    return train_data
