import collections
import datetime
import pandas as pd
import record


def inTimeRange(start_date, finish_date, date):
    return start_date < date < finish_date


def SkipTime(date):
    return datetime.date(date.year, date.month, date.day)
    

class LableRow:
    def __init__(self, action_id, start_t, finish_t):
        self.action_id = action_id
        self.start_t = start_t
        self.finish_t = finish_t

    @classmethod
    def ParseLableRecord(cls, record):
        for index, row in record.frame.iterrows():
            action_id = int(row['action_id'])
            start_t = datetime.datetime.strptime(row['start_t'], '%Y-%m-%d %H:%M:%S')
            finish_t = datetime.datetime.strptime(row['finish_t'], '%Y-%m-%d %H:%M:%S')
            
            yield cls(action_id, start_t, finish_t)

class SensorRow:
    def __init__(self, time, data):
        self.time = time
        self.data = data
        
    @classmethod
    def ParseSensorRecord(cls, record):
        for index, row in record.frame.iterrows():
            time = record.startDate + datetime.timedelta(seconds=float(row['time']))
            yield cls(time, row)
            

class Event:
    def __init__(self, klass, time, data):
        self.klass = klass
        self.time = time
        self.data = data
     
    def __lt__(self, o):
        return (self.time, self.klass) < (o.time, o.klass)

    def __str__(self):
        return 'Event( klass: {}, time: {})'.format(self.klass, self.time)


class EventProcessor:
    def __init__(self):
        self.events = []
        self.lable_records = []
        self.sensor_records = []

    def FullfillLableEvents(self, record):
        self.lable_records.append(record)

    def FullfillLableEventsReal(self, record):
        for row in LableRow.ParseLableRecord(record):
            self.events.append(Event(-1, row.start_t, row))
            self.events.append(Event(+1, row.finish_t, row))

    def FullfillSensorEvents(self, record):
        self.sensor_records.append(record)

    def FullfillSensorEventsReal(self, record):
        for row in SensorRow.ParseSensorRecord(record):
            self.events.append(Event(0, row.time, row))

    def Sort(self):
        self.events.sort()

    def WarmUp(self):
        for record in self.lable_records:
            self.FullfillLableEventsReal(record)
        self.lable_records[:] = []

        for record in self.sensor_records:
            self.FullfillSensorEventsReal(record)
        self.sensor_records[:] = []

        self.Sort()

    def __len__(self):
#        return len(self.events)
        length = 0
        for records in [self.lable_records, self.sensor_records]:
            for record in records:
                length += len(record)
        return length 


class ClassExtractor:
    def __init__(self):
        self.nurse_date_to_events = collections.defaultdict(EventProcessor)

    def LoadLables(self, records):
        for i, record in enumerate(records):
            print('Parsing lable record {}/{}. Contains {} items.'.format(i + 1, len(records), len(record)))
            event_processor = self.nurse_date_to_events[(record.nurseId, SkipTime(record.date))]
            event_processor.FullfillLableEvents(record)

    def LoadSensors(self, records):
        for i, record in enumerate(records):
            print('Parsing sensor record {}/{}. Contains {} items.'.format(i + 1, len(records), len(record)))
            event_processor = self.nurse_date_to_events[(record.nurseId, SkipTime(record.startDate))]
            event_processor.FullfillSensorEvents(record)

    def Sort(self):
        for i, (nurse_date, event_processor) in enumerate(self.nurse_date_to_events.items()):
            print('Sorting EventProcessor {}/{}. Contains {} items.'.format(i + 1, len(self.nurse_date_to_events),
                                                                            len(event_processor)))

    def Process(self):
        next_sample_id = 0

        total_length = sum([len(ep) for ep in self.nurse_date_to_events.values()])

        for i, (nurse_date, event_processor) in enumerate(self.nurse_date_to_events.items()):
            print('Processing EventProcessor {}/{} containing {}/{} items.'.format(i + 1, len(self.nurse_date_to_events),
                                                                                   len(event_processor), total_length))

            event_processor.WarmUp()
            sample_builders = {}
            for event in event_processor.events:
                data = event.data
                if event.klass == -1:
                    sample_builders[data] = record.TrainingSampleBuilder(next_sample_id, data.action_id)
                    next_sample_id += 1
                elif event.klass == +1:
                    sample = sample_builders.pop(data).build()
                    sample.write()
                elif event.klass == 0:
                    for builder in sample_builders.values():
                        builder.add_row(data.data)

            print('Erasing EventProcessor.'.format(len(event_processor)))
            self.nurse_date_to_events[nurse_date] = None
