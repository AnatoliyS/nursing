import collections
import datetime
import record
import pandas
import numpy
import feature_extractor


class Window:
    def __init__(self, start, length=256, delta=128):
        self.offset = start
        self.length = length
        self.delta = delta

    def next(self):
        self.offset += self.delta

    def hasNext(self, bound):
        return self.offset + self.length - 1 < bound

    def bounds(self):
        return range(self.offset, self.offset + self.length - 1)

    def __str__(self):
        return 'Window(start: {}, end: {})'.format(self.offset, self.offset + self.length - 1)


class SlidingWindowProcessor:
    def __init__(self, action_sample, starting_offset=0, window_length=512, delta=256):
        self.actionSample = action_sample
        self.window = Window(starting_offset, window_length, delta)

    def applyDefaultPadding(self):
        # default is to pad with zero
        data = numpy.zeros((self.window.delta, len(self.actionSample.frame.columns)))
        paddingFrame = pandas.DataFrame(data, columns=self.actionSample.frame.columns)
        self.applyPadding(paddingFrame)

    # pad with some values before and after
    def applyPadding(self, paddingFrame):
        self.actionSample.frame = pandas.concat([paddingFrame, self.actionSample.frame, paddingFrame])

    def process(self):
        time_series_length = self.actionSample.frame.shape[0]
        #sample_number = 0
        sample_id = self.actionSample.id
        sample = record.TrainingSamplePack(sample_id, self.actionSample.action_id)
        while self.window.hasNext(time_series_length):
            #print('window is now at' + self.window.__str__())
            features = feature_extractor.ExtractFeatures(self.actionSample.frame, self.window.bounds())
            sample.add_row(features)
            self.window.next()
        sample.write()
        return sample
        #sample_number += 1
