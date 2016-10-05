#!/usr/bin/env python
import ioutils as io
from record import NurseLables, NurseSensorsRecord
from class_extractor2 import ClassExtractor


labels_file_paths = io.get_filepaths("./data/processed/Labelled/labels")
sensors_file_paths = io.get_filepaths("./data/Labelled/sensors")

labels_data = [NurseLables(filepath) for filepath in labels_file_paths]
sensors_data = [NurseSensorsRecord(filepath) for filepath in sensors_file_paths]

extractor = ClassExtractor()
extractor.LoadLables(labels_data)
extractor.LoadSensors(sensors_data)
extractor.Process()
