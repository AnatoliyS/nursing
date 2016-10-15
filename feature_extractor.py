from record import NurseSensorsRecord

def ExtractFeatures(sensors_data_frame, bounds):
    """Extracts features.

    Args:
      sensors_data: data.

    Returns:
      Dictionary <feature_name, value> features.
    """
    features = {}
    for i in range(1, 10):
        features[sensors_data_frame.columns[i] + '_mean'] = sensors_data_frame.iloc[list(bounds), i].mean()
        features[sensors_data_frame.columns[i] + '_std'] = sensors_data_frame.iloc[list(bounds), i].std()

    for i in range(3):
        start_idx = 1 + i*3
        end_idx = start_idx + 3
        prefix = sensors_data_frame.columns[start_idx][:-2]
        data = sensors_data_frame.iloc[list(bounds), start_idx:end_idx]
        norm = data.pow(2).sum(1).pow(0.5)
        features[prefix + '_norm_mean'] = norm.mean()
        features[prefix + '_norm_std'] = norm.std()

    return features