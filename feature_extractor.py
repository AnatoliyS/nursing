from record import NurseSensorsRecord

def ExtractFeatures(sensors_data):
    """Extracts features.

    Args:
      sensors_data: data.

    Returns:
      Dictionary <feature_name, value> features.
    """
    features = {}
    features['record_length'] = sensors_data.frame.shape[0]
    for i in range(2, 10):
        features[sensors_data.frame.columns[i] + '_mean'] = sensors_data.frame.iloc[:, i].mean()
        features[sensors_data.frame.columns[i] + '_std'] = sensors_data.frame.iloc[:, i].std()

    for i in range(3):
        start_idx = 1 + i*3
        end_idx = start_idx + 3
        prefix = sensors_data.frame.columns[start_idx][:-2]
        data = sensors_data.frame.iloc[:, start_idx:end_idx]
        norm = data.pow(2).sum(1).pow(0.5)
        features[prefix + '_norm_mean'] = norm.mean()
        features[prefix + '_norm_std'] = norm.std()

    return features