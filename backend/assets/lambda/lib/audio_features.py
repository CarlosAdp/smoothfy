from enum import Enum

import numpy as np
import pandas as pd


class Tempo(Enum):
    GRAVE = -np.inf
    LENTO = 40
    LARGO = 45
    ADAGIO = 55
    ADAGIETTO = 65
    ANDANTE = 73
    MODERATO = 86
    ALLEGRETTO = 98
    ALLEGRO = 109
    VIVACE = 132
    PRESTO = 168
    PRESTISSIMO = 178


class Duration(Enum):
    VERY_SHORT = -np.inf
    SHORT = 1
    MEDIUM = 2.5
    LONG = 4.5
    VERY_LONG = 8


def normalize_audio_features(features: pd.DataFrame) -> pd.DataFrame:
    features = features.copy()
    features = features.drop(
        columns=['type', 'uri', 'track_href', 'analysis_url'])\
        .set_index('id')

    features['loudness'] = 10 ** (features['loudness'] / 20)

    features['tempo'] = pd.cut(
        x=features['tempo'],
        bins=[t.value for t in Tempo] + [np.inf],
        labels=np.around(np.linspace(0, 1, len(Tempo)), decimals=2)
    ).astype(float)
    features['duration'] = pd.cut(
        x=features['duration_ms'] / 60000,
        bins=[d.value for d in Duration] + [np.inf],
        labels=np.around(np.linspace(0, 1, len(Duration)), decimals=2)
    ).astype(float)
    features = features.drop(columns=['duration_ms'])

    qualitative = ['key', 'mode', 'time_signature']
    features[qualitative] = features[qualitative].astype(str)

    return features
