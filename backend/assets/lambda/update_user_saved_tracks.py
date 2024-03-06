from collections import deque
import logging
import os
import time

import awswrangler as wr
import numpy as np
import pandas as pd
import spotipy

from lib import audio_features


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def handler(event: dict, context: dict) -> dict:
    for queue_record in event['Records']:
        attributes = queue_record['messageAttributes']

        user_id = attributes['UserId']['stringValue']
        access_token = attributes['AccessToken']['stringValue']
        sp = spotipy.Spotify(auth=access_token)

        pages = wr.dynamodb.read_items(
            table_name=os.getenv('USER_SAVED_TRACKS_TABLE'),
            key_condition_expression='UserId = :user_id',
            expression_attribute_values={':user_id': user_id},
        ).reindex(columns=['UserId', 'Offset', 'Total', 'TTL'])
        pages[['Offset', 'Total']] = pages[['Offset', 'Total']].astype(int)

        total = \
            pages.loc[pages['TTL'].idxmax(), 'Total'] if len(pages) else 10000

        processing_queue = deque(range(0, total, 50))
        while processing_queue:
            page_offset = processing_queue.popleft()

            logger.info('Getting user saved tracks at offset %d', page_offset)

            if page_offset in pages['Offset'].values:
                continue
            if page_offset > total:
                break

            response = sp.current_user_saved_tracks(50, page_offset)
            total = response['total']

            track_ids = [track['track']['id'] for track in response['items']]
            features = pd.DataFrame(filter(None, sp.audio_features(track_ids)))
            features = audio_features.normalize_audio_features(features)

            tracks = pd.json_normalize(response['items'])\
                .loc[:, ['track.id', 'track.name', 'track.artists']]\
                .rename(columns={
                    'track.id': 'Id',
                    'track.name': 'Name',
                    'track.artists': 'Artists',
                })\
                .set_index('Id')\
                .join(features, how='inner')\
                .reset_index()

            numeric_cols = tracks.select_dtypes(include=[np.number]).columns
            tracks[numeric_cols] = (tracks[numeric_cols] * 1000).astype(int)

            tracks = tracks.to_dict(orient='records')

            item = {
                'UserId': user_id,
                'Offset': page_offset,
                'Total': response['total'],
                'Tracks': tracks,
                'TTL': int(time.time()) + 60 * 60 * 24,
            }

            wr.dynamodb.put_items(
                table_name=os.getenv('USER_SAVED_TRACKS_TABLE'),
                items=[item],
            )

            logger.info('Saved tracks at offset %d', page_offset)

    return {
        "statusCode": 200,
        "body": "Tracks successfully saved"
    }


if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()

    print(handler({
        'Records': [
            {
                'messageAttributes': {
                    'UserId': {'stringValue': os.getenv('USER_ID')},
                    'AccessToken': {'stringValue': os.getenv('ACCESS_TOKEN')},
                }
            }
        ]
    }, None))
