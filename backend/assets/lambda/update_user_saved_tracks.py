from collections import deque
import logging
import os
import time

import awswrangler as wr
import spotipy


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

            item = {
                'UserId': user_id,
                'Offset': page_offset,
                'Total': response['total'],
                'Tracks': [[track['track']['id']] + [
                    artist['id']
                    for artist in track['track']['artists']
                ] for track in response['items']],
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
