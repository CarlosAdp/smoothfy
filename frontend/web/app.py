import logging
import os
import time

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, session
import awswrangler as wr
import boto3
import pandas as pd
import spotipy
import spotipy.oauth2


load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

scope = [
    'user-read-private', 'user-library-read', 'user-read-recently-played',
    'user-read-currently-playing', 'user-modify-playback-state',
    'user-read-playback-state',
]
auth_manager = spotipy.oauth2.SpotifyOAuth(scope=scope)


@app.before_request
def check_token():
    if request.endpoint == 'callback':
        return

    match session.get('user_info'):
        case {'token_info': token_info}:
            logger.debug('User is authenticated')
            if auth_manager.is_token_expired(token_info):
                logger.debug('Token is expired, refreshing')
                token_info = auth_manager.refresh_access_token(
                    token_info['refresh_token'])
                session['user_info']['token_info'] = token_info
        case _:
            session['original_url'] = request.url
            logger.debug('User is not authenticated')
            return redirect(auth_manager.get_authorize_url())


@app.route('/')
def index():
    return redirect('/profile')


@app.route('/callback')
def callback():
    code = request.args.get('code')
    token_info = auth_manager.get_access_token(code)
    access_token = token_info['access_token']

    sp = spotipy.Spotify(auth=access_token)
    user = sp.current_user()

    session['user_info'] = {
        'id': user['id'],
        'display_name': user['display_name'],
        'email': user.get('email'),
        'image_url': user['images'][0]['url'] if user['images'] else None,
        'token_info': token_info,
    }

    original_url = session.pop('original_url', '/profile')

    return redirect(original_url)


@app.route('/profile')
def profile():
    user_info = session['user_info']
    return jsonify(user_info)


@app.route('/update_library')
def update_library():
    # TODO: Make this call async
    now = pd.Timestamp.now()
    access_token = session['user_info']['token_info']['access_token']
    sp = spotipy.Spotify(auth=access_token)
    saved_tracks = []

    offset = 0
    while (
        response := sp.current_user_saved_tracks(offset=offset, limit=50)
    )['next']:
        logger.info('Saving user library tracks, offset: %s', offset)
        saved_tracks.extend(response['items'])
        offset += 50

        time.sleep(1)

    saved_tracks = pd.json_normalize(saved_tracks)[[
        'track.id',
        'track.name',
        'track.type',
        'track.duration_ms',
        'track.track_number',
        'track.available_markets',
        'track.popularity',
        'track.album.id',
        'track.album.name',
        'track.album.type',
        'track.album.release_date',
        'added_at',
    ]]
    saved_tracks['user_id'] = session['user_info']['id']
    saved_tracks['collected_at'] = now

    s3_bucket = os.getenv('S3_BUCKET')
    database = os.getenv('DATABASE_NAME')
    table = 'user_library'

    boto3_session = boto3.Session(region_name=os.getenv('AWS_REGION'))
    wr.s3.to_parquet(
        boto3_session=boto3_session,
        df=saved_tracks,
        path=f's3://{s3_bucket}/{table}',
        dataset=True,
        database=database,
        table=table,
        mode='overwrite_partitions',
        partition_cols=['user_id'],
    )

    return (
        jsonify({'success': True}), 200, {'Content-Type': 'application/json'}
    )


if __name__ == '__main__':
    app.run(debug=True)
