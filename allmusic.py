from pyquery import PyQuery as pq
import requests
import json
import billboard
import re
import time
import datetime
import sys
import pymysql.cursors
import difflib
from fuzzywuzzy import fuzz

allmusic_song_search_base = 'http://www.allmusic.com/search/songs/'
SONG_QUERY_TITLE = ''
SONG_QUERY_ARTIST = ''
SONG_QUERY = SONG_QUERY_ARTIST + ' ' + SONG_QUERY_TITLE
# KNOWN_ARTISTS = ['meghan trainor']
NUM_SONG_SEARCH_RESULTS = 3
featuring_regex = r'\s(ft\.?|feat\.?|[fF]eaturing|\/|[xX]|[aA]nd|&|[wW]ith)\s'
date_format = "%Y-%m-%d"
start_date = datetime.datetime.strptime('1980-01-01', date_format).date()
week_ago = datetime.timedelta(days=-7)
next_week = datetime.timedelta(days=7)
end_date = datetime.datetime.strptime('1959-01-01', date_format).date()
songs_considered = {}
song_to_album_dict = {}
albums_considered = {}
REQUEST_DELAY = 1
TOP_N = 100
MODE = ''

def url_to_id(url):
    return url[url.rfind('/') + 1:]

def main(argv):
    if len(argv) < 4:
        print('usage: allmusic.py out_file start_date end_date mode [topN]')
        return

    out_file_name = argv[0]
    start_date = datetime.datetime.strptime(argv[1], date_format).date()
    end_date = datetime.datetime.strptime(argv[2], date_format).date()
    MODE = argv[3]
    if MODE not in ['songs', 'albums']:
        print('invalid mode:', MODE, 'valid modes are "songs" and "albums"')
        return

    if len(argv) == 5:
        TOP_N = int(argv[4])

    if MODE == 'songs':
        results = {'songs': []}
    else:
        results = {'songs': [], 'albums': []}

    date = start_date

    connection = pymysql.connect(
        host='',
        port=0,
        user='',
        password='',
        db='',
        cursorclass=pymysql.cursors.DictCursor
    )

    print('searching from', start_date, 'until', end_date)
    while date <= end_date:
        print(date)
        charts = billboard.ChartData('hot-100', date=date.strftime(date_format))
        for chart_song in charts[:TOP_N]:
            if str(chart_song) in songs_considered:
                continue

            songs_considered.add(str(chart_song))
            print('searching for:', chart_song)

            result = chart_search(chart_song)
            if result is not None:
                song, album_info, song_index = result
                if song is None:
                    print('NOTE: no songs found for', chart_song)
                    continue

                diff = fuzz.token_set_ratio(chart_song.title.lower(), song['title']['name'])
                if diff <= 70:
                    print('NOTE: wrong song found? chart:', chart_song.title, 'vs.', song['title'])

                if MODE == 'albums':
                    if len(song['composers']) > 0:
                        results['songs'].append(song)

                    results['albums'].append(album_info)
                else:
                    # song only with chart pos
                    ret_song = song
                    ret_song['peakPos'] = chart_song.peakPos
                    ret_song['weeks'] = chart_song.weeks
                    ret_song['spotify_id'] = chart_song.spotifyID
                    ret_song['billboard_title'] = chart_song.title
                    ret_song['billboard_artist'] = chart_song.artist

                    if album_info is not None:
                        ret_song['album'] = album_info['album']
                    else:
                        ret_song['album'] = None
                        print('NOTE: no album found for', chart_song)

                    results['songs'].append(ret_song)
                    composers = ret_song['composers']
                    if song_index is not None:
                        album_writers = album_info['tracks'][song_index]['writers']
                    else:
                        album_writers = []
                    print('writers from song page:', composers)
                    print('writers from album page:', album_writers)
                    unique_writers = set(json.dumps(d, sort_keys=True) for d in (composers + album_writers))
                    writers = [json.loads(s) for s in unique_writers]

                    print('union writers:', writers)
                    del ret_song['composers']
                    ret_song['writers'] = writers

                    song_to_db(connection, ret_song, date)

            print()
            time.sleep(REQUEST_DELAY)
        date += next_week

    connection.close()

def song_to_db(connection, song, curr_date):
    print('adding', song, 'to the database.')

    sql_add_person = 'insert into `Artist` (`name`, `artist_id`) values (%s, %s)'
    sql_check_song_date_weeks = 'select `first_appearance`, `weeks` from `Song` where `song_id` = %s'
    sql_update_song_date_weeks = 'update `Song` set `first_appearance` = %s, `weeks` = %s where `song_id` = %s'
    sql_add_song = 'insert into `Song` (`allmusic_title`, `billboard_title`, `billboard_artist`, `song_id`, `album_appears_on`, `peak_position`, `first_appearance`, `weeks`, `spotify_id`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    sql_add_album = 'insert into `Album` (`main_artist`, `title`, `album_id`, `release_date`) values (%s, %s, %s, %s)'
    sql_add_writer = 'insert into `Writes` (`artist_id`, `song_id`) values (%s, %s)'
    sql_add_performer = 'insert into `Performs` (`artist_id`, `song_id`) values (%s, %s)'

    try:
        # add song to db
        with connection.cursor() as cursor:
            cursor.execute(sql_check_song_date_weeks, (url_to_id(song['title']['url']),))
            result = cursor.fetchone()
            if result is not None:
                # songs exists, update the date and the weeks
                print(result)
                db_date = result['first_appearance']
                db_weeks = result['weeks']
                if curr_date < db_date:
                    # find the earliest date
                    db_date = curr_date

                if song['weeks'] > db_weeks:
                    # find the most amount of weeks
                    db_weeks = song['weeks']

                cursor.execute(sql_update_song_date_weeks, (str(db_date), db_weeks, url_to_id(song['title']['url'])))
            else:
                # song does not exist, add it
                if song['album'] is None:
                    album_add = None
                    album_add_id = None
                else:
                    album_add = song['album']['url']
                    album_add_id = url_to_id(album_add)

                cursor.execute(sql_add_song, (song['title']['name'], song['billboard_title'], song['billboard_artist'], url_to_id(song['title']['url']), album_add_id, song['peakPos'], str(curr_date), song['weeks'], song['spotify_id']))
    except Exception as e:
        print('error:', e)
    finally:
        connection.commit()

    # add album if one to database
    if song['album'] is not None:
        try:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(sql_add_album, (url_to_id(song['album']['artist']['url']), song['album']['title'], url_to_id(song['album']['url']), str(song['album']['release_date'])))
                except pymysql.err.IntegrityError:
                    pass
        except Exception as e:
            print('error:', e)
        finally:
            connection.commit()

    try:
        # add writer relationship
        with connection.cursor() as cursor:
            for writer in song['writers']:
                try:
                    cursor.execute(sql_add_writer, (url_to_id(writer['url']), url_to_id(song['title']['url'])))
                except pymysql.err.IntegrityError:
                    pass

                try:
                    cursor.execute(sql_add_person, (writer['name'], url_to_id(writer['url'])))
                except pymysql.err.IntegrityError:
                    pass
    except Exception as e:
        print('error:', e)
    finally:
        connection.commit()

    try:
        # add performs relationship
        with connection.cursor() as cursor:
            for performer in song['performers']:
                try:
                    cursor.execute(sql_add_performer, (url_to_id(performer['url']), url_to_id(song['title']['url'])))
                except pymysql.err.IntegrityError:
                    pass

                try:
                    cursor.execute(sql_add_person, (performer['name'], url_to_id(performer['url'])))
                except pymysql.err.IntegrityError:
                    pass
    except Exception as e:
        print('error:', e)
    finally:
        connection.commit()

def song_search(song, num_results):
    url = allmusic_song_search_base + song
    req = requests.get(url, headers={
        'Host': 'www.allmusic.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8'
    })
    if req.status_code != 200:
        return {'error': req.text}

    d = pq(req.text)
    songs = d('ul.search-results li.song')
    num_to_get = min(num_results, len(songs))
    song_list = []
    for song in songs[:num_to_get]:
        song_list.append(song_to_dict(song))

    return {'songs': song_list}

def song_to_dict(song):
    '''Converts a song in HTML (an <li> element from allmusic.com)
    into json.'''
    d = pq(song)

    song_dict = {}

    # get title
    title_anchor = d('div.title a').eq(0)
    title_url = title_anchor.attr('href')
    title_text = title_anchor.text().strip('"')
    song_dict['title'] = {'name': title_text, 'url': title_url}

    # get performer
    performer_list = []
    performer_anchors = d('div.performers a')
    for i in range(len(performer_anchors)):
        performer_anchor = performer_anchors.eq(i)
        performer_name = performer_anchor.text()
        performer_url = performer_anchor.attr('href')
        performer_list.append({'name': performer_name, 'url': performer_url})

    song_dict['performers'] = performer_list

    # get composer
    composer_list = []
    composer_anchors = d('div.composers a')
    for i in range(len(composer_anchors)):
        composer_anchor = composer_anchors.eq(i)
        composer_list.append({
            'name': composer_anchor.text(),
            'url': composer_anchor.attr('href')
        })

    song_dict['composers'] = composer_list
    return song_dict

def song_to_albums(song, song_url):
    req = requests.get(song_url, headers={
        'Host': 'www.allmusic.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8'
    })
    if req.status_code != 200:
        return {'error': req.text}

    d = pq(req.text)

    album_rows = d('section.appearances tr[itemprop="inAlbum"]')
    album_list = []
    for i in range(len(album_rows)):
        row = album_rows.eq(i)
        album = row('td.artist-album')

        # get artists
        artist_anchor = album('div.artist span[itemprop="name"] a').eq(0)
        artist = {
            'name': artist_anchor.text(),
            'url': artist_anchor.attr('href')
        }

        # get album title
        title_anchor = album('div.title a')
        title_name = title_anchor.text().strip('"')
        title_url = title_anchor.attr('href')

        # get album year
        year = row('td.year').text().strip()

        album_list.append({
            'artist': artist,
            'title': title_name,
            'url': title_url,
            'year': year
        })

    return {'song': song, 'albums': album_list}

def album_to_tracks(album, album_url):
    req = requests.get(album_url, headers={
        'Host': 'www.allmusic.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8'
    })
    if req.status_code != 200:
        return {'error': req.text}

    d = pq(req.text)
    track_list = []
    track_trs = d('section.track-listing tr.track')
    for i in range(len(track_trs)):
        track = track_trs.eq(i)
        # get title
        title_anchor = track('td.title-composer div.title a').eq(0)
        title_name = title_anchor.text().strip('"')
        title_url = title_anchor.attr('href')

        # get performers
        performer_list = []
        performer_anchors = track('td.performer a')
        for j in range(len(performer_anchors)):
            performer_anchor = performer_anchors.eq(j)
            performer_type = performer_anchor.parent('div').attr('class')
            performer_name = performer_anchor.text()
            performer_url = performer_anchor.attr('href')
            performer_list.append({
                'name': performer_name,
                'url': performer_url,
                'role': performer_type
            })

        # get writers
        writer_list = []
        writer_anchors = track('td.title-composer div.composer a')
        for j in range(len(writer_anchors)):
            writer_anchor = writer_anchors.eq(j)
            writer_name = writer_anchor.text()
            writer_url = writer_anchor.attr('href')
            writer_list.append({
                'name': writer_name,
                'url': writer_url
            })

        # get time
        time = track('td.time').text().strip('"')

        track_list.append({
            'performers': performer_list,
            'time': time,
            'title': {
                'name': title_name,
                'url': title_url
            },
            'writers': writer_list
        })

    # finally get album release date
    release_date_text = d('div.release-date span').text()
    print(release_date_text)
    album['release_date'] = None
    if len(release_date_text) > 0:
        date_formats = [
            "%B %d, %Y",
            "%B, %Y",
            "%Y",
            "%Y's"
        ]
        for date_format in date_formats:
            try:
                release_date = datetime.datetime.strptime(release_date_text, date_format).date()
                album['release_date'] = release_date
            except ValueError:
                print(date_format, 'did not work')
                continue

    if album['release_date'] is None:
        print('NOTE:', album, 'does not have a release date?')

    return {'album': album, 'tracks': track_list}

def song_search_matching(chart_song, query):
    """
    Search for all songs matching the given chart song, with the given query
    and artist from the query
    """
    song_searches = song_search(query, NUM_SONG_SEARCH_RESULTS)
    if 'error' in song_searches:
        print('>>> error:', song_searches['error'])
        return

    songs = []
    # print(song_searches)
    for s in song_searches['songs']:
        # print('test song:', s)
        performers = ' '.join(x['name'] for x in s['performers']).lower()

        print('checking performers:', performers, 'vs.', chart_song.artist.lower())
        print('checking titles:', '"' + s['title']['name'] + '"', 'vs.', '"' + chart_song.title + '"')
        diff1 = fuzz.token_set_ratio(chart_song.artist.lower(), performers)
        diff2 = difflib.SequenceMatcher(
            None,
            a=s['title']['name'].lower(),
            b=chart_song.title.lower()
        ).ratio()
        print('performer score:', diff1, 'and title score:', diff2)
        if diff1 >= 65 and diff2 > 0.75:
            songs.append(s)
            print('song passed with diff performers of', diff1, 'and diff title of', diff2)
            if diff1 <= 75 or diff2 < 0.85:
                print('NOTE impartial match?', s, 'for', chart_song)

    return songs

def chart_search(chart_song):
    print('>>> searching for:', chart_song)
    main_artist = [
        a.strip() for a in re.split(featuring_regex, chart_song.artist, 1)
    ][0]
    possible_queries = [
        {'name': 'normal', 'query': (main_artist + ' ' + chart_song.title, main_artist)},
        {'name': 'reverse', 'query': (chart_song.title + ' ' + main_artist, main_artist)},
    ]
    if '$' in main_artist:
        possible_queries.append({
            'name': 'normal-special',
            'query': (main_artist.replace('$', 's') + ' ' + chart_song.title, main_artist.replace('$', 's'))
        })
        possible_queries.append({
            'name': 'reverse-special',
            'query': (chart_song.title + ' ' + main_artist.replace('$', 's'), main_artist.replace('$', 's'))
        })
    best_song = None
    for query in possible_queries:
        print('trying query:', query['name'])
        print(query['query'][0])
        songs = song_search_matching(chart_song, query['query'][0])
        print('potential songs:')
        print(songs)
        if songs is None:
            print('songs are None? for', chart_song)
            continue
        if len(songs) > 0:
            working_artist = query['query'][1]
            for song in songs:
                if best_song is None:
                    best_song = song
                if len(best_song['composers']) == 0 and len(song['composers']) > 0:
                    best_song = song

                result = song_find_album_writers(song)
                if result is not None:
                    album_info, song_index = result

                if (
                    result is not None and song_index is not None
                    and len(album_info['tracks'][song_index]['writers']) > 0
                ):
                    return song, album_info, song_index

    # couldn't find an album with writers, but (hopefully) we found an individual
    # song with writers
    return (best_song, None, None)

def song_find_album_writers(song):
    print(song['title'], 'by', ', '.join(str(x) for x in song['performers']))

    song_url = song['title']['url']
    if song_url in song_to_album_dict:
        albums = song_to_album_dict[song_url]
    else:
        time.sleep(REQUEST_DELAY)
        albums = song_to_albums(song, song_url)
        song_to_album_dict[song_url] = albums

    if 'error' in albums:
        print('>>> error:', albums['error'])
        return
    if len(albums['albums']) == 0:
        print('>>> no albums found for song.')
        return

    print('>>> potential albums...')
    for a in albums['albums'][:min(5, len(albums['albums']))]:
        print(a['title'], 'by', a['artist']['name'], '(' + a['url'] + ')')
    if len(albums['albums']) > 5:
        print('and', len(albums['albums']) - 5, 'more')

    album = None
    for a in albums['albums']:
        result = check_album(a, song)
        if result is not None:
            album_info, song_index = result
            album = a
            break

    if album is None:
        print('>>> no matching albums found...')
        return

    print('>>> found album:')
    print(album['title'], 'by', album['artist']['name'], '(' + album['url'] + ')')

    # if album_tup in albums_considered:
    #     print('already have album in data.')
    #     return

    if album['url'] not in albums_considered:
        albums_considered[album['url']] = album_info

    # print('RESULTS')
    return (album_info, song_index)

def check_album(album, song):
    print('checking album:', album, 'for', song)
    album_okay = (
        "now that's what i call" not in album['title'].lower()
        and len(album['artist']['name']) > 0
        and any(
            difflib.SequenceMatcher(
                isjunk = lambda x: x in " \t.",
                a=album['artist']['name'].lower(),
                b=x['name'].lower()
            ).ratio() >= 0.6
            for x in song['performers']
        )
    )
    if not album_okay:
        return None

    if album['url'] in albums_considered:
        album_info = albums_considered[album['url']]
    else:
        time.sleep(REQUEST_DELAY)

    # find the song on the album that matches the one we're looking for
        album_info = album_to_tracks(album, album['url'])

    if 'tracks' not in album_info:
        print('NOTE:', album, 'has no tracks?')
        return None

    tracks = album_info['tracks']
    song_index = None
    for i in range(len(tracks)):
        track = tracks[i]
        if (
            fuzz.token_set_ratio(track['title']['name'].lower(), song['title']['name'].lower()) >= 60
            and len(track['writers']) > 0
        ):
            song_index = i
            break

    return album_info, song_index

if __name__ == '__main__':
    main(sys.argv[1:])
