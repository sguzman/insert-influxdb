import bs4
import json
import requests


def process_script(script):
    raw_split = script.text.split('\n')
    raw_json = raw_split[1].lstrip('    window["ytInitialData"] = ').rstrip(';')
    return json.loads(raw_json)


def select_script_tag(soup):
    for script in soup.findAll('script'):
        if script.text.startswith('\n    window["ytInitialData"]'):
            return script


def souped(url, headers):
    if headers is None:
        headers = {}

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/69.0.3497.100 Safari/537.36 '
    headers['User-Agent'] = user_agent

    req = requests.get(url, headers=headers)
    soup = bs4.BeautifulSoup(req.text, 'html.parser')

    return soup


def soup_channel(chan_serial):
    url = f'https://www.youtube.com/channel/{chan_serial}'
    return souped(url, None)


def get_subs(json_data):
    raw_str = json_data['header']['c4TabbedHeaderRenderer']['subscriberCountText']['simpleText']
    cleaned = raw_str.rstrip(' subscribers').replace(',', '')

    return int(cleaned)


def get_title(json_data):
    return json_data['header']['c4TabbedHeaderRenderer']['title']


def subs(chan_serial):
    soup = soup_channel(chan_serial)
    script = select_script_tag(soup)
    json_data = process_script(script)

    title = get_title(json_data)
    subs = get_subs(json_data)

    return title, subs