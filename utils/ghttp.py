# -*- coding: utf-8 -*- 
# created: 2013-05-25
# creator: liguopeng@liguopeng.net
from urlparse import urljoin

import requests

from gcommon.utils.gjsonobj import JsonObject


def download_file(url, target_file):
    r = requests.get(url)
    with open(target_file, "wb") as file:
        file.write(r.content)


Content_Type_XML = 'application/xml'
Content_Type_Plain_Text = 'text/plain'
Content_Type_JSON = 'application/json'
Content_Type_URL_Encoded = 'application/x-www-form-urlencoded; charset=UTF-8'


class Session(object):
    def __init__(self, base_url=""):
        self.base_url = base_url
        self.session = requests.Session()

    def set_base_url(self, base_url):
        self.base_url = base_url

    def get(self, url, **kwargs):
        if not url.lower().startswith("http"):
            url = urljoin(self.base_url, url)

        response = self.session.get(url, **kwargs)

        # if response.headers
        # if response.status_code
        try:
            data = JsonObject.loads(response.text)
        except:
            data = None

        return response, data

    def post(self, url, data, **kwargs):
        if not url.lower().startswith("http"):
            url = urljoin(self.base_url, url)

        response = self.session.post(url, json=data, **kwargs)
        # assert 200 <= response.status_code <= 299

        try:
            data = JsonObject.loads(response.text)
        except:
            data = None

        return response, data

    def put(self, url, data, **kwargs):
        if not url.lower().startswith("http"):
            url = urljoin(self.base_url, url)

        response = self.session.put(url, json=data, **kwargs)

        try:
            data = JsonObject.loads(response.text)
        except:
            data = None

        return response, data

    def delete(self, url, **kwargs):
        if not url.lower().startswith("http"):
            url = urljoin(self.base_url, url)

        response = self.session.delete(url, **kwargs)

        try:
            data = JsonObject.loads(response.text)
        except:
            data = None

        return response, data
