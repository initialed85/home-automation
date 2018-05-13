import datetime
from collections import namedtuple
from threading import RLock

from requests import Session

_BASE_URL = 'http://api.openweathermap.org/data/2.5/weather?appid={0}&lon={1}&lat={2}'


Item = namedtuple('Item', ['timestamp', 'value'])


class SlidingTimeCollection(object):
    def __init__(self, permitted_age):
        self._permitted_age_delta = datetime.timedelta(seconds=permitted_age)

        self._collection = []
        self._lock = RLock()

    def _get_valid(self, now):
        expires = now - self._permitted_age_delta

        return [
            x for x in self._collection if x.timestamp > expires
        ]

    def _prune(self, now):
        self._collection = self._get_valid(now)

    def add(self, value, now=None):
        now = now if now is not None else datetime.datetime.now()

        item = Item(
            timestamp=now,
            value=value,
        )

        with self._lock:
            self._collection.append(item)
            self._prune(now)

        return item

    def get(self, now=None):
        now = now if now is not None else datetime.datetime.now()

        with self._lock:
            valid = self._get_valid(now)
            self._collection = valid

        return [
            x.value for x in valid
        ]


class RequestsPerMinuteExceededError(Exception):
    pass


_MINUTE = 60


class OpenWeatherMapClient(object):
    def __init__(self, api_key, lon, lat, requests_per_minute):
        self._api_key = api_key
        self._lon = lon
        self._lat = lat
        self._requests_per_minute = requests_per_minute

        self._session = Session()
        self._sliding_time_collection = SlidingTimeCollection(
            permitted_age=_MINUTE,
        )

        self._last_update = None

        self._timestamp = None
        self._cloud_percent = None
        self._temperature = None
        self._humidity = None
        self._pressure = None
        self._wind_speed = None
        self._wind_direction = None
        self._rain_mm = None
        self._snow_mm = None

    def _needs_update(self):
        return self._last_update is None or (self._last_update - datetime.datetime.now()).total_seconds() > _MINUTE

    @property
    def timestamp(self):
        if self._needs_update():
            self._update()

        return self._timestamp

    @property
    def cloud_percent(self):
        if self._needs_update():
            self._update()

        return self._cloud_percent

    @property
    def temperature(self):
        if self._needs_update():
            self._update()

        return self._temperature

    @property
    def humidity(self):
        if self._needs_update():
            self._update()

        return self._humidity

    @property
    def pressure(self):
        if self._needs_update():
            self._update()

        return self._pressure

    @property
    def wind_speed(self):
        if self._needs_update():
            self._update()

        return self._wind_speed

    @property
    def wind_direction(self):
        if self._needs_update():
            self._update()

        return self._wind_direction

    @property
    def rain_mm(self):
        if self._needs_update():
            self._update()

        return self._rain_mm

    @property
    def snow_mm(self):
        if self._needs_update():
            self._update()

        return self._snow_mm

    def _update(self):
        self._sliding_time_collection.add(None)

        if len(self._sliding_time_collection.get()) > self._requests_per_minute:
            raise RequestsPerMinuteExceededError('limit of {0} requests per minute exceeded'.format(
                self._requests_per_minute
            ))

        with self._session as s:
            r = s.get(
                _BASE_URL.format(self._api_key, self._lon, self._lat),
                timeout=5
            )

            result = r.json()

            timestamp = datetime.datetime.fromtimestamp(result.get('dt'))
            cloud_percent = result.get('clouds', {}).get('all')
            temperature = result.get('main', {}).get('temp')
            temperature = temperature - 273.15 if temperature is not None else None
            humidity = result.get('main', {}).get('humidity')
            pressure = result.get('main', {}).get('pressure')
            wind_speed = result.get('wind', {}).get('speed')
            wind_speed = wind_speed * 3.6 if wind_speed is not None else None
            wind_direction = result.get('wind', {}).get('deg')
            rain_mm = result.get('rain', {}).get('3h')
            snow_mm = result.get('snow', {}).get('3h')

            self._timestamp = timestamp
            self._cloud_percent = cloud_percent
            self._temperature = temperature
            self._humidity = humidity
            self._pressure = pressure
            self._wind_speed = wind_speed
            self._wind_direction = wind_direction
            self._rain_mm = rain_mm
            self._snow_mm = snow_mm

            self._last_update = datetime.datetime.now()


def create_weather_client(api_key, lon, lat, requests_per_minute=60):
    return OpenWeatherMapClient(
        api_key=api_key,
        lon=lon,
        lat=lat,
        requests_per_minute=requests_per_minute,
    )
