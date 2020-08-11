import json
import logging
import requests
from datetime import datetime, timedelta


logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)


class WeatherScraper:
    API_URL = "https://www.metaweather.com/"
    LOCATION_FORECAST_QUERY = "api/location/{woeid}/{date}/"
    DATE_FORMAT = "%Y/%m/%d"

    @staticmethod
    def get_latest_location_forecast(woeid, date):
        """
        Getting the forecast for the required location and dat
        :param woeid: The location id
        :param date: required datetime
        :return:
        """
        # Query to search in the api
        query = WeatherScraper.LOCATION_FORECAST_QUERY.format(
            woeid=woeid, date=date.strftime(WeatherScraper.DATE_FORMAT))
        try:
            latest_location_forecast = requests.get(WeatherScraper._url(query))
            # Takes only the latest that was created
            latest_location_forecast = json.loads(latest_location_forecast.text)[0]
        except ConnectionError:
            logger.error('Failed to establish a new connection ')
            raise
        except KeyError:
            logger.error('Metaweather id not found')
            return {}
        return {'min_temp': latest_location_forecast['min_temp'], 'max_temp': latest_location_forecast['max_temp']}

    @staticmethod
    def _url(path):
        return '{}{}'.format(WeatherScraper.API_URL, path)

    @staticmethod
    def get_locations_weather_data(locations):
        """
        Getting all the locations data
        :param locations: List of lists when each list inside contains country, location and metaweather_id
        :return: List of the locations data
        """
        weather_data = []
        yesterday = datetime.now() - timedelta(days=1)
        for country, location, woeid in locations:
            forecast = WeatherScraper.get_latest_location_forecast(woeid, yesterday)
            if forecast:
                weather_data.append([country, location, forecast["min_temp"], forecast["max_temp"],
                                     woeid, datetime.now().strftime("%d-%m-%Y")])
        return weather_data
