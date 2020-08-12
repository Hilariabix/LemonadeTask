import logging
import os
import argparse
from dal import DBManager
from collector import WeatherScraper


logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)


class App:

    def __init__(self, db_name):
        self.db_manager = DBManager(db_name)

    def get_new_locations_callback(self):
        def callback(country, location, metaweather_id):
            self.parse_locations([[country, location, metaweather_id]], True)
        return callback

    def parse_locations(self, data, new=False):
        weather_data = WeatherScraper.get_locations_weather_data(data)
        if weather_data:
            self.db_manager.insert_many_to_table(weather_data, 'weather_data')
            if new:
                country = weather_data[0][0]
                country_agg = self.db_manager.get_country_updated_data(country)
            else:
                country_agg = self.db_manager.get_countries_agg_data()
            self.db_manager.insert_many_to_table(list(country_agg), 'country_agg')
        else:
            logger.info('No weather data found')


def main():
    parser = argparse.ArgumentParser(description="Weather Forecast Predictions")
    parser.add_argument("--db", default="lemonade.db", type=os.path.abspath,
                        help="DB full path to connect to.")
    parser.add_argument("--create-tables", action="store_true",
                        help="Whether to create the tables and its tables, or use an existed one.")
    args = parser.parse_args()
    app = App(args.db)
    if args.create_tables:
        app.db_manager.create_tables(app.get_new_locations_callback())
    app.parse_locations(app.db_manager.get_locations_from_db())


if __name__ == "__main__":
    main()
