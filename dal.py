import logging
import sqlite3

logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)


class DBManager:

    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)

    @staticmethod
    def last_date_inner_join(table):
        """
        The inner join part of query that will return the latest date
        :param table: The table name
        :return: The part of the query as string
        """
        return """ 
        inner join
        (
            Select max(created_at) as LatestDate, location
            from {table}
            Group by location
        ) SubMax
        on {table}.created_at = SubMax.LatestDate
        and {table}.location = SubMax.location
        """.format(table=table)

    def create_tables(self, load_new_locations):
        self.conn.execute('''CREATE TABLE if not exists  locations
             (id               INTEGER PRIMARY KEY    NOT NULL,
             country           TEXT     NOT NULL,
             location          TEXT     NOT NULL,
             metaweather_id    INTEGER  NOT NULL,
             created_at        DATE);''')
        self.conn.execute('''CREATE TABLE if not exists  weather_data
             (country           TEXT       NOT NULL,
             location           TEXT       NOT NULL,
             min_temperatures   INTEGER    NOT NULL,
             max_temperatures   INTEGER    NOT NULL,
             metaweather_id     INTEGER  NOT NULL,
             created_at         timestamp);''')
        self.conn.execute('''CREATE TABLE if not exists  country_agg
             (country                   TEXT        NOT NULL,
             average_min_temperatures   INTEGER     NOT NULL,
             average_max_temperatures   INTEGER     NOT NULL,
             minimum_temperature        INTEGER     NOT NULL,
             maximum_temperature        INTEGER     NOT NULL,
             created_at                 timestamp);''')
        self.conn.create_function("load_new_locations", 3, load_new_locations)
        cur = self.conn.cursor()
        cur.execute("CREATE TRIGGER IF NOT EXISTS location_added AFTER INSERT ON locations BEGIN "
                    "SELECT load_new_locations(NEW.country, NEW.location, NEW.metaweather_id); END;")

    def get_country_updated_data(self, country):
        """
        Getting the data of an existing country
        :param country: The required country
        :return: The country updated data
        """
        return self.conn.execute("""SELECT country, avg(min_temperatures), avg(max_temperatures), min(min_temperatures),
        max(max_temperatures), LatestDate
        FROM(				
        SELECT distinct *
        FROM weather_data
        {}
        WHERE country = '{}'
        GROUP BY country, LatestDate
                """.format(self.last_date_inner_join('weather_data'), country))

    def get_locations_from_db(self):
        """
        Getting the locations from DB
        :return: Cursor containing the locations
        """
        return self.conn.execute("""SELECT country, location, metaweather_id from(
                            SELECT country, locations.location, metaweather_id, created_at from locations
                            {})""".format(self.last_date_inner_join('locations')))

    def get_countries_agg_data(self):
        """
        Getting the aggregated data for each country
        :return: Cursor containing all the aggregated data
        """
        return self.conn.execute("""SELECT country, avg(min_temperatures), avg(max_temperatures), min(min_temperatures),
        max(max_temperatures), LatestDate
        FROM(			
        SELECT distinct *
        FROM weather_data
        {})
        GROUP BY country""".format(self.last_date_inner_join('weather_data')))

    def insert_many_to_table(self, data, table):
        """
        Inserting bulk of rows into a given table in the database
        :param data: Rows to insert
        :param table:  The table to insert the data into
        """
        args = ', '.join(['?'] * len(data[0]))
        try:
            self.conn.executemany('INSERT INTO {} VALUES({});'.format(table, args), data)
            self.conn.commit()
        except sqlite3.OperationalError:
            logger.error('Tables have not been created yet! Please use the --create-tables argument')
            raise
