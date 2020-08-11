# Lemonade
WITH min_diff as(
select created_at, country, location, min(diff) difference, max_temperatures, average_max_temperatures from(
select distinct wd.created_at, wd.country, location, ABS(ca.average_max_temperatures - wd.max_temperatures) diff, wd.max_temperatures, ca.average_max_temperatures
FROM weather_data wd
left join 
 country_agg ca 
on wd.country = ca.country and wd.created_at = ca.created_at
group by wd.location, wd.created_at)
group by country, created_at),
diff as(
select distinct wd.created_at, wd.country, location, ABS(ca.average_max_temperatures - wd.max_temperatures) difference, wd.max_temperatures, ca.average_max_temperatures
FROM weather_data wd
left join 
 country_agg ca 
on wd.country = ca.country and wd.created_at = ca.created_at
group by wd.location, wd.created_at)

select created_at "date", country, location, difference, max_temperatures temperature
from diff
where difference = (select difference
					from  min_diff
					where min_diff.difference = diff.difference 
					and min_diff.country = diff.country
					and min_diff.created_at = diff.created_at
					and min_diff.average_max_temperatures = diff.average_max_temperatures)
