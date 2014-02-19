# http://products.wolframalpha.com/api/documentation.html

import httplib

from db import models

import settings

def olympic_stats(year, season, appid=settings.WA_APPID):
	endpoint = 'api.wolframalpha.com'

	http = httplib.HTTPConnection(endpoint, 80)
	# http.set_debuglevel(1)
	http.request("GET",'/v2/query?appid=%s&input=%s+%s+olympic+medal+counts&format=plaintext&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More&podstate=OlympicMedalistResults:OlympicData__More' % (appid, year, season))
	response = http.getresponse().read()

	import xml.etree.ElementTree as ET
	root = ET.fromstring(response)
	medal_chart = [r.text for r in root.iter('plaintext')][2]

	return medal_chart

def get_medals(years, season):
	for year in years:
		print year

		medal_chart = olympic_stats(str(year), season)
		rows = medal_chart.split('\n')

		session = models.create_db_session()
		for row in rows:
			data = row.split(' | ')
			if data[0]=='country': # header
				continue

			print data[0]

			c = models.Country(name=data[0])
			matches = c.duplicates(session)
			if not matches:
				session.add(c)
				session.commit()
				c.reload_stats(session)
			else:
				c = matches[0]
			m = models.Medals(country_id=c.id, year=str(year), gold=data[1], silver=data[2], bronze=data[3])
			matches = m.duplicates(session)
			if not matches:
				session.add(m)
				session.commit()
		models.end_db_session(session)

summer_years = range(1992,2012+1,4)
summer_years.reverse()

winter_years = range(1992,1992+1,4) + range(1994,2014+1,4)
winter_years.reverse()

get_medals(winter_years, 'winter')
get_medals(summer_years, 'summer')
