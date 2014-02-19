import httplib

from sqlalchemy import MetaData, Column,\
					ForeignKey, Integer, Float, String, Text, DateTime, Date, PickleType,\
					create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, validates
from sqlalchemy.sql import func

import settings

Base = declarative_base()

class CommonBase(Base):
	__abstract__ = True

	id = Column(Integer, primary_key=True)
	created_on = Column(DateTime, default=func.now())
	updated_on = Column(DateTime, default=func.now(), onupdate=func.now())

	@classmethod
	def create_table(self):
		engine = create_db_engine()
		self.__table__.create(engine)	
		return None

	@classmethod
	def drop_table(self):
		engine = create_db_engine()
		self.__table__.drop(engine)
		return None


class Country(CommonBase):
	__tablename__ = 'country'

	name = Column(String(255), nullable=True)
	medals = relationship("Medals")

	population = Column(Float)
	total_area = Column(Float) # mi^2
	population_density = Column(Float) # people per mi^2
	life_expectancy = Column(Float)
	gdp = Column(Float)
	gdp_per_capita = Column(Float) # per year per person
	public_education_spending = Column(Float)
	health_spending = Column(Float)
	# language spoken
	# ethnicity
	# religion

	def stats(self):
		return ['population', 'total area', 'population density', 'life expectancy', 
			'gdp', 'gdp per capita']#, 'public education spending', 'health spending']

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(name=self.name).all()
		if matches:
			return matches
		return None

	def reload_stats(self, session):
		for item in self.stats():
			value = self.query_wa(item)
			
			print '%s\t%s' % (item,value)

			self.__setattr__(item.replace(' ','_'), value)
			session.merge(self)
			session.commit()
		return

	def query_wa(self, query, appid=settings.WA_APPID):
		query = '%s %s' % (self.name, query)

		endpoint = 'api.wolframalpha.com'

		http = httplib.HTTPConnection(endpoint, 80)
		# http.set_debuglevel(1)
		http.request("GET",'/v2/query?appid=%s&input=%s&format=plaintext' % (appid, query.replace(' ','+')))
		response = http.getresponse().read()

		import xml.etree.ElementTree as ET
		root = ET.fromstring(response)
		answer = [r.text for r in root.iter('plaintext')][1]

		multiplier = {'thousand':1000, 'million': 1000000, 'billion': 1000000000, 'trillion': 1000000000000}
		value = answer.replace('$','').replace('%','').split(' ')

		try: 
			float(value[0])
		except:
			print 'BAD VALUE: %s' % value[0]

			return 0

		return float(value[0])*float(multiplier[value[1]] if multiplier.has_key(value[1]) else 1)

	def __unicode__(self):
		return self.name


class Medals(CommonBase):
	__tablename__ = 'medals'

	country_id = Column(Integer, ForeignKey('country.id'))
	year = Column(String(4), nullable=False)
	gold = Column(Integer)
	silver = Column(Integer)
	bronze = Column(Integer)

	def duplicates(self, session):
		matches = session.query(type(self)).filter_by(country_id=self.country_id).filter_by(year=self.year).all()
		if matches:
			return matches
		return None

	def __unicode__(self):
		return str(self.country_id) + ' ' + self.year


def create_db_engine(db=settings.DB):
	if db['ENGINE']=='sqlite':
		from sqlite3 import dbapi2 as sqlite
		return create_engine('sqlite+pysqlite:///%s' % (db['NAME']), module=sqlite)

	engine = create_engine('%s://%s:%s@%s/%s' \
			% (db['ENGINE'], db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])\
			, use_native_unicode=False) #, client_encoding='utf8')
	return engine

def create_db_session(db=settings.DB):
	engine = create_db_engine(db)
	Session = sessionmaker(bind=engine)
	session = Session()
	return session

def end_db_session(session):
	session.commit()
	session.close()
	return


from sqlalchemy import exc, event
from sqlalchemy.pool import Pool

@event.listens_for(Pool, "checkout")
def check_connection(dbapi_con, con_record, con_proxy):
	'''Listener for Pool checkout events that pings every connection before using.
	Implements pessimistic disconnect handling strategy. See sqlalchemy docs'''
	cursor = dbapi_con.cursor()
	try:
		cursor.execute("SELECT 1")  # could also be dbapi_con.ping(),
									# not sure what is better
	except exc.OperationalError:
		raise exc.DisconnectionError()

