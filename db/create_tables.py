from db import models

def create_tables(tables):
	for table in tables:
		table.create_table()
	return None

def drop_tables(tables):
	for table in tables:
		table.drop_table()
	return None

def create_all_tables():
	tables = []

	tables.append(models.Country)
	tables.append(models.Medals)

	try:
		drop_tables(tables)
	except:
		pass
	create_tables(tables)


create_all_tables()
