
import sqlite3


def insert_one(cur):
	#cur = db.cursor()
	cur.execute("insert into tb(name) values('one');")
	#cur.execute("select last_insert_rowid();")
	cur.execute("select rowid from tb where rowid=last_insert_rowid();")
	rowid = cur.fetchone()[0]
	print rowid

	cur.execute("select * from tb where rowid=?;", [rowid])
	print cur.fetchone()

	return rowid

db = sqlite3.connect(':memory:')
cur = db.cursor()
cur.execute("""
	create table tb(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name varchar
	);""")

rowid = None

try:
	with db:
		cur.execute("select strftime('%Y-%m-%d %H:%M:%f', 'now');")
		now = cur.fetchone()[0]
		print now

		rowid = insert_one(cur)

		raise Exception('should rollback')
except Exception as e:
	print(e)

print rowid
cur.execute("select * from tb where rowid=?;", [rowid])
print cur.fetchone()

