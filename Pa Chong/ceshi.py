import pymysql
dfs = 'dddff'
ds = 'dddss'
database = 'mysql'
host = 'localhost'
user = 'root'
key = 'awr159753bnm'
mysqlDB = 'LiePinData'
db = pymysql.connect(host, user, key, mysqlDB)
cursor = db.cursor()
if database == 'mysql':
   
    sql = """CREATE TABLE `liepin32` (
            `JobTitle` CHAR,
            `company` CHAR,
            )"""
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()