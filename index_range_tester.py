from lstore.db import Database
from lstore.query import Query

db = Database()
db.open("/Users/vsiow/Documents/UCD/W2020/ECS165/ECS165A-DBDeadbodies/")
# db.open("/Users/Kurono/Documents/Junior_Year/Winter_Quarter/ECS165A/DB_Deadbodies_LStore")

# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
for i in range(0, 20): 
    key = 92106429 + i
    if (i % 2 == 0) :
        records[key] = [key, i + 1, 20 + i, 30 + i, 40 + i]
    else :
        records[key] = [key, i + 1, 39 - i, 100, 50 - i]
    print(records[key])
    query.insert(*records[key])

print("running index's locate_range(): ")
print("locate_range on begin key = 20, end key = 30, column = 2 :")
query.table.index.update(2)
print(query.table.index.locate_range(20, 30, 2))
print()

print("locate_range on begin key = 90, end key = 100, column = 3 :")
query.table.index.update(3)
print(query.table.index.locate_range(90, 100, 3))

db.close()