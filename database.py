import mysql.connector
from mysql.connector import errorcode
import csv
import os
from datetime import datetime

# Connect to sql server, create a cursor object to interact with the server, get paths for the files
dbc = mysql.connector.connect(host="127.0.0.1", user="root", password="root");
myc = dbc.cursor()
foodsURL = os.path.join(os.getcwd(), 'food.csv')
invoicesURL = os.path.join(os.getcwd(), 'invoices.csv')
storeURL = os.path.join(os.getcwd(), 'store.csv')

database="storeDB"

# Create 3 tables for insertion into the db above
foodTable= """
CREATE TABLE food (fname varchar(50) not null,
classification varchar(50),
price smallint,
primary key(fname)); """

invoiceTable = """
CREATE TABLE invoice (ID bigint not null,
fname varchar(50),
storeID smallint,
quantity smallint,
date datetime,
primary key(ID, fname),
foreign key(storeID) references store(storeID),
foreign key(fname) references food(fname)); """

storeTable = """
CREATE TABLE store (storeID smallint not null,
city varchar(30),
address varchar(50),
primary key(storeID)); """

# Creates a db if it isnt already created, if so, gives error
def db_create(c, db):

   try:
        c.execute(f"CREATE DATABASE {db}")

   except Exception as err:
        print(f"{err}")


# Get data from csv files
def getWords(url):

   dataList = []

   # Open the file with the url and read each row
   with open(url, newline='') as csvfile:
      heading = next(csvfile)
      reader = csv.reader(csvfile)

      for row in reader:

         dataList.append(row)

   return dataList


#Creates the tables, gives error if not
def table_create(c, table):

   try:
        c.execute(table)
        print(f"Table created successfully")

   except Exception as err:
        print(f"{err}")

def view_creator(view):

   # Create the necessary views
   try:

      myc.execute(view)

   except Exception as err:

      print(f"Something went wrong: {err}")


# Get all the data from the 3 files, set name-variables for easier use in formatting
foodData = getWords(foodsURL)
invoiceData = getWords(invoicesURL)
storeData = getWords(storeURL)

# Try using the db if exists, else create one. 
# when used/created, also try creating tables if not exist
try:
    myc.execute(f"USE {database}")


    table_create(myc, foodTable)
    table_create(myc, storeTable)
    table_create(myc, invoiceTable)


except mysql.connector.Error as err:
    print(f"Database {database} does not exist")

    # If db not exist, create it
    # Set it as a value in the sql server connection so we can use it without USE
    if err.errno == errorcode.ER_BAD_DB_ERROR:
       db_create(myc, database)
       print(f"Database {database} created succesfully.")
       dbc.database = database

       table_create(myc, foodTable)
       table_create(myc, storeTable)
       table_create(myc, invoiceTable)
       print("Tables created successfully")

# For each row in the datalist from planets, insert the values
# in their respective columns, commit it so it is saved. Also catch errors and print them
for dataRow in foodData:
   
   try: 
      insertVal1 = """insert into food(fname,classification,price) 
                     values(%s, %s, %s)"""

      myc.execute(insertVal1, (dataRow[0],dataRow[1],dataRow[2]))

      dbc.commit()

   except Exception as err:
        print(f"Tuple skipped (Food Table): {err}")

# Same as above but for species table
for dataRow in storeData:

   try:

      insertVal2 = """insert into store (storeID,city,address) 
                     values(%s, %s, %s)"""

      myc.execute(insertVal2, (dataRow[0],dataRow[1],dataRow[2]))

      dbc.commit()

   except Exception as err:
        print(f"Tuple skipped (Store Table): {err}")

# Same as above but for species table
for dataRow in invoiceData:

   # Make the date-time data into correct format
   d = datetime.strptime(dataRow[4],'%Y-%m-%d %H:%M:%S')
   fd = d.strftime('%Y-%m-%d %H:%M:%S')

   try:

      insertVal3 = """insert into invoice (ID,fname,storeID,quantity,date) 
                     values(%s, %s, %s, %s, %s)"""

      myc.execute(insertVal3, (dataRow[0],dataRow[1],dataRow[2],dataRow[3], fd))

      dbc.commit()

   except Exception as err:
        print(f"Tuple skipped (Invoice Table): {err}")



viewList = ["create view sortQt as select fname, sum(quantity) as qt, month(date) as dt from invoice group by dt,fname",
            "create view maxMonth as select max(qt) as qt, dt from sortQt group by dt",
            "create view storeCash as select storeID, invoice.fname, sum(quantity) as quantity, food.price from invoice inner join food on food.fname = invoice.fname group by storeID, invoice.fname, food.price"]
            
for i in viewList:

   view_creator(i)


while True:

   print("\n" * 25)

   print("1. Select the most popular food by month.")
   print("2. Select income per store.")
   print("3. Get the total amount made from each food classification.")
   print("4. Select the amount of transactions per month.")
   print("5. Selects the revenue per month.")
   print("Q. Quit")
   print("-------------------------------------------------------------------------")
   
   userInp = input("Please choose one option: ")

   # Keep asking for correct user input if user input wrong commands
   if userInp not in "12345q":

      while userInp not in "12345q":

         print("Invalid command, please try again: ")
         userInp = input("Please choose one option")

   if userInp == "1":

      myc.execute("select sortQT.fname, sortQt.qt, sortQt.dt from sortQt, maxMonth where maxMonth.qt = sortQt.qt and maxMonth.dt = sortQt.dt order by dt asc")

      # Styling for the print and prints the planets in rows of 10 planets/r
      print()
      print( " "*37 + "Foodname / Quantity / Month")
      print("-"*110)
      for p in myc:
         print(f"{str(p[0]): <{40}} |  {str(p[1]): <{10}}  |  {str(p[2]): <{10}}")

      print("\n")
      print(input())
      print("\n" * 50)

   if userInp == "2":

      myc.execute("select storeID, sum(price * quantity) as income from storeCash group by storeID")

      # Styling for the print and prints the planets in rows of 10 planets/r
      print()
      print( " "*2 + "Store(ID) / Income")
      print("-"*25)
      for p in myc:
         print(" "*5 + f"{str(p[0]): <{3}} |  {str(p[1])}kr")

      print("\n")
      print(input())
      print("\n" * 50)

   if userInp == "3":

      myc.execute("select classification, sum(food.price * invoice.quantity) as revenue from food, invoice where invoice.fname = food.fname group by classification")

      # Styling for the print and prints the planets in rows of 10 planets/r
      print()
      print( " "*10 + "Classification / Revenue")
      print("-"*40)
      for p in myc:
         print(" "*3 + f"{str(p[0]): <{20}} |  {str(p[1])}kr")

      print("\n")
      print(input())
      print("\n" * 50)

   if userInp == "4":

      myc.execute("select count(month(date)) as transactions, month(date) from ( select distinct date from invoice) as invoice group by month(date)")

      # Styling for the print and prints the planets in rows of 10 planets/r
      print()
      print( " "*2 + "Transactions/ Month")
      print("-"*40)
      for p in myc:
         print(" "*3 + f"{str(p[0]): <{5}} |  {str(p[1])}")

      print("\n")
      print(input())
      print("\n" * 50)


   if userInp == "5":

      myc.execute("select sum(food.price * invoice.quantity) as revenue, month(invoice.date) as month from food inner join invoice on food.fname = invoice.fname group by month(date)")

      # Styling for the print and prints the planets in rows of 10 planets/r
      print()
      print( " "*2 + "Revenue/ Month")
      print("-"*40)
      for p in myc:
         print(" "*3 + f"{str(p[0]): <{5}}kr |  {str(p[1])}")

      print("\n")
      print(input())
      print("\n" * 50)

   # Quits the programme
   if userInp == "q":

      break



# Closes both the connection to the SQL server and the cursor
myc.close()
dbc.close()