import random
import string
import mysql.connector
import time
import datetime

# MySQL connection details
host = '127.0.0.1'
user = 'root'
password = ''
database = 'classicmodels'

# Establish MySQL connection
db = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = db.cursor()

# Function to generate random string
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

products = ['Widget', 'Gadget', 'Device', 'Tool', 'Appliance','Lumina','FitTech','AromaMist','SweepBot','EcoGrow','PowerSnap','AquaTune','StaView','FreshAir','EverGlow','ChillBliss','FlexiFit','SmartChef','SparkleScent','GlowUp','RevitaRevive','SwiftShade','PawsomePet','LunaDream','SwiftScribe']
names = ['Max', 'Lily', 'Ethan', 'Ava', 'Noah', 'Mia', 'Lucas', 'Zoe', 'Oliver', 'Emma', 'Henry', 'Sophia', 'Leo', 'Olivia', 'Isaac', 'Amelia', 'Liam', 'Harper', 'Benjamin', 'Ella', 'Jacob', 'Emily', 'Daniel', 'Charlotte', 'Logan', 'Chloe', 'Caleb', 'Grace', 'Michael', 'Abigail']
    
    

# Infinite loop
while True:
    # Generate random retail data
    product_name = random.choice(products)
    price = round(random.uniform(10, 100), 2)
    quantity = random.randint(1, 10)
    customer_name = random.choice(names)
    ordered_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Prepare the SQL query
    query = "INSERT INTO retail_data (product_name, price, quantity, customer_name, ordered_at) VALUES (%s, %s, %s, %s, %s)"
    values = (product_name, price, quantity, customer_name, ordered_at)

    try:
        # Execute the SQL query
        cursor.execute(query, values)
        db.commit()
        print("Record inserted successfully!")
    except Exception as e:
        db.rollback()
        print("Error inserting record:", str(e))

    # Sleep for a random interval between 1-5 seconds
    time.sleep(random.randint(1, 5))

# Close the MySQL connection
cursor.close()
db.close()
