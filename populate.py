import sqlite3
con = sqlite3.connect("shipment_database.db")

def populate_shipments_from_one(filename):
    with(open(filename , 'r')) as file:
        lines = file.readlines()
        for line in lines[1:]:
            data = line.strip().split(',')
            origin, destination, product, quantity = data[0], data[1], data[2], data[4]
            
            product_id = get_product_id(product)
            
            cur = con.cursor()
            cur.execute("INSERT INTO shipment (origin, destination, product_id, quantity) VALUES (?, ?, ?, ?)", (origin, destination, product_id, quantity))
            con.commit()
            
def populate_shipments_from_two(filename_1, filename_2):
    shipments = {}
    
    with open(filename_1, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:
            data = line.strip().split(',')
            shipment_id, product = data[0], data[1]
            
            if shipment_id not in shipments:
                shipments[shipment_id] = {}
            if product not in shipments[shipment_id]:
                shipments[shipment_id][product] = 1
            else:
                shipments[shipment_id][product] += 1
    
    with open(filename_2, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:
            data = line.strip().split(',')
            shipment_id, origin, destination = data[0], data[1], data[2]
            
            if shipment_id in shipments:
                for product, quantity in shipments[shipment_id].items():
                    product_id = get_product_id(product)
                    cur = con.cursor()
                    cur.execute("INSERT INTO shipment (origin, destination, product_id, quantity) VALUES (?, ?, ?, ?)", (origin, destination, product_id, quantity))
                    con.commit()
            
def get_product_id(product):
    product_id = db_lookup_product_id(product)
    if product_id is None:
        product_id = db_create_product(product)
    return product_id

def db_lookup_product_id(product):
    cur = con.cursor()
    cur.execute("SELECT id FROM product WHERE name = ?", (product,))
    row = cur.fetchone()

    return row[0] if row else None

def db_create_product(product):
    cur = con.cursor()
    cur.execute("INSERT INTO product (name) VALUES (?)", (product,))
    con.commit()
    return cur.lastrowid
            
            
print("Populating shipments from one...")
populate_shipments_from_one('data/shipping_data_0.csv')
print("Done")

print("Populating shipments from two...")
populate_shipments_from_two('data/shipping_data_1.csv', 'data/shipping_data_2.csv')
print("Done")

print("Printing all shipment rows...")
cur = con.cursor()
cur.execute("SELECT * FROM shipment")
rows = cur.fetchall()

for row in rows:
    if input("Print row? (y/n): ").lower() == 'y':
        print(row)
    else:
        break

print("Printing all product rows...")
cur = con.cursor()
cur.execute("SELECT * FROM product")
rows = cur.fetchall()

for row in rows:
    if input("Print row? (y/n): ").lower() == 'y':
        print(row)
    else:
        break
con.close()
