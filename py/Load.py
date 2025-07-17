from DB_connect import createConnection

def Load(Products):
    PSQLConnection = createConnection()
    try:
        PSQLCursor = PSQLConnection.cursor()
    except Exception as e:
        print(e)
        return False

    PSQLTable = "products"
    INSERT = f"insert into {PSQLTable} (product_id, name, price, description) values (%s, %s, %s, %s);"

    ProductTuples = [
        (
            Product["productID"],
            Product["productName"],
            Product["productPrice"],
            Product["productDescription"]
        )
        for Product in Products
    ]

    try:
        PSQLCursor.executemany(INSERT, ProductTuples)
        PSQLConnection.commit()
    except Exception as e:
        print(f"Error: {e}")

    PSQLCursor.close()
    PSQLConnection.close()

if __name__ == "__main__":
    Products = []
    Load(Products)