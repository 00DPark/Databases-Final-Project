import pandas as pd

# NOTES: 
# The first row is skipped in the file since it just contains the word Book
# Since the warehouse is not given, just placed it in a random warehouse
# Values are only inserted into author, publisher, product, written by, book tables as these are required based on the schema

# function to update string with double single quotes for SQL compatibility
def update_string(string):
    return string.replace("'", "''")

# fill missing values from the previously filled row and handle multiple authors
def add_previous_columns_to_author(data):
    columns = ['ISBN', 'Title', 'Publisher', 'Year', 'Price', 'Category']
    data[columns] = data[columns].ffill()
    return data

# global counters and ID mappings for authors and publishers
author_counter = 501
publisher_counter = 501
author_id_map = {}
publisher_id_map = {}

# Generate unique ID values using counters
def generate_unique_code(entity_type):
    global author_counter, publisher_counter
    if entity_type == 'author':
        author_counter += 1
        return author_counter
    elif entity_type == 'publisher':
        publisher_counter += 1
        return publisher_counter

# get insert statements for authors
def get_author_inserts(data):
    global author_id_map
    inserts = []
    for _, row in data.iterrows():
        authors = row['Author(s)'].split('; ')
        for author in authors:
            if author not in author_id_map:
                names = author.split()
                first_name = update_string(names[0])
                middle_name = update_string(' '.join(names[1:-1])) if len(names) > 2 else ''
                last_name = update_string(names[-1]) if len(names) > 1 else ''
                author_id = generate_unique_code('author')
                author_id_map[author] = author_id
                inserts.append(f"INSERT INTO AUTHOR(Author_ID, First_Name, Middle_Name, Last_Name) VALUES ({author_id}, '{first_name}', '{middle_name}', '{last_name}');")
    return inserts

# get insert statements for publishers
def get_publisher_inserts(data):
    global publisher_id_map
    inserts = []
    for pub in data['Publisher'].unique():
        pub_id = generate_unique_code('publisher')
        publisher_id_map[pub] = pub_id
        inserts.append(f"INSERT INTO PUBLISHER(Publisher_ID, Publisher_Name) VALUES ({pub_id}, '{update_string(pub)}');")
    return inserts

# get insert statements for products
def get_product_inserts(data):
    unique_books = data.drop_duplicates(subset=['ISBN'])
    return [f"INSERT INTO PRODUCT(Product_Number, Price, Product_Type) VALUES ('{isbn}', {price.replace('$', '')}, '{update_string(category)}');" 
            for isbn, price, category in zip(unique_books['ISBN'], unique_books['Price'], unique_books['Category'] + ' Book')]

# get insert statements for books
def get_book_inserts(data):
    book_inserts = []
    unique_books = data.drop_duplicates(subset=['ISBN'])
    for _, row in unique_books.iterrows():
        isbn = row['ISBN']
        title = update_string(row['Title'])
        publisher = update_string(row['Publisher'])
        book_inserts.append(f"INSERT INTO BOOK(ISBN,Product_Number, Book_Title, Publisher) VALUES ('{isbn}',  '{isbn}', '{title}', '{publisher}');")
    return book_inserts

# get insert statements for written by
def get_written_by_inserts(data):
    written_by_inserts = []
    for _, row in data.iterrows():
        isbn = row['ISBN']
        authors = row['Author(s)'].split('; ')
        for author in authors:
            author_id = author_id_map[author]
            written_by_inserts.append(f"INSERT INTO WRITTEN_BY(Author_ID, ISBN) VALUES ({author_id}, '{isbn}');")
    return written_by_inserts

# get insert statements for stores_product
def get_stores_product_inserts(data):
    default_warehouse_no = 1
    default_quantity_no=1
    unique_books = data.drop_duplicates(subset=['ISBN'])
    return [f"INSERT INTO STORES_PRODUCT(Warehouse_No, Product_Number, Quantity) VALUES ({default_warehouse_no}, '{isbn}', {default_quantity_no});" 
            for isbn in unique_books['ISBN']]

# read the CSV file and skip the first row with book as the only value
input_data = pd.read_csv('data.csv', encoding='latin-1', skiprows=1)

# update data to handle the case where there are multiple authors
updated_data = add_previous_columns_to_author(input_data)

# get the insert statements for each table
author_table = get_author_inserts(updated_data)
publisher_table = get_publisher_inserts(updated_data)
product_table = get_product_inserts(updated_data)
book_table = get_book_inserts(updated_data)
written_by_table = get_written_by_inserts(updated_data)
stores_product_table = get_stores_product_inserts(updated_data)

# output file with sql print statements
with open('script_insert_output.txt', 'w') as file: 
    for inserts in [author_table, publisher_table, product_table, book_table, written_by_table, stores_product_table]:
        file.write('\n'.join(inserts) + '\n')
