import pandas as pd
# NOTES: 
# The first row is skipped in the file since it just contains the word Book
# Since the warehouse is not given, just placed it in a random warehouse
# Values are only inserted into author, publisher, product, and book tables as these are required based on the schema
# define a function to update string with double single quotes for SQL compatibility

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
    for author in data['Author(s)'].unique():
        author_id = generate_unique_code('author')
        author_id_map[author] = author_id
        inserts.append(f"INSERT INTO AUTHOR(Author_ID, Author_Name) VALUES ({author_id}, '{update_string(author)}');")
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
    default_warehouse_no = 1
    unique_books = data.drop_duplicates(subset=['ISBN'])
    return [f"INSERT INTO PRODUCT(Item_Number, Warehouse_No, Price, Product_Type) VALUES ('{isbn}', {default_warehouse_no}, {price.replace('$', '')}, '{update_string(category)}');" 
            for isbn, price, category in zip(unique_books['ISBN'], unique_books['Price'], unique_books['Category'])]

# get insert statements for books
def get_book_inserts(data):
    book_inserts = []
    joined_data = data.groupby('ISBN').agg({'Publisher': 'first', 'Title': 'first', 'Author(s)': '; '.join}).reset_index()
    
    for _, row in joined_data.iterrows():
        author_ids = ' '.join(str(author_id_map[author]) for author in row['Author(s)'].split('; '))
        publisher_id = publisher_id_map[row['Publisher']]
        book_insert = (f"INSERT INTO BOOK(ISBN, Publisher_ID, Item_Number, Book_Title, Publisher, Author_ID) " f"VALUES ('{row['ISBN']}', {publisher_id}, '{row['ISBN']}', '{update_string(row['Title'])}', '{update_string(row['Publisher'])}', '{update_string(author_ids)}');")
        book_inserts.append(book_insert)
    
    return book_inserts

# read the CSV file and skip the first row with book as the only value
input_data = pd.read_csv('data.csv', encoding='latin-1', skiprows=1)

# update data to handle the case where there are multiple authors
updated_data = add_previous_columns_to_author(input_data)

# get the insert statements for each table
author_table = get_author_inserts(updated_data)
publisher_table = get_publisher_inserts(updated_data)
product_table = get_product_inserts(updated_data)
book_table = get_book_inserts(updated_data)

# output file with sql print statements
with open('script_insert_output.txt', 'w') as file: 
    for inserts in [author_table, publisher_table, product_table, book_table]:
        file.write('\n'.join(inserts) + '\n')
