import pandas as pd

# NOTES: 
# The first row is skipped in the file since it just contains the word Book
# Since the warehouse is not given, just placed it in a random warehouse
# Values are only inserted into author, publisher, product, written by, book tables as these are required based on the schema

# function to update string with single quotes for SQL compatibility
def update_string(string):
    return string.replace("'", "''")


# fill missing values from the previously filled row and handle multiple authors
def add_previous_columns_to_author(data):
    columns = ['ISBN', 'Title', 'Publisher', 'Year', 'Price', 'Category']
    data[columns] = data[columns].ffill()
    return data

# global counters and ID mappings for authors and publishers 
# note these are just arbitraily chosen numbers
author_counter = 501
publisher_counter = 501
author_id_map = {}
publisher_id_map = {}

# generate unique ID values using counters
def generate_unique_code(entity_type):
    #get global counter for author and publisher
    global author_counter, publisher_counter

    #check which type of entity it is and return counter +1
    if entity_type == 'author':
        author_counter += 1
        return author_counter
    elif entity_type == 'publisher':
        publisher_counter += 1
        return publisher_counter

# get insert statements for authors
def get_author_inserts(data):
    #get the mapping of the author ids
    global author_id_map
    inserts = []

    #iterate through all of the rows of data
    for _, row in data.iterrows():
        #get the rows of the authors
        authors = row['Author(s)'].split('; ')

        #iterate through each author 
        for author in authors:
            #add to map if the author is not already in there
            if author not in author_id_map:

                #split the names of the author
                names = author.split()

                #get the first, middle, last name
                first_name = update_string(names[0])
                middle_name = update_string(' '.join(names[1:-1])) if len(names) > 2 else ''
                last_name = update_string(names[-1]) if len(names) > 1 else ''

                #now generate unique id for author id and map the name of the author
                author_id = generate_unique_code('author')
                author_id_map[author] = author_id
                
                #return the author id, first name, middle name, last name
                inserts.append(f"INSERT INTO AUTHOR(Author_ID, First_Name, Middle_Name, Last_Name) VALUES ({author_id}, '{first_name}', '{middle_name}', '{last_name}');")
    return inserts

# get insert statements for publishers
def get_publisher_inserts(data):
    #get the map containing all of the publishers
    global publisher_id_map
    inserts = []

    #iterate through all of the unique publishers
    for pub in data['Publisher'].unique():

        #now get a unique code for the publisher and assign it to the publisher 
        pub_id = generate_unique_code('publisher')
        publisher_id_map[pub] = pub_id

        #return the publisher id and name
        inserts.append(f"INSERT INTO PUBLISHER(Publisher_ID, Publisher_Name) VALUES ({pub_id}, '{update_string(pub)}');")
    return inserts

# get insert statements for products
def get_product_inserts(data):
    #get rid of duplicate books if the ISBNs are the same
    unique_books = data.drop_duplicates(subset=['ISBN'])

    #return the product number, price, and type of product which is a combination of gategory and book
    return [f"INSERT INTO PRODUCT(Product_Number, Price, Product_Type) VALUES ('{isbn}', {price.replace('$', '')}, '{update_string(category)}');" 
            for isbn, price, category in zip(unique_books['ISBN'], unique_books['Price'], unique_books['Category'] + ' Book')]

# # get insert statements for books
# def get_book_inserts(data):
#     inserts = []
#     # get rid of duplicate books if the ISBNs are the same
#     unique_books = data.drop_duplicates(subset=['ISBN'])

#     # iterate through all of the data with unique books
#     for _, row in unique_books.iterrows():
#         # return the book data such as ISBN, Title, and Publisher
#         isbn = row['ISBN']
#         title = update_string(row['Title'])
#         publisher = update_string(row['Publisher'])
#         publisher_id = publisher_id_map[publisher]  # Retrieve publisher ID from the map
#         inserts.append(f"INSERT INTO BOOK(ISBN, Product_Number, Book_Title, Publisher_ID) VALUES ('{isbn}',  '{isbn}', '{title}', {publisher_id});")
#     return inserts
# get insert statements for publishers
# def get_publisher_inserts(data):
#     # get the map containing all of the publishers
#     global publisher_id_map
#     inserts = []

#     # iterate through all of the unique publishers
#     for pub in data['Publisher'].unique():

#         # now get a unique code for the publisher and assign it to the publisher
#         pub_id = generate_unique_code('publisher')
#         publisher_id_map[pub] = pub_id

#         # return the publisher id and name
#         inserts.append(f"INSERT INTO PUBLISHER(Publisher_ID, Publisher_Name) VALUES ({pub_id}, '{pub}');")
#     return inserts

# # # get insert statements for books
# def get_book_inserts(data):
#     inserts = []
#     # get rid of duplicate books if the ISBNs are the same
#     unique_books = data.drop_duplicates(subset=['ISBN'])

#     # iterate through all of the data with unique books
#     for _, row in unique_books.iterrows():

#         # return the book data such as ISBN, Title, and Publisher
#         isbn = row['ISBN']
#         title = update_string(row['Title'])
#         publisher = row['Publisher']  # Use original publisher name
#         inserts.append(f"INSERT INTO BOOK(ISBN,Product_Number, Book_Title, Publisher) VALUES ('{isbn}',  '{isbn}', '{title}', '{publisher}');")
#     return inserts
# get insert statements for books
def get_book_inserts(data):
    inserts = []
    # get rid of duplicate books if the ISBNs are the same
    unique_books = data.drop_duplicates(subset=['ISBN'])

    # iterate through all of the data with unique books
    for _, row in unique_books.iterrows():

        # return the book data such as ISBN, Title, and Publisher
        isbn = row['ISBN']
        title = update_string(row['Title'])
        publisher = row['Publisher']  # Use original publisher name
        
        # Get the Publisher_ID from the publisher_id_map
        publisher_id = publisher_id_map[publisher]
        
        # Include Publisher_ID in the SQL insert statement
        inserts.append(f"INSERT INTO BOOK(ISBN, Product_Number, Book_Title, Publisher_ID) VALUES ('{isbn}',  '{isbn}', '{title}', {publisher_id});")
    return inserts

# get insert statements for written by
def get_written_by_inserts(data):
    inserts = []
    #iterate through all of the rows of data
    for _, row in data.iterrows():

        #get the isbn and author rows
        isbn = row['ISBN']
        authors = row['Author(s)'].split('; ')

        #goes through the authors  and return the author id and matching isbn
        for author in authors:
            author_id = author_id_map[author]
            inserts.append(f"INSERT INTO WRITTEN_BY(Author_ID, ISBN) VALUES ({author_id}, '{isbn}');")
    return inserts

# get insert statements for stores_product
def get_stores_product_inserts(data):
    #choosing a random warehouse to place all products in
    default_warehouse_no = 1 

    #assuming that there is only one book
    default_quantity_no=1

    #make sure there are no duplicate books
    unique_books = data.drop_duplicates(subset=['ISBN'])

    #return the warehouse number, book, and quantity for each book
    return [f"INSERT INTO STORES_PRODUCT(Warehouse_No, Product_Number, Quantity) VALUES ({default_warehouse_no}, '{isbn}', {default_quantity_no});" 
            for isbn in unique_books['ISBN']]

# read the CSV file and skip the first row with book as the only value
input_data = pd.read_csv('Database_Data_Extraction/data.csv', encoding='latin-1', skiprows=1)

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
with open('Database_Data_Extraction/script_insert_output.txt', 'w') as file: 
    for inserts in [author_table, publisher_table, product_table, book_table, written_by_table, stores_product_table]:
        file.write('\n'.join(inserts) + '\n')
