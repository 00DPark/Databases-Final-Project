import pandas as pd
# NOTES: 
# The first row is skipped in the file since it just contains the word Book
# Since the warehouse is not given, just placed it in a random warehouse
# Values are only inserted into author, publisher, product, and book tables as these are required based on the schema

# handles the case where a single quote appears in the name of something by replacing single quote with double single quotes
def update_string(string):
    return string.replace("'", "''")

# fills missing values from the previously filled row and handles the case when a book has multiple authors.
def add_previous_columns_to_author(df):
    columns = ['ISBN', 'Title', 'Publisher', 'Year', 'Price', 'Category']
    df[columns] = df[columns].ffill()
    return df

# get author ids separated by a space
def generate_author_ids(authors):
    return ' '.join(str(generate_unique_code(author)) for author in authors)

# generate unique code values using hash code
def generate_unique_code(value):
    return abs(hash(value))

# get insert statements for authors
def get_author_inserts(df):
    authors = df['Author(s)'].unique() 
    return [f"INSERT INTO AUTHOR(Author_ID, Author_Name) VALUES ({generate_unique_code(author)}, '{update_string(author)}');" for author in authors]

# get insert statements for publishers
def get_publisher_inserts(df):
    publishers = df['Publisher'].unique() 
    return [f"INSERT INTO PUBLISHER(Publisher_ID, Publisher_Name) VALUES ({generate_unique_code(pub)}, '{update_string(pub)}');" for pub in publishers]

# get insert statements for products
def get_product_inserts(df):
    default_warehouse_no = 1
    unique_books = df.drop_duplicates(subset=['ISBN'])
    return [f"INSERT INTO PRODUCT(Item_Number, Warehouse_No, Price, Product_Type) VALUES ('{isbn}', {default_warehouse_no}, {price.replace('$', '')}, '{update_string(category)}');" 
            for isbn, price, category in zip(unique_books['ISBN'], unique_books['Price'], unique_books['Category'])]

# get insert statements for books
def get_book_inserts(df):
    book_inserts = []
    joined_data = df.groupby('ISBN').agg({'Publisher': 'first', 'Title': 'first', 'Author(s)': '; '.join}).reset_index()
    
    for _, row in joined_data.iterrows():
        #generate unique author and publisher ids and note these should return the same values as author and publisher since that is how the hash function works
        author_ids = generate_author_ids(row['Author(s)'])
        publisher_id = generate_unique_code(row['Publisher'])
        
        #adds book onto the array
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
    # Write SQL insert statements for author, publisher, warehouse, product, and book tables in a single line each
    for inserts in [author_table, publisher_table, product_table, book_table]:
        file.write('\n'.join(inserts) + '\n')
