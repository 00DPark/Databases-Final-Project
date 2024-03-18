import pandas as pd

#NOTES: 
# books with multiple authors are assigned to whichever author appears first
# authors that are not listed with each book are still added to the author list and given its attributes
# abs(hash(...) is used to generate unique positive values 
# zip() function is used to iterate over multiple lists at a single times
# first row is skipped in file since it just contains title of file
# since warehouse is not given, simply created a default warehouse to house them
# values are only inserted into author, publisher, warehouse, product, and book tables as these are required based on the schema

# handles the case where a single quote appears in name of something by replacing single quote with double single quotes
def update_string(string):
    return string.replace("'", "''")

# fills missing values from the previously filled row
# this handles the case when a book has multiple authors
def add_previous_columns_to_author(df):
    columns = ['ISBN', 'Title', 'Publisher', 'Year', 'Price', 'Category']
    df[columns] = df[columns].ffill()
    return df

# get insert statements for authors
def get_author_inserts(df):
    authors = df['Author(s)'].unique() #gets the unique rows
    return [f"INSERT INTO AUTHOR(Author_ID, Author_Name) VALUES ({abs(hash(author))}, '{update_string(author)}');" for author in authors]

# get insert statements for publishers
def get_publisher_inserts(df):
    publishers = df['Publisher'].unique() #gets the unique rows
    return [f"INSERT INTO PUBLISHER(Publisher_ID, Publisher_Name) VALUES ({abs(hash(pub))}, '{update_string(pub)}');" for pub in publishers]

# get insert statements for warehouse
def get_warehouse_inserts():
    #since this data was not generated this is just a default warehouse value
    default_warehouse = (1, 0, '123 Malibu Lane, Los Angeles, CA 11108')
    return [f"INSERT INTO WAREHOUSE(Warehouse_No, Total_Books, Address) VALUES {default_warehouse};"]

# get insert statements for products
def get_product_inserts(df):
    default_warehouse_no = 1
    unique_books = df.drop_duplicates(subset=['ISBN']) #gets the unique book for each product
    return [f"INSERT INTO PRODUCT(Item_Number, Warehouse_No, Transaction_ID, Price, Product_Type) VALUES ('{isbn}', {default_warehouse_no}, '', {price.replace('$', '').strip()}, '{update_string(category)}');" 
            for isbn, price, category in zip(unique_books['ISBN'], unique_books['Price'], unique_books['Category'])]

# gets SQL insert statements for books
def get_book_inserts(df):
    books = df.groupby('ISBN').first() #retrieves the author listed first
    return [f"INSERT INTO BOOK(ISBN, Publisher_ID, Item_Number, Book_Title, Publisher, Author_ID) VALUES ('{isbn}', {abs(hash(publisher))}, '{isbn}', '{update_string(title)}', '{update_string(publisher)}', {abs(hash(authors))});" 
            for isbn, title, publisher, authors in zip(books.index, books['Title'], books['Publisher'], books['Author(s)'])]

# reads the csv file and skips first row with book as the only value
df = pd.read_csv('PROJECT/data.csv', encoding='latin-1', skiprows=1)

# updates data to handle case where there are multiple authors
df = add_previous_columns_to_author(df)

# set the insert statements for each table
author_table = get_author_inserts(df)
publisher_table = get_publisher_inserts(df)
warehouse_table = get_warehouse_inserts()
product_table = get_product_inserts(df)
book_table = get_book_inserts(df)

# opens file in write mode
with open('PROJECT/output.sql', 'w') as file: 
    # write SQL insert statements for  author, publisher, warehouse, product, and book tables in a single line each
    for inserts in [author_table, publisher_table, warehouse_table, product_table, book_table]:
        file.write('\n'.join(inserts) + '\n')
