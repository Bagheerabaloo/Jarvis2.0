import pandas as pd
from neomodel import (StructuredNode, StringProperty, IntegerProperty, RelationshipTo, config)

# Configure the connection to Neo4j
config.DATABASE_URL = 'bolt://neo4j:local_host@localhost:7687'  # Replace with your credentials

# Define the models
class Author(StructuredNode):
    name = StringProperty(unique_index=True, required=True)  # Unique author name
    wrote = RelationshipTo('Quote', 'WROTE')  # Relationship to Quote


class Rank(StructuredNode):
    name = StringProperty(unique_index=True, required=True)  # Unique Rank label
    describes = RelationshipTo('Quote', 'DESCRIBES')  # Relationship to Quote


class Quote(StructuredNode):
    quote_id = IntegerProperty(unique_index=True, required=True)  # Unique ID
    quote_eng = StringProperty(required=True)  # Quote text
    quote_ita = StringProperty()  # Italian translation
    rank = RelationshipTo('Rank', 'HAS_RANK')  # Relationship to Rank


# Load CSV file
csv_file = r"C:\Users\Vale\Downloads\quotes_1.csv"  # Replace with your file path
df = pd.read_csv(csv_file)  # Use correct delimiter (tab or comma)


# Insert data into Neo4j
def load_quotes_and_authors(df_):
    for _, row in df_.iterrows():
        # Create or get the author
        author = Author.nodes.get_or_none(name=row['author'])
        if not author:
            author = Author(name=row['author']).save()

        # Create or get the quote
        quote = Quote.nodes.get_or_none(quote_id=row['quote_id'])
        if not quote:
            quote = Quote(
                quote_id=row['quote_id'],
                quote_eng=row['quote'],
                quote_ita=row['quote_ita']
            ).save()

        # Create or get the Rank node
        if pd.notnull(row['Best']):  # Ensure 'Best' is not null
            rank_node = Rank.nodes.get_or_none(name=row['Best'])
            if not rank_node:
                rank_node = Rank(name=row['Best']).save()

            # Create relationship between quote and rank
            if not quote.rank.is_connected(rank_node):
                quote.rank.connect(rank_node)

        # Create relationship between author and quote
        if not author.wrote.is_connected(quote):
            author.wrote.connect(quote)


# Run the data loading function
load_quotes_and_authors(df)
print("Data successfully loaded into Neo4j!")

