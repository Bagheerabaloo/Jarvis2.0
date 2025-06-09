import pandas as pd

class ExchangeMapper:
    """
    This class contains an internal dictionary to map the values of a specific
    column in a DataFrame (e.g., 'exchange') to more descriptive values.
    For example, 'NMS' -> 'NASDAQ'.
    """
    def __init__(self, source_col='exchange', target_col='exchange'):
        """
        Initializes the ExchangeMapper with the column names (source and target)
        and a default mapping dictionary defined within the class.

        :param source_col: Name of the column in the DataFrame to be mapped.
        :param target_col: Name of the column where the mapped values will be stored.
                          It can be the same as source_col (overwriting the original values).
        """
        self.source_col = source_col
        self.target_col = target_col

        # The internal mapping dictionary
        self.mapping_dict = {
            'NYQ': 'NYSE',
            'NMS': 'NASDAQ',
            'NGM': 'NASDAQ GLOBAL MARKET',
            'NCM': 'NASDAQ CAPITAL MARKET',
            # Add more mappings as needed
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the mapping to a given DataFrame.
        Returns a new DataFrame with the target column containing the mapped values.

        If a key in the source column is not found in the dictionary,
        it will be mapped to NaN by default.

        :param df: The input DataFrame on which the mapping should be applied.
        :return: A new DataFrame with the mapped column.
        """
        # Create a copy to avoid modifying the original DataFrame in-place
        df_copy = df.copy()

        # Use 'map' to replace source_col values based on the mapping dictionary
        df_copy[self.target_col] = df_copy[self.source_col].map(self.mapping_dict)

        return df_copy


if __name__ == '__main__':
    # Example usage:
    data = {
        'id': [1, 2, 3, 4],
        'symbol': ['AAPL', 'TSLA', 'IBM', 'AMZN'],
        'exchange': ['NMS', 'NYQ', 'NGM', 'NCM']
    }
    df = pd.DataFrame(data)

    print("DataFrame before:")
    print(df)

    mapper = ExchangeMapper(source_col='exchange', target_col='exchange')
    df_transformed = mapper.transform(df)

    print("\nDataFrame after:")
    print(df_transformed)
