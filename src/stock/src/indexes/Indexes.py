class Indexes:
    """
    Ticker	    Description	                            Maintained By
    ^GSPC	    S&P 500 Index (Market Cap Weighted)	    S&P Dow Jones Indices
    ^SP500EW	S&P 500 Equal Weight Index	            S&P Dow Jones Indices
    ^SP500G	    S&P 500 Growth Index	                S&P Dow Jones Indices
    ^SP500V	    S&P 500 Value Index	                    S&P Dow Jones Indices
    ^SPDAUDP	S&P 500 Dividend Aristocrats	        S&P Dow Jones Indices
    ^SP500LVOL	S&P 500 Low Volatility Index	        S&P Dow Jones Indices
    ^SPXESUP	S&P 500 ESG Index	                    S&P Dow Jones Indices
    ^SP500PG	S&P 500 Pure Growth Index	            S&P Dow Jones Indices
    ^SP500PV	S&P 500 Pure Value Index	            S&P Dow Jones Indices
    ^SPX50	    S&P 500 Top 50 Index	                Likely a custom index; check provider
    ^SPXHDUP	S&P 500 High Dividend Index	            Likely a custom index; check provider
    ^SP500-20	S&P 500 Industrials Index	            S&P Dow Jones Indices
    ^SP500-45	S&P 500 Information Technology Index	S&P Dow Jones Indices
    ^SP500-35	S&P 500 Health Care Index	            S&P Dow Jones Indices
    ^SP500-40	S&P 500 Financials Index	            S&P Dow Jones Indices
    """

    SP500_INDEXES = [
        "^GSPC", "^SP500EW", "^SP500G", "^SP500V", "^SPDAUDP", "^SP500LVOL",
        "^SPXESUP", "^SP500PG", "^SP500PV", "^SPX50", "^SPXHDUP", "^SP500-20",
        "^SP500-45", "^SP500-35", "^SP500-40"
    ]

    GLOBAL_INDEXES = [
        "^DJI", "^IXIC", "^RUT", "^FTSE", "^GDAXI", "^FCHI", "^N225", "^HSI",
        "000001.SS", "^STOXX50E", "^BVSP", "^GSPTSE", "^AXJO", "^KS11",
        "^SP400", "^SP600", "^MSCIW", "^NYA", "^XAX", "^XAR", "^IBEX",
        "^MDAXI", "^SSMI", "^TA35", "^VIX", "^MXX", "^BSESN", "^NSEI",
        "^AEX", "FTSEMIB.MI", "^NDX", "^W5000", "^OEX", "^DJT", "^DJU"
    ]

    ALL_INDEXES = SP500_INDEXES + GLOBAL_INDEXES

    @classmethod
    def print_all(cls):
        print("SP500 Indexes:", cls.SP500_INDEXES)
        print("Global Indexes:", cls.GLOBAL_INDEXES)
        print("All Indexes:", cls.ALL_INDEXES)

    @staticmethod
    def get_all_indexes() -> list[str]:
        return Indexes.ALL_INDEXES
