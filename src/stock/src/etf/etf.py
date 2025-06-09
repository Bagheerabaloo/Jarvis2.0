class Etf:
    """
    Nome dell'ETF	        Descrizione	        Società di Gestione	        Exchange	        Ticker (Yahoo Finance)
    SPDR S&P 500 ETF Trust	Il primo ETF lanciato negli Stati Uniti nel 1993, replica fisicamente l'indice S&P 500.	State Street Global Advisors	NYSE Arca	SPY
    iShares Core S&P 500 ETF	Replica fisicamente l'indice S&P 500, offrendo un'esposizione diversificata alle principali società statunitensi.	BlackRock	NYSE Arca	IVV
    Vanguard S&P 500 ETF	Offre un'esposizione all'indice S&P 500 con un basso rapporto di spesa.	Vanguard	NYSE Arca	VOO
    Invesco S&P 500 UCITS ETF (Acc)	Replica l'indice S&P 500 utilizzando una replica fisica con accumulazione dei dividendi.	Invesco	Borsa Italiana	SPXS.MI
    SPDR S&P 500 UCITS ETF (Dist)	Replica fisicamente l'indice S&P 500 con distribuzione dei dividendi.	State Street Global Advisors	Borsa Italiana	SPY5.MI
    iShares Core S&P 500 UCITS ETF (Acc)	Offre un'esposizione all'indice S&P 500 con accumulazione dei dividendi.	BlackRock	Borsa Italiana	SXR8.DE
    Vanguard S&P 500 UCITS ETF	Replica l'indice S&P 500 con distribuzione dei dividendi trimestrale.	Vanguard	Borsa Italiana	VUSA.L
    Lyxor S&P 500 UCITS ETF	Replica fisicamente l'indice S&P 500 con una politica di distribuzione dei dividendi.	Lyxor	Borsa Italiana	SP5G.PA
    HSBC S&P 500 UCITS ETF	Offre un'esposizione all'indice S&P 500 con replica fisica e accumulazione dei dividendi.	HSBC Global Asset Management	Borsa Italiana	HSPX.L
    Amundi S&P 500 UCITS ETF	Replica l'indice S&P 500 utilizzando una replica fisica con distribuzione dei dividendi.	Amundi	Borsa Italiana	500.PA
    Xtrackers S&P 500 UCITS ETF	Offre un'esposizione all'indice S&P 500 con replica fisica e accumulazione dei dividendi.	DWS	Borsa Italiana	XDWS.DE
    UBS S&P 500 UCITS ETF	Replica l'indice S&P 500 con replica fisica e distribuzione dei dividendi.	UBS	Borsa Italiana	SP5U.SW
    Franklin S&P 500 UCITS ETF	Offre un'esposizione all'indice S&P 500 con replica fisica e accumulazione dei dividendi.	Franklin Templeton	Borsa Italiana	FLXG.L
    SPDR Portfolio S&P 500 ETF	Replica l'indice S&P 500 con un basso rapporto di spesa, ideale per investitori attenti ai costi.	State Street Global Advisors	NYSE Arca	SPLG
    Schwab S&P 500 Index Fund	Un fondo indicizzato che replica l'S&P 500, noto per le sue basse spese di gestione.	Charles Schwab	NYSE Arca	SWPPX
    iShares S&P 500 Growth ETF	Si concentra sulle società dell'S&P 500 con caratteristiche di crescita, offrendo un'esposizione mirata.	BlackRock	NYSE Arca	IVW
    iShares S&P 500 Value ETF	Si concentra sulle società dell'S&P 500 considerate sottovalutate, offrendo un'esposizione al segmento value.	BlackRock	NYSE Arca	IVE
    SPDR S&P 500 Fossil Fuel Reserves Free ETF	Replica l'indice S&P 500 escludendo le società con riserve di combustibili fossili, per investitori attenti ai criteri ESG.	State Street Global Advisors	NYSE Arca	SPYX
    Invesco S&P 500 Equal Weight ETF	Offre un'esposizione equamente ponderata alle società dell'S&P 500, riducendo la concentrazione sulle grandi capitalizzazioni.	Invesco	NYSE Arca	RSP
    JPMorgan BetaBuilders S&P 500 ETF	Replica l'indice S&P 500 con un focus sulla riduzione dei costi e sull'efficienza fiscale.	JPMorgan Chase	NYSE Arca	BBUS
    iShares S&P 500 Information Technology ETF	Fornisce un'esposizione al settore tecnologico dell'S&P 500, concentrandosi sulle principali società tecnologiche.	BlackRock	NYSE Arca	IYW
    iShares S&P 500 Financials Sector ETF	Offre un'esposizione al settore finanziario dell'S&P 500, includendo banche, assicurazioni e altre istituzioni finanziarie.	BlackRock	NYSE Arca	IYF
    iShares S&P 500 Health Care Sector ETF	Si concentra sulle società del settore sanitario all'interno dell'S&P 500, comprese aziende farmaceutiche e biotecnologiche.	BlackRock	NYSE Arca	IYH
    iShares S&P 500 Consumer Discretionary ETF	Replica il settore dei beni di consumo discrezionali dell'S&P 500, includendo aziende come quelle automobilistiche e di beni di lusso.	BlackRock	NYSE Arca	IYC
    iShares S&P 500 Consumer Staples ETF	Offre un'esposizione al settore dei beni di consumo di base dell'S&P 500, includendo aziende alimentari e di prodotti per la casa.	BlackRock	NYSE Arca	IYK
    iShares S&P 500 Industrials Sector ETF	Si concentra sulle società industriali dell'S&P 500, includendo aziende manifatturiere e di servizi industriali.	BlackRock	NYSE Arca	IYJ
    """

    """
    ETF iShares per tutti gli 11 settori GICS dell'S&P 500
    Settore                 GICS (S&P 500)  ETF iShares                     Ticker      Corrispondenza YFinance
    Information Technology  iShares S&P 500 Information Technology ETF      IYW         Technology
    Financials	            iShares S&P 500 Financials Sector ETF	        IYF         Financial Services
    Health Care	            iShares S&P 500 Health Care Sector ETF	        IYH     	Healthcare
    Consumer Discretionary	iShares S&P 500 Consumer Discretionary ETF	    IYC	        Consumer Cyclical
    Consumer Staples	    iShares S&P 500 Consumer Staples ETF	        IYK	        Consumer Defensive
    Industrials	            iShares S&P 500 Industrials Sector ETF	        IYJ	        Industrials
    Energy	                iShares U.S. Energy ETF	                        IYE	        Energy
    Utilities	            iShares U.S. Utilities ETF	                    IDU	        Utilities
    Communication Services	iShares U.S. Telecommunications ETF	            IYZ	        Communication Services
    Real Estate	            iShares U.S. Real Estate ETF	                IYR	        Real Estate
    Materials	            iShares U.S. Basic Materials ETF	            IYM	        Basic Materials

    📈 ETF iShares S&P 500 - Generali
    Questi ETF offrono esposizione all'intero indice S&P 500.
    ETF	                        Ticker	Descrizione
    iShares Core S&P 500 ETF	IVV	    Replica fedelmente l'S&P 500, simile a SPY e VOO.
    iShares S&P 500 ETF (UCITS)	CSPX	Versione UCITS (per investitori europei) dell'ETF IVV.
    
    📉 ETF iShares S&P 500 - Fattori e Smart Beta
    Questi ETF selezionano le azioni dell'S&P 500 in base a specifici criteri quantitativi.
    ETF	                                        Ticker	Descrizione
    iShares S&P 500 Value ETF	                IVE	    Replica l'S&P 500 Value Index (titoli sottovalutati).
    iShares S&P 500 Growth ETF	                IVW	    Replica l'S&P 500 Growth Index (aziende a crescita elevata).
    iShares S&P 500 Momentum ETF	            SPMO	Seleziona azioni con trend rialzisti (momentum).
    iShares Edge MSCI USA Quality Factor ETF	QUAL	Seleziona aziende dell’S&P 500 con bilanci solidi.
    iShares Edge MSCI USA Value Factor ETF	    VLUE	Seleziona titoli value (basso P/E e P/B).
    iShares Edge MSCI USA Size Factor ETF	    SIZE	Favorisce aziende di medie dimensioni nell'S&P 500.
    
    ⚖️ ETF iShares S&P 500 - Equal Weight
    Questi ETF ponderano tutte le azioni dell'S&P 500 in modo equo, senza dare più peso alle grandi capitalizzazioni.
    ETF	                                Ticker	Descrizione
    iShares S&P 500 Equal Weight ETF	EUSA	Distribuisce il peso in modo equo tra le 500 aziende.
    
    📊 ETF iShares S&P 500 - Volatilità e Strategie Difensive
    Questi ETF sono progettati per proteggere gli investitori nei periodi di alta volatilità.
    ETF	                                                Ticker	Descrizione
    iShares MSCI USA Min Vol Factor ETF	                USMV	Replica un S&P 500 con aziende a bassa volatilità.
    iShares Edge MSCI USA Minimum Volatility UCITS ETF	MVUS	Versione UCITS per investitori europei.
    
    🌍 ETF iShares S&P 500 - Internazionali
    ETF basati sull'S&P 500 ma ottimizzati per investitori fuori dagli Stati Uniti.
    ETF	                            Ticker	Descrizione
    iShares S&P 500 EUR Hedged ETF	IUSE	Replica l'S&P 500 con copertura valutaria in euro.
    iShares S&P 500 GBP Hedged ETF	IUSP	Versione con copertura valutaria in sterline.
    
    📅 ETF iShares S&P 500 - Covered Call e Income
    Questi ETF generano reddito distribuendo dividendi più alti, utilizzando strategie covered call.
    ETF	                            Ticker	Descrizione
    iShares S&P 500 BuyWrite ETF	XYLD	Usa opzioni covered call per generare reddito.
    """

    """
    Nome dell'ETF	                            Ticker	Descrizione	                                                                                Spese di Gestione (TER)
    SPDR S&P 500 ETF Trust	                    SPY	    Replica l'indice S&P 500, offrendo esposizione alle 500 principali società statunitensi.	0,0945%
    SPDR Portfolio S&P 500 ETF	                SPLG	Fornisce un'esposizione all'S&P 500 con un TER inferiore rispetto a SPY.	                0,02%
    SPDR Portfolio S&P 500 Growth ETF	        SPYG	Si concentra sulle società dell'S&P 500 con caratteristiche di crescita.	                0,04%
    SPDR Portfolio S&P 500 Value ETF	        SPYV	Replica l'S&P 500 Value Index, focalizzandosi su società sottovalutate.	                    0,04%
    SPDR Portfolio S&P 500 High Dividend ETF	SPYD	Si concentra sulle società dell'S&P 500 che offrono dividendi elevati.	                    0,07%
    SPDR S&P 500 Fossil Fuel Free ETF	        SPYX	Replica l'S&P 500 escludendo le società coinvolte nella produzione di combustibili fossili.	0,20%
    
    Nome dell'ETF	                                Ticker	Settore	                        Descrizione
    Financial Select Sector SPDR Fund	            XLF	    Finanziario	                    Replica il settore finanziario dell'S&P 500, includendo banche, assicurazioni e altre istituzioni finanziarie.
    Technology Select Sector SPDR Fund	            XLK	    Tecnologia	                    Offre esposizione al settore tecnologico dell'S&P 500, comprendendo aziende di software, hardware e servizi IT.
    Energy Select Sector SPDR Fund	                XLE	    Energia	                        Si concentra sulle società energetiche dell'S&P 500, incluse quelle operanti nel petrolio, gas e consumi energetici.
    Consumer Discretionary Select Sector SPDR Fund	XLY	    Beni di Consumo Discrezionali	Copre il settore dei beni di consumo discrezionali dell'S&P 500, come aziende automobilistiche e di beni di lusso.
    Industrial Select Sector SPDR Fund	            XLI	    Industriali	                    Rappresenta il settore industriale dell'S&P 500, includendo aziende manifatturiere e di servizi industriali.
    Health Care Select Sector SPDR Fund	            XLV	    Sanità	                        Fornisce esposizione al settore sanitario dell'S&P 500, comprese aziende farmaceutiche e biotecnologiche.
    Consumer Staples Select Sector SPDR Fund	    XLP	    Beni di Consumo Primari	        Si focalizza sul settore dei beni di consumo primari dell'S&P 500, includendo aziende alimentari e di prodotti per la casa.
    Utilities Select Sector SPDR Fund	            XLU	    Utility	                        Copre il settore delle utility dell'S&P 500, comprendendo aziende di servizi pubblici come elettricità e gas.
    Materials Select Sector SPDR Fund	            XLB	    Materiali	                    Rappresenta il settore dei materiali dell'S&P 500, includendo aziende minerarie e chimiche.
    Real Estate Select Sector SPDR Fund	            XLRE	Immobiliare	                    Offre esposizione al settore immobiliare dell'S&P 500, comprendendo società immobiliari e REITs.
    Communication Services Select Sector SPDR Fund	XLC	    Servizi di Comunicazione	    Si concentra sul settore dei servizi di comunicazione dell'S&P 500, includendo aziende di telecomunicazioni e media.
    """

    """
    Nome dell'ETF	                    Ticker	Descrizione	                                    TER	    Mercato	    Dividendi
    Vanguard S&P 500 ETF	            VOO	    Replica l'S&P 500 con bassi costi di gestione.	0,03%	USA	        Distribuiti
    Vanguard S&P 500 UCITS ETF	        VUSA	Versione UCITS per investitori europei.	        0,07%	Europa	    Distribuiti
    Vanguard S&P 500 UCITS ETF (Acc)	VUAA	Versione UCITS con accumulazione dei dividendi.	0,07%	Europa	    Accumulati
    Vanguard S&P 500 Growth ETF	        VOOG	Si concentra sulle azioni growth dell'S&P 500.	0,10%	USA	        Distribuiti
    Vanguard S&P 500 Value ETF	        VOOV	Si concentra sulle azioni value dell'S&P 500.	0,10%	USA	        Distribuiti
    
    Nome dell'ETF	                    Ticker	Settore	                        Spese di Gestione (TER)	Descrizione
    Vanguard Consumer Discretionary ETF	VCR	    Beni di Consumo Discrezionali	0,10%	                Esposizione a società che offrono beni e servizi non essenziali, come aziende automobilistiche e di beni di lusso.
    Vanguard Consumer Staples ETF	    VDC	    Beni di Consumo Primari	        0,10%	                Include aziende che producono beni di prima necessità, come alimentari e prodotti per la casa.
    Vanguard Energy ETF	                VDE	    Energia	                        0,10%	                Focalizzato su società operanti nel settore energetico, inclusi petrolio, gas e energie rinnovabili.
    Vanguard Financials ETF	            VFH	    Finanziario	                    0,10%	                Comprende banche, assicurazioni e altre istituzioni finanziarie.
    Vanguard Health Care ETF	        VHT	    Sanità	                        0,10%	                Esposizione a società del settore sanitario, comprese aziende farmaceutiche e biotecnologiche.
    Vanguard Industrials ETF	        VIS	    Industriali	                    0,10%	                Include aziende manifatturiere e di servizi industriali.
    Vanguard Information Technology ETF	VGT	    Tecnologia dell'Informazione	0,10%	                Focalizzato su aziende di software, hardware e servizi IT.
    Vanguard Materials ETF	            VAW	    Materiali	                    0,10%	                Comprende società operanti nel settore dei materiali, come aziende chimiche e minerarie.
    Vanguard Real Estate ETF	        VNQ	    Immobiliare	                    0,12%	                Esposizione a società immobiliari e REITs.
    Vanguard Utilities ETF	            VPU	    Utility	                        0,10%	                Include aziende di servizi pubblici, come elettricità e gas.
    """

    # __ BlackRock iShares SP500 ETFs __
    iShares_SP500_SECTORS = ["IYW", "IYF", "IYH", "IYC", "IYK", "IYJ", "IYE", "IDU", "IYZ", "IYR", "IYM"]
    iShares_SP500_GENERAL = ["IVV", "CSPX"]
    iShares_SP500_FACTORS = ["IVE", "IVW", "SPMO", "QUAL", "VLUE", "SIZE"]
    iShares_SP500_EQUAL_WEIGHT = ["EUSA"]
    iShares_SP500_VOLATILITY = ["USMV", "MVUS"]
    iShares_SP500_INTERNATIONAL = ["IUSE", "IUSP"]
    iShares_SP500_COVERED_CALL = ["XYLD"]
    iShares_SP500 = iShares_SP500_SECTORS + iShares_SP500_GENERAL + iShares_SP500_FACTORS + iShares_SP500_EQUAL_WEIGHT + iShares_SP500_VOLATILITY + iShares_SP500_INTERNATIONAL + iShares_SP500_COVERED_CALL

    # __ State Street Global Advisors SP500 ETFs __
    SPDR_SP500_GENERAL = ["SPY", "SPLG", "SPYG", "SPYV", "SPYD", "SPYX"]
    SPDR_SP500_SECTORS = ["XLF", "XLK", "XLE", "XLY", "XLI", "XLV", "XLP", "XLU", "XLB", "XLRE", "XLC"]
    SPDR_SP500 = SPDR_SP500_GENERAL + SPDR_SP500_SECTORS

    # __ Vanguard SP500 ETFs __
    VANGUARD_SP500_GENERAL = ["VOO", "VUSA", "VUAA", "VOOG", "VOOV"]
    VANGUARD_SP500_SECTORS = ["VCR", "VDC", "VDE", "VFH", "VHT", "VIS", "VGT", "VAW", "VNQ", "VPU"]
    VANGUARD_SP500 = VANGUARD_SP500_GENERAL + VANGUARD_SP500_SECTORS

    OTHER_SP500_ETF = ["SPXS.MI", "SPY5.MI", "SXR8.DE", "VUSA.L", "SP5G.PA", "HSPX.L", "500.PA",
        "XDWS.DE", "SP5U.SW", "FLXG.L", "SWPPX", "RSP", "BBUS"
    ]

    ALL_ETF = iShares_SP500 + SPDR_SP500 + VANGUARD_SP500 + OTHER_SP500_ETF

    @classmethod
    def print_all(cls):
        print("iShares SP500 ETFs:", cls.iShares_SP500)
        print("SPDR SP500 ETFs:", cls.SPDR_SP500)
        print("Vanguard SP500 ETFs:", cls.VANGUARD_SP500)
        print("Other SP500 ETFs:", cls.OTHER_SP500_ETF)

    @staticmethod
    def get_all_etf_tickers() -> list[str]:
        return Etf.ALL_ETF
