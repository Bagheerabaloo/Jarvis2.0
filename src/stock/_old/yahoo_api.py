import yahoo_fin.stock_info as si
import yahoo_fin.options as opt
from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from time import sleep, time
from src.common.tools.library import seconds_to_time_str


@dataclass
class YahooFin:
    ticker: str

    def collect(self):

        income_statement = self.get_income_statement(include_ttm=False)
        sleep(0.5)

        balance_sheet = self.get_balance_sheet(include_ttm=False)
        sleep(0.5)

        cash_flow = self.get_cash_flow(include_ttm=False)
        sleep(0.5)

        analyst_info = self.get_analyst_info()
        sleep(0.5)

        # data = self.get_data()
        # sleep(0.5)

        holders = self.get_holders()
        holders = holders['Major Holders'].set_index(1).T
        sleep(0.5)

        quote_table = self.get_quote_table(parse=True)
        sleep(0.5)

        stats = self.get_stats()
        sleep(0.5)

        # stats_evaluation = self.get_stats_valuation()
        # sleep(0.5)

        options = self.get_options()
        sleep(0.5)

        expiration_dates = self.get_expiration_dates()

        df = income_statement.join(balance_sheet, how='outer').join(cash_flow, how='outer')
        assert(df.shape[1] == len(balance_sheet.columns) + len(cash_flow.columns) + len(income_statement.columns))

        assert holders.loc[0, '% of Shares Held by All Insider'] == stats.loc[0, '% Held by Insiders 1']
        assert holders.loc[0, '% of Shares Held by Institutions'] == stats.loc[0, '% Held by Institutions 1']
        stats = stats.join(holders[['% of Float Held by Institutions', 'Number of Institutions Holding Shares']])

        assert 1 == 1
        stats = stats.join(quote_table[['1y Target Est', "Day's Range", 'EPS (TTM)', 'Earnings Date', 'Ex-Dividend Date', 'Market Cap', 'Open', 'PE Ratio (TTM)', 'Previous Close', 'Quote Price', 'Volume']])

        # stats['52WeeksDown'] = stats['52WeeksDown'].str.replace(',', '').astype(float)
        # stats['52WeeksUp'] = stats['52WeeksUp'].str.replace(',', '').astype(float)
        # stats['52Week%Down'] = (1 - stats['52WeeksDown'] / stats['52WeeksUp']) * 100
        # stats['52WeeksPercDown'] = (1 - stats['52WeeksDown'] / stats['52WeeksUp']) * 100
        # stats['52WeeksPercSpan'] = (stats['Quote Price'] - stats['52WeeksDown']) / (stats['52WeeksUp'] - stats['52WeeksDown']) * 100
        # stats['Market Cap (M)'] = stats['Market Cap'].apply(lambda x: self.string_number_parser(x))
        # stats['PE Ratio (TTM)'] = stats['PE Ratio (TTM)'].replace('∞', np.nan).astype(float)

        stats.sort_index(axis=1, ascending=True, inplace=True)
        print('end')

    def collect_statistics(self) -> pd.DataFrame:

        holders = self.get_holders()
        holders = holders['Major Holders'].set_index("Unnamed: 1").T
        sleep(5)

        quote_table = self.get_quote_table(parse=False)
        sleep(5)

        stats_ = self.get_stats()
        sleep(5)

        if holders.loc[0, '% of Shares Held by All Insider'] != stats_.loc[0, '% Held by Insiders 1']:
            print("WARNING: {} - % Held by Insiders is different".format(self.ticker))
        if holders.loc[0, '% of Shares Held by Institutions'] != stats_.loc[0, '% Held by Institutions 1']:
            print("WARNING: {} - % Held by Institutions is different".format(self.ticker))
        if quote_table.loc[0, '52 Week Range'] != "{} - {}".format(stats_.loc[0, '52 Week Low 3'], stats_.loc[0, '52 Week High 3']):
            print("WARNING: {} - 52 Week Range is different".format(self.ticker))
        if float(quote_table.loc[0, 'Beta (5Y Monthly)']) != float(stats_.loc[0, 'Beta (5Y Monthly)']):
            print("WARNING: {} - Beta (5Y Monthly) is different".format(self.ticker))

        df = stats_.join(holders[['% of Float Held by Institutions', 'Number of Institutions Holding Shares']])
        df = df.join(quote_table[['1y Target Est', "Day's Range", 'Avg. Volume', 'EPS (TTM)', 'Earnings Date', 'Ex-Dividend Date', 'Forward Dividend & Yield', 'Market Cap', 'Open', 'PE Ratio (TTM)', 'Previous Close', 'Quote Price', 'Volume']])
        df['Last Update'] = time()

        return df.rename(index={0: self.ticker})

    # _____ summary _____
    def get_quote_table(self, ticker: str = None, parse: bool = False, as_dict: bool = False):
        ticker = self.ticker if not ticker else ticker
        # res = si.get_quote_table(ticker=ticker)

        # __ new piece of code __
        dict_result = True
        headers = {'User-agent': 'Mozilla/5.0'}

        site = "https://finance.yahoo.com/quote/" + ticker + "?p=" + ticker
        tables = pd.read_html(requests.get(site, headers=headers).text)

        data = tables[0].append(tables[1])

        data.columns = ["attribute", "value"]

        quote_price = pd.DataFrame(["Quote Price", get_live_price(ticker)]).transpose()
        quote_price.columns = data.columns.copy()

        data = data.append(quote_price)

        data = data.sort_values("attribute")

        data = data.drop_duplicates().reset_index(drop=True)

        data["value"] = data.value.map(force_float)

        if dict_result:
            result = {key: val for key, val in zip(data.attribute, data.value)}
            return result

        return data

        if not parse:
            return res if as_dict else pd.DataFrame.from_dict(res, orient='index').T

        res['52WeeksDown'], res['52WeeksUp'] = res['52 Week Range'].split(' - ', 1)
        res['52WeeksDown'] = float(res['52WeeksDown'].replace(',', ''))
        res['52WeeksUp'] = float(res['52WeeksUp'].replace(',', ''))
        res['52WeeksPercDown'] = (1 - res['52WeeksDown'] / res['52WeeksUp']) * 100
        res['52WeeksPercSpan'] = (res['Quote Price'] - res['52WeeksDown']) / (res['52WeeksUp'] - res['52WeeksDown']) * 100
        res['Market Cap (M)'] = float(self.string_number_parser(res['Market Cap']))
        if type(res['PE Ratio (TTM)']) == str:
            res['PE Ratio (TTM)'] = float(res['PE Ratio (TTM)'].replace('∞', np.nan))

        return res if as_dict else pd.DataFrame.from_dict(res, orient='index').T

    # _____ statistics _____
    def get_stats(self, ticker: str = None, as_dict: bool = False):
        ticker = self.ticker if not ticker else ticker
        url = "https://finance.yahoo.com/quote/" + ticker + "/key-statistics?p=" + ticker
        headers = {'User-agent': 'Mozilla/5.0'}

        tables = pd.read_html(requests.get(url, headers=headers).text)
        tables = [table for table in tables if table.shape[1] == 2]
        table = tables[0]
        for elt in tables[1:]:
            table = table.append(elt)
        table.columns = ["Attribute", "Value"]
        df = table.reset_index(drop=True).rename(columns={'Value': 0})

        return df.set_index('Attribute').to_dict()[0] if as_dict else df.set_index('Attribute').T

    def get_stats_valuation(self, ticker: str = None, as_dict: bool = False):
        ticker = self.ticker if not ticker else ticker
        df = si.get_stats_valuation(ticker=ticker)
        return df.set_index(0).to_dict()[1] if as_dict else df.set_index(0).T

    # _____ financials _____
    def get_balance_sheet(self, ticker: str = None, include_ttm: bool = False):
        ticker = self.ticker if not ticker else ticker

        url = "https://finance.yahoo.com/quote/" + ticker + "/balance-sheet?p=" + ticker
        headers = {'User-agent': 'Mozilla/5.0'}
        df = self._parse_table(url=url, headers=headers, include_ttm=include_ttm)

        return df

    def get_cash_flow(self, ticker: str = None, include_ttm: bool = False):
        ticker = self.ticker if not ticker else ticker

        url = "https://finance.yahoo.com/quote/" + ticker + "/cash-flow?p=" + ticker
        headers = {'User-agent': 'Mozilla/5.0'}
        df = self._parse_table(url=url, headers=headers, include_ttm=include_ttm)

        return df

    def get_income_statement(self, ticker: str = None, include_ttm: bool = False):
        ticker = self.ticker if not ticker else ticker

        url = "https://finance.yahoo.com/quote/" + ticker + "/financials?p=" + ticker
        headers = {'User-agent': 'Mozilla/5.0'}
        df = self._parse_table(url=url, headers=headers, include_ttm=include_ttm)

        return df

    @staticmethod
    def _parse_table(url: str, headers: dict, include_ttm: bool = True):
        html = requests.get(url=url, headers=headers).text
        soup = BeautifulSoup(html, 'html.parser')

        header = [x.text for x in soup.findAll('div', 'D(tbr) C($primaryColor)')[0].findAll('span')]
        rows = [x for x in soup.findAll('div', 'fi-row')]

        # fi_rows = [[y.text for y in x.findAll('span')] for x in soup.findAll('div', 'fi-row')]
        fi_rows = []
        for row in rows:
            fi_cols = []
            for element in row.findChildren('div', recursive=False):
                try:
                    txt = element.findAll('span')[0].text
                except:
                    txt = element.text
                fi_cols.append(txt.replace('&', 'and').replace(',', ''))
            fi_rows.append(fi_cols)

        df = pd.DataFrame(fi_rows, columns=header).set_index('Breakdown').replace('-', np.nan).astype(np.float64)
        if not include_ttm and 'ttm' in df.columns:
            del df['ttm']
        return df.T

    # _____ analysis _____
    def get_analyst_info(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = si.get_analysts_info(ticker=ticker)

        if not stack:
            return res

        earnings_estimate = res['Earnings Estimate'].set_index('Earnings Estimate').stack().to_dict()
        revenue_estimate = res['Revenue Estimate'].set_index('Revenue Estimate').stack().to_dict()
        earnings_history = res['Earnings History'].set_index('Earnings History').stack().to_dict()
        eps_trend = res['EPS Trend'].set_index('EPS Trend').stack().to_dict()
        eps_revisions = res['EPS Revisions'].set_index('EPS Revisions').stack().to_dict()
        growth_estimates = res['Growth Estimates'].set_index('Growth Estimates').stack().to_dict()
        return {'EarningsEstimate': earnings_estimate, 'RevenueEstimate': revenue_estimate,
                'EarningHistory': earnings_history, 'EpsTrend': eps_trend,
                'EpsRevisions': eps_revisions, 'GrowthEstimates': growth_estimates}

    # _____ holders _____
    def get_holders(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = si.get_holders(ticker=ticker)

        if not stack:
            return res

        indexes = {'Major Holders': 1,
                   'Direct Holders (Forms 3 and 4)': None,
                   'Top Institutional Holders': None}

        return {x: res[x].set_index(indexes[x]).stack().to_dict() if indexes[x] else res[x] for x in res}

    def get_data(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = si.get_data(ticker=ticker)

        if not stack:
            return res

        return res

    def get_live_price(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = si.get_live_price(ticker=ticker)

        if not stack:
            return res

        return res

    @staticmethod
    def get_day_gainers(count: int = 100, stack: bool = False):
        res = si.get_day_gainers(count=count)

        if not stack:
            return res

        return res

    @staticmethod
    def get_day_losers(count: int = 100, stack: bool = False):
        res = si.get_day_losers(count=count)

        if not stack:
            return res

        return res

    @staticmethod
    def get_day_most_active(count: int = 100, stack: bool = False):
        res = si.get_day_most_active(count=count)

        if not stack:
            return res

        return res

    @staticmethod
    def tickers_dow():
        return si.tickers_dow()

    @staticmethod
    def tickers_nasdaq():
        return si.tickers_nasdaq()

    @staticmethod
    def tickers_sp500(include_company_data=False):
        """Downloads list of tickers currently listed in the S&P 500"""

        # __ get list of all S&P 500 stocks __
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        # sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)

        if include_company_data:
            return sp500

        sp_tickers = sp500.Symbol.tolist()
        sp_tickers = sorted(sp_tickers)

        return sp_tickers

    @staticmethod
    def tickers_other():
        return si.tickers_other()

    @staticmethod
    def string_number_parser(x):
        if type(x) != str:
            return np.nan
        elif 'M' in x:
            return float(x.split('M')[0])
        elif 'B' in x:
            return float(x.split('B')[0]) * 1000
        elif 'T' in x:
            return float(x.split('T')[0]) * 1000000
        else:
            print(x)
            return 0

    # _____ Options _____
    def get_calls(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = opt.get_calls(ticker=ticker)

        if not stack:
            return res

        return res

    def get_puts(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = opt.get_puts(ticker=ticker)

        if not stack:
            return res

        return res

    def get_options(self, ticker: str = None, date: bool = None):
        ticker = self.ticker if not ticker else ticker
        return opt.get_options_chain(ticker, date)

    def get_expiration_dates(self, ticker: str = None, stack: bool = False):
        ticker = self.ticker if not ticker else ticker
        res = opt.get_expiration_dates(ticker=ticker)

        if not stack:
            return res

        return res


def string_number_parser(x):
        if type(x) != str:
            return np.nan
        elif 'k' in x:
            return float(x.split('k')[0]) / 1000
        elif 'M' in x:
            return float(x.split('M')[0])
        elif 'B' in x:
            return float(x.split('B')[0]) * 1000
        elif 'T' in x:
            return float(x.split('T')[0]) * 1000000
        else:
            print(x)
            return 0


def string_number_parser_v2(x):
    if type(x) != str:
        return np.nan
    elif 'k' in x:
        return float(x.split('k')[0]) * 1000
    else:
        return float(x)


if __name__ == '__main__':

    y = YahooFin(ticker='')
    dow_ts = y.tickers_dow()
    sp500_ts = y.tickers_sp500()
    nasdaq_ts = y.tickers_nasdaq()

    ts = list(set(dow_ts + sp500_ts))
    ts.sort()

    # stats = pd.read_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/stats_baseline.csv').set_index('Unnamed: 0')
    # ts = [x for x in ts if x not in stats.index.to_list()]

    begin = time()
    stats = None
    for index, t in enumerate(ts):
        cl = YahooFin(ticker=t)
        try:
            if index == 0 and stats is None:
                stats = cl.collect_statistics()
            else:
                stats = stats.append(cl.collect_statistics())
            print("Collecting statistics for {} - {}/{} - # na: {} - Elapsed time: {} - Remaining time: {}".format(t, index + 1, len(ts), stats.iloc[-1][stats.iloc[-1].isna()].shape[0], seconds_to_time_str(time() - begin), seconds_to_time_str((time() - begin) * (len(ts) - index - 1) / (index + 1))))
        except:
            print("ERROR collecting {}".format(t))

    stats_copy = stats.copy()
    stats = stats_copy.copy()

    stats['Market Cap (intraday)'] = stats['Market Cap (intraday)'].apply(lambda x: string_number_parser(x))
    stats['Enterprise Value'] = stats['Enterprise Value'].apply(lambda x: string_number_parser(x))
    stats['Trailing P/E'] = stats['Trailing P/E'].apply(lambda x: string_number_parser_v2(x))
    stats['Forward P/E'] = stats['Forward P/E'].apply(lambda x: string_number_parser_v2(x))
    stats['PEG Ratio (5 yr expected)'] = stats['PEG Ratio (5 yr expected)'].astype(float)
    stats['Price/Sales (ttm)'] = stats['Price/Sales (ttm)'].astype(float)
    stats['Price/Book (mrq)'] = stats['Price/Book (mrq)'].astype(float)
    stats['Enterprise Value/Revenue'] = stats['Enterprise Value/Revenue'].astype(float)
    stats['Enterprise Value/EBITDA'] = stats['Enterprise Value/EBITDA'].apply(lambda x: string_number_parser_v2(x))
    stats['Beta (5Y Monthly)'] = stats['Beta (5Y Monthly)'].astype(float)
    stats['52-Week Change 3'] = stats['52-Week Change 3'].str.strip('%').astype(float)
    stats['S&P500 52-Week Change 3'] = stats['S&P500 52-Week Change 3'].str.strip('%').astype(float)
    stats['52 Week High 3'] = stats['52 Week High 3'].astype(float)
    stats['52 Week Low 3'] = stats['52 Week Low 3'].astype(float)
    stats['50-Day Moving Average 3'] = stats['50-Day Moving Average 3'].astype(float)
    stats['200-Day Moving Average 3'] = stats['200-Day Moving Average 3'].astype(float)
    stats['Avg Vol (3 month) 3'] = stats['Avg Vol (3 month) 3'].apply(lambda x: string_number_parser(x))
    stats['Avg Vol (10 day) 3'] = stats['Avg Vol (10 day) 3'].apply(lambda x: string_number_parser(x))
    stats['Shares Outstanding 5'] = stats['Shares Outstanding 5'].apply(lambda x: string_number_parser(x))
    stats['Float 8'] = stats['Float 8'].apply(lambda x: string_number_parser(x))
    stats['% Held by Insiders 1'] = stats['% Held by Insiders 1'].str.strip('%').astype(float)
    stats['% Held by Institutions 1'] = stats['% Held by Institutions 1'].str.strip('%').astype(float)
    stats['Forward Annual Dividend Rate 4'] = stats['Forward Annual Dividend Rate 4'].astype(float)
    stats['Forward Annual Dividend Yield 4'] = stats['Forward Annual Dividend Yield 4'].str.strip('%').astype(float)
    stats['Trailing Annual Dividend Rate 3'] = stats['Trailing Annual Dividend Rate 3'].astype(float)
    stats['Trailing Annual Dividend Yield 3'] = stats['Trailing Annual Dividend Yield 3'].str.strip('%').astype(float)
    stats['5 Year Average Dividend Yield 4'] = stats['5 Year Average Dividend Yield 4'].astype(float)
    stats['Payout Ratio 4'] = stats['Payout Ratio 4'].str.strip('%').astype(float)
    stats['Profit Margin'] = stats['Profit Margin'].str.strip('%').astype(float)
    stats['Operating Margin (ttm)'] = stats['Operating Margin (ttm)'].str.strip('%').astype(float)
    stats['Return on Assets (ttm)'] = stats['Return on Assets (ttm)'].str.strip('%').astype(float)
    stats['Return on Equity (ttm)'] = stats['Return on Equity (ttm)'].str.replace(',', '').str.strip('%').astype(float)
    stats['Revenue (ttm)'] = stats['Revenue (ttm)'].apply(lambda x: string_number_parser(x))
    stats['Revenue Per Share (ttm)'] = stats['Revenue Per Share (ttm)'].astype(float)
    stats['Quarterly Revenue Growth (yoy)'] = stats['Quarterly Revenue Growth (yoy)'].str.replace(',', '').str.strip('%').astype(float)
    stats['Gross Profit (ttm)'] = stats['Gross Profit (ttm)'].apply(lambda x: string_number_parser(x))
    stats['EBITDA'] = stats['EBITDA'].apply(lambda x: string_number_parser(x))
    stats['Net Income Avi to Common (ttm)'] = stats['Net Income Avi to Common (ttm)'].apply(lambda x: string_number_parser(x))
    stats['Diluted EPS (ttm)'] = stats['Diluted EPS (ttm)'].astype(float)
    stats['Quarterly Earnings Growth (yoy)'] = stats['Quarterly Earnings Growth (yoy)'].str.replace(',', '').str.strip('%').astype(float)
    stats['Total Cash (mrq)'] = stats['Total Cash (mrq)'].apply(lambda x: string_number_parser(x))
    stats['Total Cash Per Share (mrq)'] = stats['Total Cash Per Share (mrq)'].astype(float)
    stats['Total Debt (mrq)'] = stats['Total Debt (mrq)'].apply(lambda x: string_number_parser(x))
    stats['Total Debt/Equity (mrq)'] = stats['Total Debt/Equity (mrq)'].astype(float)
    stats['Current Ratio (mrq)'] = stats['Current Ratio (mrq)'].astype(float)
    stats['Book Value Per Share (mrq)'] = stats['Book Value Per Share (mrq)'].astype(float)
    stats['Operating Cash Flow (ttm)'] = stats['Operating Cash Flow (ttm)'].apply(lambda x: string_number_parser(x))
    stats['Levered Free Cash Flow (ttm)'] = stats['Levered Free Cash Flow (ttm)'].apply(lambda x: string_number_parser(x))
    stats['% of Float Held by Institutions'] = stats['% of Float Held by Institutions'].str.strip('%').astype(float)
    stats['Number of Institutions Holding Shares'] = stats['Number of Institutions Holding Shares'].astype(int)
    stats['1y Target Est'] = stats['1y Target Est'].astype(float)
    stats['EPS (TTM)'] = stats['EPS (TTM)'].astype(float)
    stats['Market Cap'] = stats['Market Cap'].apply(lambda x: string_number_parser(x))
    stats['Open'] = stats['Open'].astype(float)
    stats['PE Ratio (TTM)'] = stats['PE Ratio (TTM)'].astype(float)
    stats['Previous Close'] = stats['Previous Close'].astype(float)
    stats['Previous Close'] = stats['Previous Close'].astype(float)
    stats['Quote Price'] = stats['Quote Price'].astype(float)
    stats['Volume'] = stats['Volume'].astype(float)
    stats['52WeeksPercDown'] = (1 - stats['52 Week Low 3'] / stats['52 Week High 3']) * 100
    stats['52WeeksPercSpan'] = (stats['Quote Price'] - stats['52 Week Low 3']) / (stats['52 Week High 3'] - stats['52 Week Low 3']) * 100

    stats = stats.rename(columns={'52-Week Change 3': '52-Week Change',
                                  'S&P500 52-Week Change 3': 'S&P500 52-Week Change',
                                  '52 Week High 3': '52 Week High',
                                  '52 Week Low 3': '52 Week Low',
                                  '50-Day Moving Average 3': '50-Day Moving Average',
                                  '200-Day Moving Average 3': '200-Day Moving Average',
                                  'Avg Vol (3 month) 3': 'Avg Vol (3 month)',
                                  'Avg Vol (10 day) 3': 'Avg Vol (10 day)',
                                  'Shares Outstanding 5': 'Shares Outstanding',
                                  'Implied Shares Outstanding 6': 'Implied Shares Outstanding',
                                  'Float 8': 'Float ',
                                  '% Held by Insiders 1': '% Held by Insiders ',
                                  '% Held by Institutions 1': '% Held by Institutions',
                                  'Forward Annual Dividend Rate 4': 'Forward Annual Dividend Rate',
                                  'Forward Annual Dividend Yield 4': 'Forward Annual Dividend Yield',
                                  'Trailing Annual Dividend Rate 3': 'Trailing Annual Dividend Rate',
                                  'Trailing Annual Dividend Yield 3': 'Trailing Annual Dividend Yield',
                                  '5 Year Average Dividend Yield 4': '5 Year Average Dividend Yield',
                                  'Payout Ratio 4': 'Payout Ratio',
                                  'Dividend Date 3': 'Dividend Date',
                                  'Ex-Dividend Date 4': 'Ex-Dividend Date',
                                  'Last Split Factor 2': 'Last Split Factor',
                                  'Last Split Date 3': 'Last Split Date'
                                  })

    stats_2 = stats[['Market Cap', 'Market Cap (intraday)', 'Enterprise Value', 'Previous Close', 'Quote Price',
                     'PE Ratio (TTM)', 'Trailing P/E', 'Forward P/E', 'EPS (TTM)', '52 Week Low', '52 Week High',
                     '52WeeksPercDown', '52WeeksPercSpan', 'Revenue (ttm)', 'Profit Margin', 'Gross Profit (ttm)',
                     'Net Income Avi to Common (ttm)', '% Held by Insiders ', '% Held by Institutions',
                     'Number of Institutions Holding Shares',
                     'PEG Ratio (5 yr expected)', 'Price/Sales (ttm)', 'Price/Book (mrq)',
                     'Enterprise Value/Revenue', 'Enterprise Value/EBITDA', 'Beta (5Y Monthly)', '52-Week Change',
                     'S&P500 52-Week Change', '50-Day Moving Average', '200-Day Moving Average',
                     'Avg Vol (3 month)', 'Avg Vol (10 day)', 'Shares Outstanding', 'Implied Shares Outstanding',
                     'Float ',  'Shares Short (Jan 12, 2023) 4',
                     'Short Ratio (Jan 12, 2023) 4', 'Short % of Float (Jan 12, 2023) 4',
                     'Short % of Shares Outstanding (Jan 12, 2023) 4', 'Shares Short (prior month Dec 14, 2022) 4',
                     'Forward Annual Dividend Rate', 'Forward Annual Dividend Yield', 'Trailing Annual Dividend Rate',
                     'Trailing Annual Dividend Yield', '5 Year Average Dividend Yield', 'Payout Ratio', 'Dividend Date',
                     'Ex-Dividend Date', 'Last Split Factor', 'Last Split Date', 'Fiscal Year Ends',
                     'Most Recent Quarter (mrq)',  'Operating Margin (ttm)', 'Return on Assets (ttm)',
                     'Return on Equity (ttm)',  'Revenue Per Share (ttm)',
                     'Quarterly Revenue Growth (yoy)',  'EBITDA',
                     'Diluted EPS (ttm)', 'Quarterly Earnings Growth (yoy)', 'Total Cash (mrq)',
                     'Total Cash Per Share (mrq)', 'Total Debt (mrq)', 'Total Debt/Equity (mrq)',
                     'Current Ratio (mrq)', 'Book Value Per Share (mrq)', 'Operating Cash Flow (ttm)',
                     'Levered Free Cash Flow (ttm)', '% of Float Held by Institutions', '1y Target Est',
                     "Day's Range", 'Earnings Date', 'Ex-Dividend Date', 'Open', 'Volume']]

    print('end')

    stats.to_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/stats_dow.csv')

    # result = fin.get_analyst_info(ticker='META', stack=True)
    # bs = fin.get_balance_sheet(ticker='META', stack=True)
    # cf = fin.get_cash_flow(ticker='META', stack=True)
    # ist = fin.get_income_statement(ticker='META', stack=True)
    # result = fin.get_data(ticker='META', stack=True)
    # result = fin.get_day_gainers(stack=True)
    # result = fin.get_day_losers(stack=True)
    # result = fin.get_day_most_active(stack=True)
    # result = fin.get_holders(ticker='META', stack=True)
    # result = fin.get_quote_table(ticker='META', parse=True)
    # result = fin.get_stats(ticker='META')
    # result = fin.get_stats_valuation(ticker='META', as_dict=True)
    # result = fin.tickers_dow()
    # result = fin.tickers_nasdaq()
    # result = fin.tickers_sp500()
    # result = fin.tickers_other()

    # result = fin.get_calls(ticker='META', stack=True)
    # result = fin.get_puts(ticker='META', stack=True)
    # result = fin.get_options(ticker='META')
    # result = fin.get_expiration_dates(ticker='META')

    print('end')


