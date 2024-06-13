import yfinance as yf
from dataclasses import dataclass
import pandas as pd
import numpy as np
from time import sleep, time
from src.common.tools.library import seconds_to_time_str


@dataclass
class YahooFin:
    ticker: str

    def get_financial_data(self) -> dict:
        stock = yf.Ticker(self.ticker)
        financial_data = {}

        try:
            # Summary Table
            financial_data['Previous Close'] = stock.info['previousClose']  # The closing price of the stock on the previous trading day.
            financial_data['Open'] = stock.info['open']  # The price at which the stock opened on the current trading day.
            financial_data['Bid'] = stock.info['bid']  # The highest price a buyer is willing to pay for the stock.
            financial_data['Ask'] = stock.info['ask']  # The lowest price a seller is willing to accept for the stock.
            financial_data['Volume'] = stock.info['volume']  # The number of shares traded during the current trading day.
            financial_data['Avg. Volume'] = stock.info['averageVolume']  # The average number of shares traded per day over a specific period, typically 3 months.
            financial_data['Market Cap'] = stock.info['marketCap']  # The total market value of the company’s outstanding shares.
            financial_data['Beta (5Y Monthly)'] = stock.info['beta']  # A measure of the stock's volatility compared to the market.
            financial_data['PE Ratio (TTM)'] = stock.info['trailingPE']  # The price-to-earnings ratio, calculated by dividing the current stock price by the earnings per share (EPS) over the trailing twelve months (TTM).
            financial_data['EPS (TTM)'] = stock.info['trailingEps']  # Earnings per share over the trailing twelve months.
            financial_data['Earnings Date'] = stock.earnings_dates  # The next date on which the company is scheduled to report its earnings.
            financial_data['Forward Dividend & Yield'] = stock.info['dividendYield']  # The expected annual dividend payment and its yield based on the current stock price.
            financial_data['Ex-Dividend Date'] = stock.info['exDividendDate']  # The date on which the stock starts trading without the value of its next dividend payment.
            financial_data['1y Target Est'] = stock.info['targetMeanPrice']  # The average target price estimated by analysts for the next year.

            # Valuation Measures
            financial_data['Market Cap (intraday)'] = stock.info['marketCap']  # The total market value of the company’s outstanding shares.
            financial_data['Enterprise Value'] = stock.info['enterpriseValue']  # A measure of a company's total value.
            financial_data['Trailing P/E'] = stock.info['trailingPE']  # The price-to-earnings ratio calculated using the earnings over the last 12 months.
            financial_data['Forward P/E'] = stock.info['forwardPE']  # The price-to-earnings ratio calculated using the forecasted earnings for the next 12 months.
            financial_data['PEG Ratio (5 yr expected)'] = stock.info['pegRatio']  # The price/earnings-to-growth ratio.
            financial_data['Price/Sales (ttm)'] = stock.info['priceToSalesTrailing12Months']  # The price-to-sales ratio calculated over the trailing twelve months.
            financial_data['Price/Book (mrq)'] = stock.info['priceToBook']  # The price-to-book ratio calculated using the most recent quarter's data.
            financial_data['Enterprise Value/Revenue'] = stock.info['enterpriseToRevenue']  # A measure of a company's total value divided by its revenue.
            financial_data['Enterprise Value/EBITDA'] = stock.info['enterpriseToEbitda']  # A measure of a company's total value divided by its earnings before interest, taxes, depreciation, and amortization.

            # Financial Highlights
            financial_data['Fiscal Year Ends'] = stock.info['fiscalYearEnd']  # The month and day when the company's fiscal year ends.
            financial_data['Most Recent Quarter (mrq)'] = stock.info['mostRecentQuarter']  # The most recent quarter's data.
            financial_data['Profit Margin'] = stock.info['profitMargins']  # The percentage of revenue that is profit.
            financial_data['Operating Margin (ttm)'] = stock.info['operatingMargins']  # The percentage of revenue that is operating profit.
            financial_data['Return on Assets (ttm)'] = stock.info['returnOnAssets']  # The percentage return on the company's assets.
            financial_data['Return on Equity (ttm)'] = stock.info['returnOnEquity']  # The percentage return on the company's equity.
            financial_data['Revenue (ttm)'] = stock.info['totalRevenue']  # Total revenue over the trailing twelve months.
            financial_data['Revenue Per Share (ttm)'] = stock.info['revenuePerShare']  # Total revenue per share over the trailing twelve months.
            financial_data['Quarterly Revenue Growth (yoy)'] = stock.info['revenueQuarterlyGrowth']  # The year-over-year quarterly revenue growth.
            financial_data['Gross Profit (ttm)'] = stock.info['grossProfits']  # Total gross profit over the trailing twelve months.
            financial_data['EBITDA'] = stock.info['ebitda']  # Earnings before interest, taxes, depreciation, and amortization.
            financial_data['Net Income Avi to Common (ttm)'] = stock.info['netIncomeToCommon']  # Net income available to common shareholders over the trailing twelve months.
            financial_data['Diluted EPS (ttm)'] = stock.info['trailingEps']  # Diluted earnings per share over the trailing twelve months.
            financial_data['Quarterly Earnings Growth (yoy)'] = stock.info['earningsQuarterlyGrowth']  # The year-over-year quarterly earnings growth.
            financial_data['Total Cash (mrq)'] = stock.info['totalCash']  # Total cash on hand at the most recent quarter.
            financial_data['Total Cash Per Share (mrq)'] = stock.info['totalCashPerShare']  # Total cash per share at the most recent quarter.
            financial_data['Total Debt (mrq)'] = stock.info['totalDebt']  # Total debt at the most recent quarter.
            financial_data['Total Debt/Equity (mrq)'] = stock.info['debtToEquity']  # Total debt divided by total equity at the most recent quarter.
            financial_data['Current Ratio (mrq)'] = stock.info['currentRatio']  # Current assets divided by current liabilities at the most recent quarter.
            financial_data['Book Value Per Share (mrq)'] = stock.info['bookValue']  # Total equity divided by total shares outstanding at the most recent quarter.
            financial_data['Operating Cash Flow (ttm)'] = stock.info['operatingCashflow']  # Total operating cash flow over the trailing twelve months.
            financial_data['Levered Free Cash Flow (ttm)'] = stock.info['freeCashflow']  # Total levered free cash flow over the trailing twelve months.

            # Trading Information
            financial_data['52-Week Change'] = stock.info['52WeekChange']  # The percentage change in the stock price over the last 52 weeks.
            financial_data['52 Week High'] = stock.info['fiftyTwoWeekHigh']  # The highest price at which the stock has traded over the last 52 weeks.
            financial_data['52 Week Low'] = stock.info['fiftyTwoWeekLow']  # The lowest price at which the stock has traded over the last 52 weeks.
            financial_data['50-Day Moving Average'] = stock.info['fiftyDayAverage']  # The average price over the last 50 days.
            financial_data['200-Day Moving Average'] = stock.info['twoHundredDayAverage']  # The average price over the last 200 days.
            financial_data['Avg Vol (3 month)'] = stock.info['averageVolume']  # The average number of shares traded per day over a 3-month period.
            financial_data['Avg Vol (10 day)'] = stock.info['averageDailyVolume10Day']  # The average number of shares traded per day over a 10-day period.
            financial_data['Shares Outstanding'] = stock.info['sharesOutstanding']  # The total number of shares currently outstanding.
            financial_data['Float'] = stock.info['floatShares']  # The number of shares available for trading by the public.
            financial_data['% Held by Insiders'] = stock.info['heldPercentInsiders']  # The percentage of shares held by insiders.
            financial_data['% Held by Institutions'] = stock.info['heldPercentInstitutions']  # The percentage of shares held by institutional investors.
            financial_data['Shares Short'] = stock.info['sharesShort']  # The total number of shares currently sold short.
            financial_data['Short Ratio'] = stock.info['shortRatio']  # The ratio of shares short to the average daily volume.
            financial_data['Short % of Float'] = stock.info['shortPercentOfFloat']  # The percentage of the float that is sold short.
            financial_data['Short % of Shares Outstanding'] = stock.info['sharesPercentSharesOut']  # The percentage of shares outstanding that is sold short.
            financial_data['Shares Short (prior month)'] = stock.info['sharesShortPriorMonth']  # The total number of shares sold short in the prior month.

            # Dividends & Splits
            financial_data['Forward Annual Dividend Rate'] = stock.info['dividendRate']  # The expected annual dividend payment.
            financial_data['Forward Annual Dividend Yield'] = stock.info['dividendYield']  # The expected annual dividend yield.
            financial_data['Trailing Annual Dividend Rate'] = stock.info['trailingAnnualDividendRate']  # The trailing annual dividend payment.
            financial_data['Trailing Annual Dividend Yield'] = stock.info['trailingAnnualDividendYield']  # The trailing annual dividend yield.
            financial_data['Payout Ratio'] = stock.info['payoutRatio']  # The percentage of earnings paid out as dividends.
            financial_data['Dividend Date'] = stock.info['dividendDate']  # The date of the next dividend payment.
            financial_data['Ex-Dividend Date'] = stock.info['exDividendDate']  # The date on which the stock starts trading without the value of its next dividend payment.
            financial_data['Last Split Factor'] = stock.info['lastSplitFactor']  # The ratio of the last stock split.
            financial_data['Last Split Date'] = stock.info['lastSplitDate']  # The date of the last stock split.

            # Profile Tab
            financial_data['Sector'] = stock.info['sector']  # The industry sector the company operates in.
            financial_data['Industry'] = stock.info['industry']  # The specific industry the company operates in.
            financial_data['Full Time Employees'] = stock.info['fullTimeEmployees']  # The total number of full-time employees.
            financial_data['Business Summary'] = stock.info['longBusinessSummary']  # A brief overview of the company's operations, products, and services.
            financial_data['Website'] = stock.info['website']  # The company's website.

            # Financial Tab
            financial_data['Total Revenue'] = stock.financials.loc['Total Revenue'].iloc[0]  # The total revenue generated by the company.
            financial_data['Cost of Revenue'] = stock.financials.loc['Cost Of Revenue'].iloc[0]  # The total cost incurred to generate the revenue.
            financial_data['Gross Profit'] = stock.financials.loc['Gross Profit'].iloc[0]  # The profit after deducting the cost of revenue.
            financial_data['Operating Expense'] = stock.financials.loc['Operating Expense'].iloc[0]  # The total operating expenses.
            financial_data['Operating Income'] = stock.financials.loc['Operating Income'].iloc[0]  # The income from operations.
            financial_data['Net Non Operating Interest Income Expense'] = stock.financials.loc['Interest Expense'].iloc[0]  # The net interest income/expense from non-operating activities.
            financial_data['Other Income Expense'] = stock.financials.loc['Other Income Expense'].iloc[0]  # Other income and expenses.
            financial_data['Pretax Income'] = stock.financials.loc['Pretax Income'].iloc[0]  # The income before taxes.
            financial_data['Tax Provision'] = stock.financials.loc['Tax Provision'].iloc[0]  # The provision for income taxes.
            financial_data['Net Income Common Stockholders'] = stock.financials.loc['Net Income Common Stockholders'].iloc[0]  # The net income attributable to common stockholders.
            financial_data['Diluted NI Available to Com Stockholders'] = stock.financials.loc['Diluted NI Available to Com Stockholders'].iloc[0]  # The diluted net income available to common stockholders.
            financial_data['Basic EPS'] = stock.financials.loc['Basic EPS'].iloc[0]  # The basic earnings per share.
            financial_data['Diluted EPS'] = stock.financials.loc['Diluted EPS'].iloc[0]  # The diluted earnings per share.
            financial_data['Basic Average Shares'] = stock.financials.loc['Basic Average Shares'].iloc[0]  # The basic average shares outstanding.
            financial_data['Diluted Average Shares'] = stock.financials.loc['Diluted Average Shares'].iloc[0]  # The diluted average shares outstanding.
            financial_data['Total Operating Income as Reported'] = stock.financials.loc['Total Operating Income as Reported'].iloc[0]  # The total operating income as reported.
            financial_data['Total Expenses'] = stock.financials.loc['Total Expenses'].iloc[0]  # The total expenses.
            financial_data['Net Income from Continuing & Discontinued Operation'] = stock.financials.loc['Net Income from Continuing & Discontinued Operation'].iloc[0]  # The net income from continuing and discontinued operations.
            financial_data['Normalized Income'] = stock.financials.loc['Normalized Income'].iloc[0]  # The normalized income.
            financial_data['Interest Income'] = stock.financials.loc['Interest Income'].iloc[0]  # The interest income.
            financial_data['Interest Expense'] = stock.financials.loc['Interest Expense'].iloc[0]  # The interest expense.
            financial_data['Net Interest Income'] = stock.financials.loc['Net Interest Income'].iloc[0]  # The net interest income.
            financial_data['EBIT'] = stock.financials.loc['EBIT'].iloc[0]  # Earnings before interest and taxes.
            financial_data['EBITDA'] = stock.financials.loc['EBITDA'].iloc[0]  # Earnings before interest, taxes, depreciation, and amortization.

            # Balance Sheet
            financial_data['Total Assets'] = stock.balance_sheet.loc['Total Assets'].iloc[0]  # The total assets of the company.
            financial_data['Total Liabilities Net Minority Interest'] = stock.balance_sheet.loc['Total Liabilities Net Minority Interest'].iloc[0]  # The total liabilities net of minority interest.
            financial_data['Total Equity Gross Minority Interest'] = stock.balance_sheet.loc['Total Equity Gross Minority Interest'].iloc[0]  # The total equity including minority interest.
            financial_data['Total Capitalization'] = stock.balance_sheet.loc['Total Capitalization'].iloc[0]  # The total capitalization.
            financial_data['Common Stock Equity'] = stock.balance_sheet.loc['Common Stock Equity'].iloc[0]  # The common stock equity.
            financial_data['Net Tangible Assets'] = stock.balance_sheet.loc['Net Tangible Assets'].iloc[0]  # The net tangible assets.
            financial_data['Working Capital'] = stock.balance_sheet.loc['Working Capital'].iloc[0]  # The working capital.
            financial_data['Invested Capital'] = stock.balance_sheet.loc['Invested Capital'].iloc[0]  # The invested capital.
            financial_data['Tangible Book Value'] = stock.balance_sheet.loc['Tangible Book Value'].iloc[0]  # The tangible book value.
            financial_data['Total Debt'] = stock.balance_sheet.loc['Total Debt'].iloc[0]  # The total debt.
            financial_data['Net Debt'] = stock.balance_sheet.loc['Net Debt'].iloc[0]  # The net debt.
            financial_data['Share Issued'] = stock.balance_sheet.loc['Share Issued'].iloc[0]  # The number of shares issued.
            financial_data['Ordinary Shares Number'] = stock.balance_sheet.loc['Ordinary Shares Number'].iloc[0]  # The number of ordinary shares.
            financial_data['Treasury Shares Number'] = stock.balance_sheet.loc['Treasury Shares Number'].iloc[0]  # The number of treasury shares.

            # Cash Flow
            financial_data['Operating Cash Flow'] = stock.cashflow.loc['Operating Cash Flow'].iloc[0]  # The cash flow from operating activities.
            financial_data['Investing Cash Flow'] = stock.cashflow.loc['Investing Cash Flow'].iloc[0]  # The cash flow from investing activities.
            financial_data['Financing Cash Flow'] = stock.cashflow.loc['Financing Cash Flow'].iloc[0]  # The cash flow from financing activities.
            financial_data['End Cash Position'] = stock.cashflow.loc['End Cash Position'].iloc[0]  # The ending cash position.
            financial_data['Income Tax Paid Supplemental Data'] = stock.cashflow.loc['Income Tax Paid Supplemental Data'].iloc[0]  # The income tax paid as supplemental data.
            financial_data['Interest Paid Supplemental Data'] = stock.cashflow.loc['Interest Paid Supplemental Data'].iloc[0]  # The interest paid as supplemental data.
            financial_data['Capital Expenditure'] = stock.cashflow.loc['Capital Expenditure'].iloc[0]  # The capital expenditure.
            financial_data['Issuance of Capital Stock'] = stock.cashflow.loc['Issuance of Capital Stock'].iloc[0]  # The issuance of capital stock.
            financial_data['Issuance of Debt'] = stock.cashflow.loc['Issuance of Debt'].iloc[0]  # The issuance of debt.
            financial_data['Repayment of Debt'] = stock.cashflow.loc['Repayment of Debt'].iloc[0]  # The repayment of debt.
            financial_data['Repurchase of Capital Stock'] = stock.cashflow.loc['Repurchase of Capital Stock'].iloc[0]  # The repurchase of capital stock.
            financial_data['Free Cash Flow'] = stock.cashflow.loc['Free Cash Flow'].iloc[0]  # The free cash flow.

        except KeyError as e:
            print(f"KeyError: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return financial_data

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


if __name__ == '__main__':
    # Example usage
    ticker = "AAPL"
    yahoo = YahooFin(ticker=ticker)
    data = yahoo.get_financial_data()
    print(data)


