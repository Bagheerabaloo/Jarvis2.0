import pandas as pd
import yfinance as yf


def get_base_ddl(table_name) -> str:
    return f"""CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES Ticker(id),
                date TIMESTAMP,"""


def generate_transpose_numeric_ddl(dataframe) -> str:
    ddl = ""
    for column in dataframe.T.columns:
        # Convert column names to a valid SQL identifier format (remove spaces, special characters, etc.)
        column_name = column.replace(' ', '_').replace('&', 'And').replace('/', '_').replace('(', '').replace(')', '')
        ddl += f"    {column_name} NUMERIC,\n"
    return ddl


def generate_dataframe_ddl(dataframe: pd.DataFrame, table_name: str) -> str:
    # Generate the DDL for the Actions table using these columns
    ddl = get_base_ddl(table_name=table_name)

    # Add the columns dynamically based on the actions fields
    ddl += generate_transpose_numeric_ddl(dataframe=dataframe)

    # Remove the last comma and add closing parenthesis
    ddl = ddl.rstrip(",\n") + "\n);\n"

    return ddl


def generate_ticker_ddl() -> str:
    return """CREATE TABLE ticker (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                company_name TEXT,
                sector VARCHAR(100),
                industry VARCHAR(100),
                full_time_employees INTEGER,
                business_summary TEXT,
                website VARCHAR(255)
            );\n"""


def generate_earnings_dates_ddl() -> str:
    return """CREATE TABLE earnings_dates (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date DATE,
                eps_estimate NUMERIC,
                reported_eps NUMERIC,
                surprise_percent NUMERIC
            );
            """


def generate_insider_purchases_ddl() -> str:
    return """CREATE TABLE insider_purchases (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                insider_purchases_last_6m VARCHAR(255),
                shares INTEGER,
                trans INTEGER,
                date DATE
                    );\n"""


def generate_insider_roster_holders_ddl() -> str:
    return """CREATE TABLE insider_roster_holders (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                name VARCHAR(255),
                position VARCHAR(255),
                url VARCHAR(255),
                most_recent_transaction VARCHAR(255),
                latest_transaction_date TIMESTAMP,
                shares_owned_directly NUMERIC,
                position_direct_date TIMESTAMP,
                shares_owned_indirectly NUMERIC,
                position_indirect_date TIMESTAMP,
                date DATE
            );\n"""


def generate_insider_transactions_ddl() -> str:
    return """CREATE TABLE insider_transactions (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                shares NUMERIC,
                value NUMERIC,
                url VARCHAR(255),
                text VARCHAR(255),
                insider VARCHAR(255),
                position VARCHAR(255),
                transaction VARCHAR(255),
                start_date TIMESTAMP,
                ownership VARCHAR(1)
            );\n"""


def generate_institutional_holders_ddl() -> str:
    return """CREATE TABLE institutional_holders (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date_reported TIMESTAMP,
                holder VARCHAR(255),
                pct_held NUMERIC,
                shares NUMERIC,
                value NUMERIC
            );\n"""


def generate_mutual_fund_holders_ddl() -> str:
    return """CREATE TABLE mutualfund_holders (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date_reported TIMESTAMP,
                holder VARCHAR(255),
                pct_held NUMERIC,
                shares NUMERIC,
                value NUMERIC
            );\n"""


def generate_recommendations_ddl() -> str:
    return """CREATE TABLE recommendations (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date TIMESTAMP,
                period VARCHAR(10),
                strong_buy INTEGER,
                buy INTEGER,
                hold INTEGER,
                sell INTEGER,
                strong_sell INTEGER
            );\n"""


def generate_upgrades_downgrades_ddl() -> str:
    return """CREATE TABLE upgrades_downgrades (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date TIMESTAMP,
                firm VARCHAR(255),
                to_grade VARCHAR(255),
                from_grade VARCHAR(255),
                action VARCHAR(50)
            );\n"""


def generate_info_ddl() -> str:
    return """CREATE TABLE info (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES ticker(id),
                date TIMESTAMP,
                currency VARCHAR(10),
                day_high NUMERIC,
                day_low NUMERIC,
                exchange VARCHAR(10),
                fifty_day_average NUMERIC,
                last_price NUMERIC,
                last_volume INTEGER,
                market_cap NUMERIC,
                open NUMERIC,
                previous_close NUMERIC,
                quote_type VARCHAR(10),
                regular_market_previous_close NUMERIC,
                shares BIGINT,
                ten_day_average_volume INTEGER,
                three_month_average_volume INTEGER,
                timezone VARCHAR(50),
                two_hundred_day_average NUMERIC,
                year_change NUMERIC,
                year_high NUMERIC,
                year_low NUMERIC,
                dividend_date DATE,
                ex_dividend_date DATE,
                earnings_high NUMERIC,
                earnings_low NUMERIC,
                earnings_average NUMERIC,
                revenue_high NUMERIC,
                revenue_low NUMERIC,
                revenue_average NUMERIC,
                symbol VARCHAR(10),
                exchange_name VARCHAR(50),
                full_exchange_name VARCHAR(50),
                instrument_type VARCHAR(10),
                first_trade_date BIGINT,
                regular_market_time BIGINT,
                has_pre_post_market_data BOOLEAN,
                gmt_offset INTEGER,
                exchange_timezone_name VARCHAR(50),
                regular_market_price NUMERIC,
                fifty_two_week_high NUMERIC,
                fifty_two_week_low NUMERIC,
                regular_market_day_high NUMERIC,
                regular_market_day_low NUMERIC,
                regular_market_volume INTEGER,
                chart_previous_close NUMERIC,
                scale INTEGER,
                price_hint INTEGER,
                address1 VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(50),
                zip VARCHAR(20),
                country VARCHAR(50),
                phone VARCHAR(50),
                website VARCHAR(255),
                industry VARCHAR(100),
                sector VARCHAR(100),
                long_business_summary TEXT,
                full_time_employees INTEGER,
                audit_risk INTEGER,
                board_risk INTEGER,
                compensation_risk INTEGER,
                share_holder_rights_risk INTEGER,
                overall_risk INTEGER,
                governance_epoch_date BIGINT,
                compensation_as_of_epoch_date BIGINT,
                ir_website VARCHAR(255),
                max_age INTEGER,
                regular_market_open NUMERIC,
                dividend_rate NUMERIC,
                dividend_yield NUMERIC,
                payout_ratio NUMERIC,
                five_year_avg_dividend_yield NUMERIC,
                beta NUMERIC,
                trailing_pe NUMERIC,
                forward_pe NUMERIC,
                volume INTEGER,
                average_volume INTEGER,
                average_volume_10days INTEGER,
                average_daily_volume_10day INTEGER,
                bid NUMERIC,
                ask NUMERIC,
                bid_size INTEGER,
                ask_size INTEGER,
                price_to_sales_trailing_12_months NUMERIC,
                trailing_annual_dividend_rate NUMERIC,
                trailing_annual_dividend_yield NUMERIC,
                enterprise_value NUMERIC,
                profit_margins NUMERIC,
                float_shares BIGINT,
                shares_outstanding BIGINT,
                shares_short BIGINT,
                shares_short_prior_month BIGINT,
                shares_short_previous_month_date BIGINT,
                date_short_interest BIGINT,
                shares_percent_shares_out NUMERIC,
                held_percent_insiders NUMERIC,
                held_percent_institutions NUMERIC,
                short_ratio NUMERIC,
                short_percent_of_float NUMERIC,
                implied_shares_outstanding BIGINT,
                book_value NUMERIC,
                price_to_book NUMERIC,
                last_fiscal_year_end BIGINT,
                next_fiscal_year_end BIGINT,
                most_recent_quarter BIGINT,
                earnings_quarterly_growth NUMERIC,
                net_income_to_common NUMERIC,
                trailing_eps NUMERIC,
                forward_eps NUMERIC,
                peg_ratio NUMERIC,
                last_split_factor VARCHAR(10),
                last_split_date BIGINT,
                enterprise_to_revenue NUMERIC,
                enterprise_to_ebitda NUMERIC,
                fifty_two_week_change NUMERIC,
                s_and_p_52_week_change NUMERIC,
                last_dividend_value NUMERIC,
                last_dividend_date BIGINT,
                underlying_symbol VARCHAR(10),
                short_name VARCHAR(255),
                long_name VARCHAR(255),
                first_trade_date_epoch_utc BIGINT,
                time_zone_full_name VARCHAR(50),
                time_zone_short_name VARCHAR(10),
                uuid VARCHAR(50),
                message_board_id VARCHAR(50),
                gmt_offset_milliseconds BIGINT,
                current_price NUMERIC,
                target_high_price NUMERIC,
                target_low_price NUMERIC,
                target_mean_price NUMERIC,
                target_median_price NUMERIC,
                recommendation_mean NUMERIC,
                recommendation_key VARCHAR(10),
                number_of_analyst_opinions INTEGER,
                total_cash NUMERIC,
                total_cash_per_share NUMERIC,
                ebitda NUMERIC,
                total_debt NUMERIC,
                quick_ratio NUMERIC,
                current_ratio NUMERIC,
                total_revenue NUMERIC,
                debt_to_equity NUMERIC,
                revenue_per_share NUMERIC,
                return_on_assets NUMERIC,
                return_on_equity NUMERIC,
                free_cashflow NUMERIC,
                operating_cashflow NUMERIC,
                earnings_growth NUMERIC,
                revenue_growth NUMERIC,
                gross_margins NUMERIC,
                ebitda_margins NUMERIC,
                operating_margins NUMERIC,
                financial_currency VARCHAR(10),
                trailing_peg_ratio NUMERIC
            );\n"""


if __name__ == "__main__":
    # Get the balance sheet data for a sample ticker
    ticker = "AAPL"
    stock = yf.Ticker(ticker)

    ticker_ddl = generate_ticker_ddl()

    # dictionary objects
    basic_info = dict(stock.basic_info)
    calendar = stock.calendar
    # fast_info is equal to basic_info
    history_metadata = stock.history_metadata
    info = stock.info
    dictionary = {**basic_info, **calendar, **history_metadata, **info}

    info_ddl = generate_info_ddl()

    # generate the DDL for transposed dataframes
    actions_ddl = generate_dataframe_ddl(dataframe=stock.actions, table_name="Actions")
    balance_sheet_ddl = generate_dataframe_ddl(dataframe=stock.balance_sheet, table_name="BalanceSheet")
    cash_flow_ddl = generate_dataframe_ddl(dataframe=stock.cashflow, table_name="CashFlow")
    financials_ddl = generate_dataframe_ddl(dataframe=stock.financials, table_name="Financials")
    income_statement_ddl = generate_dataframe_ddl(dataframe=stock.incomestmt, table_name="IncomeStatement")
    major_holders_ddl = generate_dataframe_ddl(dataframe=stock.major_holders, table_name="MajorHolders")
    quarterly_balance_sheet_ddl = generate_dataframe_ddl(dataframe=stock.quarterly_balance_sheet, table_name="QuarterlyBalanceSheet")
    quarterly_cash_flow_ddl = generate_dataframe_ddl(dataframe=stock.quarterly_cashflow, table_name="QuarterlyCashFlow")
    quarterly_financials_ddl = generate_dataframe_ddl(dataframe=stock.quarterly_financials, table_name="QuarterlyFinancials")
    quarterly_income_statement_ddl = generate_dataframe_ddl(dataframe=stock.quarterly_incomestmt, table_name="QuarterlyIncomeStatement")

    # generate the DDL for not transposed dataframes
    earnings_dates_ddl = generate_earnings_dates_ddl()
    insider_purchases_ddl = generate_insider_purchases_ddl()
    insider_roster_holders_ddl = generate_insider_roster_holders_ddl()
    insider_transactions_ddl = generate_insider_transactions_ddl()
    institutional_holders_ddl = generate_institutional_holders_ddl()
    mutual_fund_holders_ddl = generate_mutual_fund_holders_ddl()
    recommendations_ddl = generate_recommendations_ddl()
    upgrades_downgrades_ddl = generate_upgrades_downgrades_ddl()

    # broken methods
    analyst_price_targets_ddl = ''
    earnings = ''
    earnings_forecasts = ''
    earnings_trend = ''
    quarterly_earnings = ''
    revenue_forecasts = ''
    shares = ''
    sustainability = ''
    trend_details = ''

    # discard methods
    news = ''
    options = ''

    total_ddl = ticker_ddl + info_ddl + actions_ddl + balance_sheet_ddl + cash_flow_ddl + earnings_dates_ddl + financials_ddl + income_statement_ddl + insider_purchases_ddl + insider_roster_holders_ddl + insider_transactions_ddl + institutional_holders_ddl + major_holders_ddl + mutual_fund_holders_ddl + quarterly_balance_sheet_ddl + quarterly_cash_flow_ddl + quarterly_financials_ddl + quarterly_income_statement_ddl + recommendations_ddl + upgrades_downgrades_ddl
    dictionary_pop = {k: v for k, v in dictionary.items() if isinstance(v, (list, dict))}
    dictionary = {k: v for k, v in dictionary.items() if not isinstance(v, (list, dict))}
    df = pd.DataFrame(dictionary, index=[0])

    with open(r'C:\Users\Vale\PycharmProjects\Jarvis2.0\src\stock\DB\ddl_output.txt', 'w') as file:
        file.write(total_ddl)
