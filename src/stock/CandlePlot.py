from dataclasses import dataclass, field
import pandas as pd

import matplotlib
import matplotlib.dates as mpl_dates
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.ticker import MaxNLocator, FuncFormatter
from mplfinance.original_flavor import candlestick_ohlc
import mplfinance as mpf
from mplfinance.original_flavor import candlestick_ohlc

from src.common.tools.library import *
from CandleDataInterval import CandleDataInterval

matplotlib.use('TkAgg')


@dataclass
class CandlePlot:
    symbol: str                                     # The ticker symbol
    interval: CandleDataInterval                    # The time interval for the candle data
    candle_data: pd.DataFrame                       # DataFrame to store the candle data
    oscillator_column: str = field(default=None)      # The oscillator column to plot
    column_to_use: str = 'Session'                  # The column to use for the x-axis, whether 'Session' or 'Date'

    """ Data preparation for plotting and accessories functions"""
    def sanitize_data_for_plot(self) -> None:
        """Sanitize the data for plotting"""
        candle_data = self.candle_data

        # Remove rows with empty values in the 'Trend' column
        candle_data = candle_data[candle_data['Trend'] != '']

        # Convert the 'Date' column to datetime if it's not already
        if not np.issubdtype(candle_data['Date'].dtype, np.datetime64):
            candle_data['Date'] = pd.to_datetime(candle_data['Date'], errors='coerce')

        # Convert dates to matplotlib float format
        candle_data['Date'] = candle_data['Date'].apply(mpl_dates.date2num)

        # Ensure numeric columns are float and handle conversion errors
        for col in ['Open', 'High', 'Low', 'Close', self.oscillator_column]:
            candle_data[col] = pd.to_numeric(candle_data[col], errors='coerce')

        # Drop rows with NaN values that couldn't be converted
        candle_data = candle_data.dropna()

        # Sort the data by date
        candle_data = candle_data.sort_values('Date')
        # candle_data['Session'] = range(len(candles))

        self.candle_data = candle_data
        return None

    def get_intervals(self) -> list:
        # __ identify the points where the trend changes __
        intervals_ = []
        for i_ in range(1, len(self.candle_data)):
            if self.candle_data['Trend'].iloc[i_] != self.candle_data['Trend'].iloc[i_ - 1]:
                trend_ = self.candle_data['Trend'].iloc[i_]
                intervals_.append({'index_stop': i_, 'trend': trend_})

        return intervals_

    def format_date(self, x, pos):
        """Format the date for the x-axis of the plot"""
        try:
            return mpl_dates.num2date(self.candle_data['Date'].iloc[int(pos)]).strftime('%d-%m-%Y')
        except IndexError:
            return ''

    @staticmethod
    def calculate_slope(x1, y1, x2, y2):
        return (y2 - y1) / (x2 - x1)

    """ Intermediate plotting functions """
    def plot_sens_down(self, ax,  start_index_, i_, i_end, linewidth_=1):
        # ax1.axhline(y=self.candle_data.iloc[i]['currMax'] * (1 - self.candle_data.iloc[i]['SensDown'] / 100),
        #             color='red',
        #             linestyle='--',
        #             linewidth=1)
        ax.plot([self.candle_data.iloc[start_index_][self.column_to_use], self.candle_data.iloc[i_end][self.column_to_use]],
                [self.candle_data.iloc[i_]['currMax'] * (1 - self.candle_data.iloc[i_]['SensDown'] / 100),
                 self.candle_data.iloc[i_]['currMax'] * (1 - self.candle_data.iloc[i_]['SensDown'] / 100)],
                color='red',
                linestyle='--',
                linewidth=linewidth_)

    def plot_sens_up(self, ax, start_index_, i_, i_end, linewidth_=1):
        # ax1.axhline(y=self.candle_data.iloc[i - 1]['currMin'] * (1 + self.candle_data.iloc[i]['SensUp'] / 100),
        #             color='green',
        #             linestyle='--',
        #             linewidth=1)
        ax.plot([self.candle_data.iloc[start_index_][self.column_to_use], self.candle_data.iloc[i_end][self.column_to_use]],
                [self.candle_data.iloc[i_]['currMin'] * (1 + self.candle_data.iloc[i_]['SensUp'] / 100),
                 self.candle_data.iloc[i_]['currMin'] * (1 + self.candle_data.iloc[i_]['SensUp'] / 100)],
                color='green',
                linestyle='--',
                linewidth=linewidth_)

    def plot_cur_max(self, ax, start_index_, i_, linewidth_=1):
        # ax1.axhline(y=self.candle_data.iloc[i]['currMax'], color='lightgreen', linestyle='--', linewidth=linewidth)

        ax.plot([self.candle_data.iloc[start_index_][self.column_to_use], self.candle_data.iloc[i_][self.column_to_use]],
                [self.candle_data.iloc[i_]['currMax'], self.candle_data.iloc[i_]['currMax']],
                color='lightgreen',
                linestyle='--',
                linewidth=linewidth_)

    def plot_cur_min(self, ax, start_index_, i_, linewidth_=1):
        # ax1.axhline(y=self.candle_data.iloc[i]['currMin'], color='lightcoral', linestyle='--', linewidth=linewidth)
        ax.plot([self.candle_data.iloc[start_index_][self.column_to_use], self.candle_data.iloc[i_][self.column_to_use]],
                [self.candle_data.iloc[i_]['currMin'], self.candle_data.iloc[i_]['currMin']],
                color='lightcoral',
                linestyle='--',
                linewidth=linewidth_)

    def plot_intervals(self, ax, i, intervals):
        for interval in intervals:
            if interval['index_stop'] <= i:
                ax.axvline(x=self.candle_data.iloc[interval['index_stop']][self.column_to_use], linestyle='--',
                           color='green' if interval['trend'] == 'up' else 'red', alpha=0.5, linewidth=1)

    def plot_trend_line(self, ax, i):
        # Plotting the trend line between min_1 and min_2
        # ax1.plot([self.candle_data.iloc[i]['session_min_1'], self.candle_data.iloc[i]['session_min_2']],
        #          [self.candle_data.iloc[i]['min_1'], self.candle_data.iloc[i]['min_2']], linestyle='-', color='black', alpha=0.5, linewidth=1)

        # Plotting the trend line between max_1 and max_2
        # ax1.plot([self.candle_data.iloc[i]['session_max_1'], self.candle_data.iloc[i]['session_max_2']],
        #          [self.candle_data.iloc[i]['max_1'], self.candle_data.iloc[i]['max_2']], linestyle='-', color='black', alpha=0.5, linewidth=1)

        # __ plotting the extended trend line between min_1 and min_2 __
        x1_min = self.candle_data.iloc[i]['session_min_1']
        y1_min = self.candle_data.iloc[i]['min_1']
        x2_min = self.candle_data.iloc[i]['session_min_2']
        y2_min = self.candle_data.iloc[i]['min_2']

        x_min = min(x1_min, x2_min)
        y_min = y1_min if x1_min == x_min else y2_min

        session_ = self.candle_data.iloc[i]['Session']
        slope_min = self.calculate_slope(x1_min, y1_min, x2_min, y2_min)
        extended_y_min = y2_min + slope_min * (session_ - x2_min)
        ax.plot([x_min, session_], [y_min, extended_y_min], linestyle='-', color='black', alpha=0.5, linewidth=1)

        # Plotting the trend line between max_1 and max_2
        x1_max = self.candle_data.iloc[i]['session_max_1']
        y1_max = self.candle_data.iloc[i]['max_1']
        x2_max = self.candle_data.iloc[i]['session_max_2']
        y2_max = self.candle_data.iloc[i]['max_2']

        x_max = min(x1_max, x2_max)
        y_max = y1_max if x1_max == x_max else y2_max

        slope_max = self.calculate_slope(x1_max, y1_max, x2_max, y2_max)
        extended_x_max = x2_max + 10  # extend 10 sessions further
        extended_y_max = y2_max + slope_max * (session_ - x2_max)
        ax.plot([x_max, session_], [y_max, extended_y_max], linestyle='-', color='black', alpha=0.5, linewidth=1)

    def plot_local_max(self, ax, i):
        for window in [13, 27, 51, 101]:
            linewidth = 0.5 if window == 13 else 1 if window == 27 else 1.5 if window == 51 else 2
            alpha = 0.5
            vertical_lines = self.candle_data[self.candle_data[f'shifted_local_max_{window}']]['Session'].values.tolist()
            vertical_lines = [x - window // 2 for x in vertical_lines if self.candle_data.iloc[window // 2]['Session'] <= x <= self.candle_data.iloc[i]['Session']]
            for index, x in enumerate(vertical_lines):
                # if index == len(vertical_lines) - 1 and x == self.candle_data.iloc[i]['Session']:
                #     alpha *= 2
                ax.axvline(x=x, linestyle='--', color='blue', alpha=alpha, linewidth=linewidth)

    def plot_resistance(self, ax, i):
        for window in [13, 27, 51, 101]:
            linewidth = 0.5 if window == 13 else 1 if window == 27 else 1.5 if window == 51 else 2
            vertical_lines = self.candle_data[self.candle_data[f'shifted_local_max_{window}']]['Session'].values.tolist()
            vertical_lines = [x - window // 2 for x in vertical_lines if self.candle_data.iloc[window // 2]['Session'] <= x <= self.candle_data.iloc[i]['Session']]
            if window in [13, 27, 51] and len(vertical_lines) > 1:
                vertical_lines = [vertical_lines[-1]]
            for x in vertical_lines:
                index = self.candle_data[self.candle_data['Session'] == x].index[0]
                ax.plot([self.candle_data.iloc[index][self.column_to_use], self.candle_data.iloc[i][self.column_to_use]],
                        [self.candle_data.iloc[index]['High'], self.candle_data.iloc[index]['High']],
                        color='red' if self.candle_data.iloc[i]['Close'] < self.candle_data.iloc[index]['High'] else 'green',
                        linestyle='--',
                        alpha=0.5,
                        linewidth=linewidth)

    def plot_support(self, ax, i):
        for window in [13, 27, 51, 101]:
            linewidth = 0.5 if window == 13 else 1 if window == 27 else 1.5 if window == 51 else 2
            vertical_lines = self.candle_data[self.candle_data[f'shifted_local_min_{window}']]['Session'].values.tolist()
            vertical_lines = [x - window // 2 for x in vertical_lines if self.candle_data.iloc[window // 2]['Session'] <= x <= self.candle_data.iloc[i]['Session']]
            if window in [13, 27, 51] and len(vertical_lines) > 1:
                vertical_lines = [vertical_lines[-1]]
            for x in vertical_lines:
                index = self.candle_data[self.candle_data['Session'] == x].index[0]
                ax.plot([self.candle_data.iloc[index][self.column_to_use], self.candle_data.iloc[i][self.column_to_use]],
                        [self.candle_data.iloc[index]['Low'], self.candle_data.iloc[index]['Low']],
                        color='red' if self.candle_data.iloc[i]['Close'] < self.candle_data.iloc[index]['Low'] else 'green',
                        linestyle='--',
                        alpha=0.5,
                        linewidth=linewidth)

    def plot_sens_channel(self, intervals, ax, i, linewidth, not_is_interval):
        previous_interval = next((interval for interval in reversed(intervals) if interval['index_stop'] <= i - 1), {'index_stop': 0, 'trend': self.candle_data['Trend'].iloc[0]})
        current_interval = next((interval for interval in reversed(intervals) if interval['index_stop'] <= i), {'index_stop': 0, 'trend': self.candle_data['Trend'].iloc[0]})
        start_index = current_interval['index_stop']
        previous_start_index = previous_interval['index_stop']

        # Add horizontal dashed lines for currMin, currMin*(1+SensUp), currMax, currMax*(1-SensDown)
        if self.candle_data.iloc[i]['Trend'] == 'up':
            self.plot_sens_down(ax, start_index, i, i) if not_is_interval else self.plot_sens_up(ax, previous_start_index, i - 1, i)
            self.plot_cur_max(ax, start_index, i, linewidth_=linewidth)

        elif self.candle_data.iloc[i]['Trend'] == 'down':
            self.plot_sens_up(ax, start_index, i, i) if not_is_interval else self.plot_sens_down(ax, previous_start_index, i - 1, i)
            self.plot_cur_min(ax, start_index, i, linewidth_=linewidth)

    def plot_moving_average(self, ax, i):
        ax.plot(self.candle_data[self.column_to_use].iloc[:i + 1],
                self.candle_data['MA200'].iloc[:i + 1],
                linestyle='-',
                linewidth=1,
                color='goldenrod',
                label='MA200',
                alpha=0.5)

    def plot_oscillator(self, ax, i):
        ax.plot(self.candle_data.iloc[:i + 1][self.column_to_use],
                self.candle_data.iloc[:i + 1][self.oscillator_column],
                color='blue')

    """ Plotting functions """
    def plot_candle_data(self,
                         plot_ma: bool = False,
                         plot_oscillator: bool = False,
                         plot_sensitivity_channel: bool = False,
                         plot_intervals: bool = False,
                         plot_trend_lines: bool = False,
                         plot_local_max: bool = False,
                         ):

        def clear_and_set_axis():
            ax1.clear()  # Clear the plot to update it
            ax2.clear()  # Clear the plot to update it

            # ax1.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%m-%Y'))
            ax1.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            ax1.set_xlabel(self.column_to_use)
            ax1.set_ylabel('Price')
            ax1.grid(color='lightgray', linestyle='-', linewidth=0.3)

            # Determine y-axis limits
            ymax = self.candle_data.iloc[0:min(i + 100, len(self.candle_data))]['High'].max() * 1.1
            ymin = self.candle_data.iloc[0:min(i + 100, len(self.candle_data))]['Low'].min() * 0.9
            ax1.set_ylim(ymin, ymax)

            ax2.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%m-%Y')) if self.column_to_use == 'Date' else None
            # ax2.set_xlabel(column_to_use)
            ax2.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            ax2.set_ylabel(self.oscillator_column)
            ax2.grid()

            # Set the secondary x-axis to Date
            sec_ax1 = ax1.secondary_xaxis('top')
            sec_ax1.set_xlabel(f"{self.symbol}")
            sec_ax1.xaxis.set_major_formatter(FuncFormatter(self.format_date))

            sec_ax2 = ax2.secondary_xaxis('bottom')
            # sec_ax2.set_xlabel('Date')
            sec_ax2.xaxis.set_major_formatter(FuncFormatter(self.format_date))

        # __ sanitize the data for plotting __
        self.sanitize_data_for_plot()

        # __ identify the points where the trend changes __
        intervals = self.get_intervals()

        # Creating Subplots
        fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})
        plt.ion()               # Enable interactive mode
        plt.show(block=False)   # Show the plot without blocking the execution

        for i in range(1, len(self.candle_data)):
            # __ calculate useful variables __
            not_is_interval = i not in [interval['index_stop'] for interval in intervals]
            linewidth = 1 if not_is_interval else 2
            block_unlocked = self.candle_data['block'].iloc[i] > 7

            # __ clear and set axis __
            fig.suptitle(f'Candlestick Chart with Trend Change Lines and {self.oscillator_column} - {i} candles')
            fig.autofmt_xdate()

            # __ clear and set axis __
            clear_and_set_axis()

            # __ plot up to the current candle __
            candlestick_ohlc(ax1, self.candle_data.iloc[:i + 1][[self.column_to_use, 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='green', colordown='red', alpha=0.8)

            # __ add moving averages __
            self.plot_moving_average(ax1, i) if plot_ma else None

            # __ add oscillator __
            self.plot_oscillator(ax2, i) if plot_oscillator else None

            # __ add trendlines __
            self.plot_trend_line(ax=ax1, i=i) if block_unlocked and plot_trend_lines else None

            # __ add vertical lines where the trend changes up to the current candle __
            self.plot_intervals(ax1, i, intervals) if plot_intervals else None

            # __ add local maxima __
            # self.plot_local_max(ax1, i) if plot_local_max else None
            self.plot_resistance(ax1, i) if plot_local_max else None
            self.plot_support(ax1, i) if plot_local_max else None

            # __ add sensitivity channel __
            self.plot_sens_channel(intervals, ax1, i, linewidth, not_is_interval) if plot_sensitivity_channel else None

            fig.canvas.draw()   # Draw the canvas to ensure it updates
            plt.pause(0.01)      # A shorter pause to allow the plot to update

            # Pause for 2 seconds if the current candle is a trend change point
            if not not_is_interval and (plot_intervals or plot_sensitivity_channel):
                sleep(2)

        plt.ioff()  # Disable interactive mode
        plt.show()  # Keep the plot open after the loop


