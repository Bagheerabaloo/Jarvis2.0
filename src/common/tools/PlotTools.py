import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import matplotlib.patches as patches
from matplotlib import dates, ticker
from datetime import datetime, timedelta
from scipy.stats import norm
from scipy.cluster.hierarchy import dendrogram
from scipy.cluster.hierarchy import linkage
from src.Tools.library import *


def rgb(triplet):
    _NUMERALS = '0123456789abcdefABCDEF'
    _HEXDEC = {v: int(v, 16) for v in (x + y for x in _NUMERALS for y in _NUMERALS)}
    return _HEXDEC[triplet[0:2]], _HEXDEC[triplet[2:4]], _HEXDEC[triplet[4:6]]


def new_fig(fig_size=(9, 9), nrows=1, ncols=1):

    fig_, ax_ = plt.subplots(nrows, ncols, figsize=fig_size)

    return fig_, ax_


#   ___ Histograms ____
def save_hist_fig(series, name, bins, folder='analysis/', gaussian=False, save=True, show_stats=True):
    try:
        fig = plt.figure(figsize=(16, 10))
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twinx() if gaussian else None
        mean = np.mean(series)
        sigma = np.sqrt(np.var(series))
        label = name + ' (mean: {}, std: {})'.format(round(mean, 2), round(sigma, 2)) if show_stats else name
        ax1.hist(series, bins=bins, alpha=0.5, label=label)
        ax1.set_xlabel("Time (ms)")
        ax1.set_ylabel("# of Samples")
        plt.title(name + ' (' + str(series.shape[0]) + ' deals)')
        if gaussian:
            x = np.linspace(0, series.agg('max'), bins * 5)
            ax2.plot(x, norm.pdf(x, mean, sigma), alpha=0.5)
        ax1.set_xlim(left=0)
        if gaussian:
            ax2.set_ylim(bottom=0)

        ax1.legend()
        ax1.grid()
        if save:
            fig.savefig(folder + '_'.join(
                [x.lower().replace('-', '_').replace('<', 'under').replace('>', 'over') for x in
                 name.split(' ')]) + '.png')
    except:
        print('Error in saving fig: {}'.format(name))
        print_exception()


def save_multiple_hist_fig(series, name, labels, bins, folder='analysis/', gaussian=False, save=True, show_stats=True):
    try:
        fig = plt.figure(figsize=(16, 10))
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twinx() if gaussian else None

        ax1.set_xlabel("Time (ms)")
        ax1.set_ylabel("# of Samples")

        for index, series_item in enumerate(series):
            mean = np.mean(series_item)
            sigma = np.sqrt(np.var(series_item))
            label = labels[index] + ' (mean: {}, std: {})'.format(round(mean, 2), round(sigma, 2)) if show_stats else \
                labels[index]
            ax1.hist(series_item, bins=bins[index], alpha=0.5, label=label)
            if gaussian:
                x = np.linspace(0, series_item.agg('max'), bins[index] * 5)
                ax2.plot(x, norm.pdf(x, mean, sigma), alpha=0.5)

        ax1.set_xlim(left=0)
        if gaussian:
            ax2.set_ylim(bottom=0)

        ax1.legend()
        plt.title(name + ' (' + str(series[0].shape[0]) + ' deals)')
        ax1.grid()
        if save:
            fig.savefig(folder + '_'.join(
                [x.lower().replace('-', '_').replace('<', 'under').replace('>', 'over') for x in
                 name.split(' ')]) + '.png')
    except:
        print('Error in saving fig: {}'.format(name))
        print_exception()


#   ___ Line, Charts, Pies ___

def plot_line(x, data, xlabel=None, ylabel=None, show=True):

    fig, ax = new_fig()

    ax.plot(x, data)
    ax.grid(True)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if show:
        plt.show()


def plot_line_with_dates(data, title=None, grid=True):

    # data = [x for x in data if int(x[0]) > date2timestamp('01-01-2019', '%d-%m-%Y')]

    vector = [x[1] for x in data]

    dates = [datetime.strptime(timestamp2date(int(x[0])), '%Y-%m-%d %H:%M:%S').date() for x in data]

    # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    # plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.plot(dates, vector)
    plt.gcf().autofmt_xdate()
    if title:
        plt.title(title)
    plt.grid(grid)
    plt.show()


def plot_line_with_dates_from_dataframe(df, x, y, title=None, grid=True):
    vector = df[y].values.tolist()
    dates = [datetime.strptime(x, '%Y-%m-%d').date() for x in df[x].values.tolist()]

    # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    # plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.plot(dates, vector)
    plt.gcf().autofmt_xdate()
    if title:
        plt.title(title)
    plt.grid(grid)
    plt.show()


def plot_line_with_dates_v2(data, title=None, grid=True, show=True):

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)

    vector = [x[1] for x in data]
    dates = [datetime.strptime(timestamp2date(int(x[0])), '%Y-%m-%d %H:%M:%S').date() for x in data]

    plt.plot(dates, vector)
    plt.gcf().autofmt_xdate()
    if title:
        plt.title(title)
    plt.grid(grid)
    if show:
        plt.show()

    return fig


def plot_two_line_with_dates_v2(vector_dates, line_1, line_2, title=None, grid=True, y_log_1=False, y_log_2=False, color_line_1='navy', color_line_2='orange',
                                ylabel_1='y1', ylabel_2='y2', y1_percentage=False, y2_percentage=False):

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)

    vector_dates = [datetime.strptime(timestamp2date(int(x)), '%Y-%m-%d %H:%M:%S').date() for x in vector_dates]

    plt.plot(vector_dates, line_1, color=color_line_1)
    plt.gcf().autofmt_xdate()
    if y_log_1:
        ax.set_yscale('log')
    if y1_percentage:
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    bottom, top = plt.ylim()  # return the current ylim
    bottom = bottom * 0.1 if y_log_1 else bottom - (top - bottom) / 2
    plt.ylim((bottom, top))  # set the ylim to bottom, top
    ax.set_ylabel(ylabel_1, color=color_line_1)
    ax.tick_params(axis='y', labelcolor=color_line_1)

    ax2 = ax.twinx()  # instantiate a second axes that shares the same x-axis

    ax2.set_ylabel(ylabel_2, color=color_line_2)
    ax2.plot(vector_dates, line_2, color=color_line_2)
    ax2.tick_params(axis='y', labelcolor=color_line_2)
    if y_log_2:
        ax2.set_yscale('log')
    if y2_percentage:
        ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    bottom, top = plt.ylim()  # return the current ylim
    top = top * 10 if y_log_2 else top * 2
    plt.ylim((bottom, top))  # set the ylim to bottom, top

    if title:
        plt.title(title)
    plt.grid(grid)
    plt.show()

    return fig


def plot_charts(axis, charts, title=None, grid=True, show=True):

    """
    axis = [{'name': 'y' if ax == 'yaxis' else 'y2',
             'title': layout[ax]['title']['text'],
             'color': layout[ax]['color'],
             'percentage': True if 'tickformat' in layout[ax] and layout[ax]['tickformat'] == '%' else False,
             'y_log': True if 'type' in layout[ax] and layout[ax]['type'] == 'log' else False} for ax in layout if ax in ['yaxis', 'yaxis2']]

    charts.append({'type': 'line',
               'line': [x for x in row['y']],
               'color': row['line']['color'],
               'yaxis': row['yaxis'],
               'name': row['name']})
    """

    def set_y_axis(ax, axis_item, shape='none', force=None, legend_position='upper_left', bbox_to_anchor=None):

        ax.set_yscale('log') if 'y_log' in axis_item and axis_item['y_log'] else None
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0)) if 'percentage' in axis_item and axis_item['percentage'] else None
        ax.set_ylabel(axis_item['title'], color=axis_item['color'] if 'color' in axis_item and axis_item['color'] else (0, 0, 0, 0.5)) if 'title' in axis_item and axis_item['title'] else None
        ax.tick_params(axis='y', labelcolor=axis_item['color'] if 'color' in axis_item and axis_item['color'] else (0, 0, 0, 0.5))
        bottom, top = ax.get_ylim()  # return the current ylim
        if 'range' in axis_item and axis_item['range']:
            bottom = axis_item['range'][0]
            top = axis_item['range'][1] if axis_item['range'][1] else top
        ax.set_ylim((force['force_bottom'] if force and 'force_bottom' in force else bottom,
                     force['force_top'] if force and 'force_top' in force else top))  # set the ylim to bottom, top
        if 'tickvals' in axis_item and 'ticklabels' in axis_item and axis_item['tickvals'] and axis_item['ticklabels'] and len(axis_item['ticklabels']) == len(axis_item['tickvals'])\
                and axis_item['ticklabels'] != [''] and axis_item['tickvals'] != ['']:
            ax.set_yticks(axis_item['tickvals'])
            ax.set_yticklabels(axis_item['ticklabels'])

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels, loc=legend_position, bbox_to_anchor=bbox_to_anchor)
        ax.grid(grid)

    def set_x_axis(ax, axis_item):
        ax.set_xscale('log') if 'x_log' in axis_item and axis_item['x_log'] else None
        if 'ticks' in axis_item:
            ax.xaxis.set_ticks(axis_item['ticks'])
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0)) if 'percentage' in axis_item and axis_item['percentage'] else None
        ax.set_xlabel(axis_item['title'], color=axis_item['color'] if 'color' in axis_item else 'navy') if 'title' in axis_item and axis_item['title'] else None
        ax.tick_params(axis='x', labelcolor=axis_item['color'] if 'color' in axis_item else 'navy')
        bottom, top = ax.get_xlim()  # return the current xlim
        if 'range' in axis_item and axis_item['range']:
            bottom = axis_item['range'][0]
            top = axis_item['range'][1]
        ax.set_xlim(bottom, top)  # set the xlim to bottom, top

    default_colors = ['orange', 'indianred', 'lightcyan', 'limegreen', 'lightpink']

    # ___ Initialize Plot and axis ___
    fig = plt.figure(figsize=(16, 10))
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twinx() if any(x for x in axis if x['name'] == 'y2') else None  # instantiate a second axes that shares the same x-axis

    # ___ Plot Lines, Markers and Shapes ___
    # charts = sorted(charts, key=lambda k: k['yaxis'])
    for index, chart in enumerate(charts):
        color = chart['color'] if 'color' in chart and chart['color'] else default_colors[index]
        if 'rgba' in color:
            color = tuple(float(x)/255 if index < 3 else float(x) for index, x in enumerate(color.split('rgba')[1].lstrip('(').rstrip(')').split(',')))
        if color == '#000000':
            color = (0, 0, 0, 0.5)
        if chart['type'] == 'line':
            linestyle = chart['linestyle'] if 'linestyle' in chart and chart['linestyle'] else '-'
            label = chart['label'] if 'label' in chart and chart['label'] else None
            if 'yaxis' not in chart or chart['yaxis'] == 'y':
                ax1.plot(chart['x'], chart['y'], color=color, linestyle=linestyle, label=label)
            else:
                ax2.plot(chart['x'], chart['y'], color=color, linestyle=linestyle, label=label)
            if 'fill' in chart and chart['fill'] == 'tonexty' and index > 0:
                if 'fillcolor' in chart and chart['fillcolor']:
                    fillcolor = tuple(float(x) / 255 if index < 3 else float(x) for index, x in enumerate(chart['fillcolor'].split('rgba')[1].lstrip('(').rstrip(')').split(',')))
                else:
                    fillcolor = color
                if 'yaxis' not in chart or chart['yaxis'] == 'y':
                    ax1.fill_between(chart['x'], chart['y'], charts[index - 1]['y'], facecolor=fillcolor, interpolate=True)
                else:
                    ax2.fill_between(chart['x'], chart['y'], charts[index - 1]['y'], facecolor=fillcolor, interpolate=True)
        elif chart['type'] == 'marker':
            ax1.scatter(chart['x'], chart['y'],  marker='o', facecolors=color, s=chart['size']*10) if chart['yaxis'] == 'y' \
                else ax2.scatter(chart['x'], chart['y'],  marker='o', facecolors=color, s=chart['size']*10)

    # ___ Set x, y1 and y2 axis ___
    for axis_item in axis:
        if axis_item['name'] in ['y1', 'y2']:
            legend_position = 'upper right' if axis_item['name'] == 'y1' else 'upper left'
            if 'legend_position' in axis_item:
                legend_position = axis_item['legend_position']
            bbox_to_anchor = axis_item['bbox_to_anchor'] if 'bbox_to_anchor' in axis_item else None
            set_y_axis(ax1 if axis_item['name'] == 'y1' else ax2, axis_item, shape='bottom' if axis_item['name'] == 'y2' else 'top',
                       force=axis_item['force'] if 'force' in axis_item and axis_item['force'] else None,
                       legend_position=legend_position, bbox_to_anchor=bbox_to_anchor)
        elif axis_item['name'] == 'x':
            set_x_axis(ax1, axis_item)

    # ___ Plot Annotations ___
    for axis_item in axis:
        if axis_item['name'] == 'annotation':
            x_bot, x_top = ax1.get_xlim()
            y_bot, y_top = ax1.get_ylim()
            ax1.text(x_bot + axis_item['x'] * (x_top - x_bot), y_bot + axis_item['y'] * (y_top - y_bot), axis_item['text'], style='italic')

    # ___ Remove ticks and tick labels if axis is not present ___
    if 'y1' not in [x['name'] for x in axis]:
        # ax1.set_yticklabels(['']*len([item.get_text() for item in ax1.get_yticklabels()]))
        ax1.tick_params(axis='y', which='both', left=False, right=False, labelleft=False)
    if ax2 and 'y2' not in [x['name'] for x in axis]:
        ax2.tick_params(axis='y', which='both', left=False, right=False, labelright=False)

    # ___ Plot Title and show ___
    plt.title(title if title else 'No title')
    plt.show() if show else None

    return fig


def plot_charts_with_dates(axis, charts, title=None, grid=True, show=True):

    """
    axis = [{'name': 'y' if ax == 'yaxis' else 'y2',
             'title': layout[ax]['title']['text'],
             'color': layout[ax]['color'],
             'percentage': True if 'tickformat' in layout[ax] and layout[ax]['tickformat'] == '%' else False,
             'y_log': True if 'type' in layout[ax] and layout[ax]['type'] == 'log' else False} for ax in layout if ax in ['yaxis', 'yaxis2']]

    charts.append({'type': 'line',
               'line': [x for x in row['y']],
               'color': row['line']['color'],
               'yaxis': row['yaxis'],
               'name': row['name']})
    """

    def set_y_axis(ax, axis_item, shape='none', force=None, legend_position='upper_left'):

        color = axis_item['color'] if 'color' in axis_item and axis_item['color'] else (0, 0, 0, 0.5)
        if len(color) == 4 and color[0] == '#':
            color = '#' + ''.join([x * 2 for x in color[1:]])

        ax.set_yscale('log') if 'y_log' in axis_item and axis_item['y_log'] else None
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0)) if 'percentage' in axis_item and axis_item['percentage'] else None
        ax.set_ylabel(axis_item['title'], color=color) if 'title' in axis_item and axis_item['title'] else None
        ax.tick_params(axis='y', labelcolor=color)
        bottom, top = ax.get_ylim()  # return the current ylim
        if 'range' in axis_item and axis_item['range']:
            bottom = axis_item['range'][0]
            top = axis_item['range'][1]
        ax.set_ylim((force['force_bottom'] if force and 'force_bottom' in force else bottom,
                     force['force_top'] if force and 'force_top' in force else top))  # set the ylim to bottom, top
        if 'tickvals' in axis_item and 'ticklabels' in axis_item and axis_item['tickvals'] and axis_item['ticklabels'] and len(axis_item['ticklabels']) == len(axis_item['tickvals'])\
                and axis_item['ticklabels'] != [''] and axis_item['tickvals'] != ['']:
            ax.set_yticks(axis_item['tickvals'])
            ax.set_yticklabels(axis_item['ticklabels'])

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels, loc=legend_position)
        ax.grid(grid)

    def set_x_axis(ax, axis_item):

        color = axis_item['color'] if 'color' in axis_item and axis_item['color'] else (0,0,0,0.5)
        if len(color) == 4 and color[0] == '#':
            color = '#' + ''.join([x * 2 for x in color[1:]])

        ax.set_xscale('log') if 'x_log' in axis_item and axis_item['x_log'] else None
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0)) if 'percentage' in axis_item and axis_item['percentage'] else None
        ax.set_xlabel(axis_item['title'], color=color) if 'title' in axis_item and axis_item['title'] else None
        ax.tick_params(axis='x', labelcolor=color)
        bottom, top = ax.get_xlim()  # return the current xlim
        if 'range' in axis_item and axis_item['range']:
            bottom = datetime.strptime(timestamp2date(int(axis_item['range'][0])), '%Y-%m-%d %H:%M:%S').toordinal()
            top = datetime.strptime(timestamp2date(int(axis_item['range'][1])), '%Y-%m-%d %H:%M:%S').toordinal()
        ax.set_xlim(bottom, top)  # set the xlim to bottom, top

    default_colors = ['orange', 'indianred', 'lightcyan', 'limegreen', 'lightpink']

    # ___ Initialize Plot and axis ___
    fig = plt.figure(figsize=(16, 10))
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twinx() if any(x for x in axis if x['name'] == 'y2') else None  # instantiate a second axes that shares the same x-axis

    # ___ Plot Lines, Markers and Shapes ___
    # charts = sorted(charts, key=lambda k: k['yaxis'])
    for index, chart in enumerate(charts):
        color = chart['color'] if 'color' in chart and chart['color'] else default_colors[min(index, len(default_colors)-1)]
        if 'rgba' in color:
            color = tuple(float(x)/255 if index < 3 else float(x) for index, x in enumerate(color.split('rgba')[1].lstrip('(').rstrip(')').split(',')))
        elif len(color) == 4 and color[0] == '#':
            color = '#' + ''.join([x*2 for x in color[1:]])
            if 'opacity' in chart and chart['opacity']:
                color = tuple([x/255 for x in rgb(color[1:])]) + (float(chart['opacity']),)
        if color == '#000000':
            color = (0, 0, 0, 0.5)
        if chart['type'] == 'line':
            vector_dates = [datetime.strptime(timestamp2date(int(x)), '%Y-%m-%d %H:%M:%S') for x in chart['x']]
            linestyle = chart['linestyle'] if 'linestyle' in chart and chart['linestyle'] else '-'
            label = chart['label'] if 'label' in chart and chart['label'] else None
            if 'yaxis' not in chart or chart['yaxis'] in ['y', 'y1']:
                ax1.plot(vector_dates, chart['y'], color=color, linestyle=linestyle, label=label)
            else:
                ax2.plot(vector_dates, chart['y'], color=color, linestyle=linestyle, label=label)
            if 'fill' in chart and chart['fill'] == 'tonexty' and index > 0:
                if 'fillcolor' in chart and chart['fillcolor']:
                    fillcolor = tuple(float(x) / 255 if index < 3 else float(x) for index, x in enumerate(chart['fillcolor'].split('rgba')[1].lstrip('(').rstrip(')').split(',')))
                else:
                    fillcolor = color
                if chart['yaxis'] == 'y':
                    ax1.fill_between(vector_dates, chart['y'], charts[index - 1]['y'], facecolor=fillcolor, interpolate=True)
                else:
                    ax2.fill_between(vector_dates, chart['y'], charts[index - 1]['y'], facecolor=fillcolor, interpolate=True)
        elif chart['type'] == 'marker':
            vector_dates = [datetime.strptime(timestamp2date(int(x)), '%Y-%m-%d %H:%M:%S') for x in chart['x']]
            ax1.scatter(vector_dates, chart['y'],  marker='o', facecolors=color, s=chart['size']*10) if chart['yaxis'] == 'y' \
                else ax2.scatter(vector_dates, chart['y'],  marker='o', facecolors=color, s=chart['size']*10)
        elif chart['type'] == 'rect':
            fillcolor = chart['fillcolor'] if 'fillcolor' in chart else default_colors[min(index, len(default_colors)-1)]
            if fillcolor and 'rgba' in fillcolor:
                fillcolor = tuple(float(x) / 255 if index < 3 else float(x) for index, x in enumerate(fillcolor.split('rgba')[1].lstrip('(').rstrip(')').split(',')))
            elif fillcolor and len(fillcolor) == 4 and fillcolor[0] == '#':
                fillcolor = '#' + ''.join([x * 2 for x in fillcolor[1:]])
                if 'opacity' in chart and chart['opacity']:
                    fillcolor = tuple([x / 255 for x in rgb(fillcolor[1:])]) + (float(chart['opacity']),)
            x0 = datetime.strptime(timestamp2date(int(chart['x0'])), '%Y-%m-%d %H:%M:%S')
            x1 = datetime.strptime(timestamp2date(int(chart['x1'])), '%Y-%m-%d %H:%M:%S')
            ax1.add_patch(
                patches.Rectangle(
                    (x0, chart['y0']),  # (x, y)
                    x1 - x0,  # width
                    chart['y1'] - chart['y0'],  # height
                    fill=True,
                    facecolor=fillcolor,
                    edgecolor=color,
                    zorder=2
                )
            )

    # ___ Set x, y1 and y2 axis ___
    for axis_item in axis:
        if axis_item['name'] in ['y1', 'y2']:
            set_y_axis(ax1 if axis_item['name'] == 'y1' else ax2, axis_item, shape='bottom' if axis_item['name'] == 'y2' else 'top',
                       force=axis_item['force'] if 'force' in axis_item and axis_item['force'] else None,
                       legend_position='upper right' if axis_item['name'] == 'y1' else 'upper left')
        elif axis_item['name'] == 'x':
            set_x_axis(ax1, axis_item)

    # ___ Plot Annotations ___
    for axis_item in axis:
        if axis_item['name'] == 'annotation':
            x_bot, x_top = ax1.get_xlim()
            y_bot, y_top = ax1.get_ylim()
            ax1.text(x_bot + axis_item['x'] * (x_top - x_bot), y_bot + axis_item['y'] * (y_top - y_bot), axis_item['text'], style='italic')

    # ___ Remove ticks and tick labels if axis is not present ___
    if 'y1' not in [x['name'] for x in axis]:
        # ax1.set_yticklabels(['']*len([item.get_text() for item in ax1.get_yticklabels()]))
        ax1.tick_params(axis='y', which='both', left=False, right=False, labelleft=False)
    if ax2 and 'y2' not in [x['name'] for x in axis]:
        ax2.tick_params(axis='y', which='both', left=False, right=False, labelright=False)

    # ___ Plot Title and show ___
    plt.title(title if title else 'No title')
    plt.gcf().autofmt_xdate()
    plt.show() if show else None

    return fig


def plot_image_from_url(url, show=True):

    fig = plt.figure(figsize=(16, 10))

    a = plt.imread(url)
    img = plt.imshow(a)
    img.set_cmap('hot')
    plt.axis('off')
    plt.show() if show else None

    return fig


def bar_plot_line_with_dates(data, auto_format=True, label=None):

    vector = [x[1] for x in data]
    # dates = [datetime.strptime(timestamp2date(int(x[0])), '%Y-%m-%d %H:%M:%S').date() for x in data]
    dates = [datetime.fromtimestamp(int(x[0])) for x in data]

    fig, ax = plt.subplots()
    ax.bar(dates, vector, width=10)

    locator = mdates.MonthLocator()
    fmt = mdates.DateFormatter('%b-%Y')
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(fmt)

    if auto_format:
        plt.gcf().autofmt_xdate()

    if label:
        fmt = label + '%.2f'
        formatter = ticker.FormatStrFormatter(fmt)
        ax.yaxis.set_major_formatter(formatter)
    plt.grid(True)
    plt.show()


def bar_plot_groups_with_dates(data, names, auto_format=True, label=None, show=True):

    dates_x = [get_compact_month_year_from_timestamp(int(x[0])) for x in data]

    fig, ax = plt.subplots()

    bar_width = 10
    r1 = np.arange(len([x[1] for x in data])) * 40
    r1 = r1 - bar_width/2
    r2 = r1 + bar_width

    p1 = ax.bar(r1, [x[1] for x in data], width=bar_width)
    p2 = ax.bar(r2, [x[2] for x in data], width=bar_width)

    ax.legend((p1[0], p2[0]), (names[0], names[1]))

    plt.xticks(r1 + bar_width/2, dates_x)

    if label:
        fmt = label + '%.2f'
        formatter = ticker.FormatStrFormatter(fmt)
        ax.yaxis.set_major_formatter(formatter)

    if auto_format:
        plt.gcf().autofmt_xdate()

    plt.grid(True)
    if show:
        plt.show()

    return fig, ax


def plot_pie_from_data_frame(df, column, title='Title', plot_table=False, table_df=None):

    plt.figure(figsize=(16, 8))
    ax = plt.subplot(121, aspect='equal')
    df.plot.pie(ax=ax, y=column, autopct='%1.1f%%', pctdistance=0.85, rotatelabels=True, textprops={'fontsize': 8}, labeldistance=1.05, startangle=90, counterclock=False,
                colors=['#EA5C2B', '#F6D860', '#95CD41', '#548CFF', '#93FFD8', '#CFFFDC', '#FF1700', '#676FA3', '#557C55', '#FFBD35', '#3FA796', '#0F0E0E', '#D77FA1', '#7900FF'])

    # ax = expenses.plot.pie(y='Netto', autopct='%1.1f%%', pctdistance=0.85, rotatelabels=True, textprops={'fontsize': 8}, labeldistance=1.05 , colors=['#EA5C2B', '#F6D860', '#95CD41', '#548CFF', '#93FFD8', '#CFFFDC', '#FF1700', '#676FA3', '#557C55', '#FFBD35', '#3FA796', '#0F0E0E', '#D77FA1', '#7900FF'])
    x_axis = ax.axes.get_xaxis()
    x_label = x_axis.get_label()
    x_label.set_visible(False)
    ax.get_yaxis().set_visible(False)
    ax.grid()
    ax.legend(fontsize='7', loc='upper left', bbox_to_anchor=(-0.25, 0.5, 0.5, 0.5))
    fmt = '€' + '%.2f'
    formatter = ticker.FormatStrFormatter(fmt)
    ax.yaxis.set_major_formatter(formatter)
    fig = ax.get_figure()
    fig.suptitle(title, fontsize=15)

    if not plot_table:
        return fig

    # # __ plot table __
    # table_df['Spese'] = table_df['Netto'].apply(lambda s: '   ' + str(round(s, 2)) + ' €   ')
    # table_df['%'] = table_df['Netto'].apply(lambda x: '   ' + str(round(float(x) / tot * 100, 1)) + ' %  ')
    # expenses = expenses.drop(columns=['Netto'])
    #
    # saves = True
    # if row['Netto'] > 0:
    #     expenses.loc['Risparmi'] = ['-', '   ' + str(round(-row['Netto'], 2)) + ' €   ']
    #     saves = False
    #
    # ax2 = plt.subplot(122)
    # plt.axis('off')
    # tbl = ax2.table(cellText=expenses.values, colLabels=expenses.columns, rowLabels=expenses.index, loc='center')
    # tbl.set_fontsize(12)
    # tbl.auto_set_column_width([0, 1])
    # tbl.scale(1, 2)
    #
    # for k, cell in six.iteritems(tbl._cells):
    #     cell.set_edgecolor('w')
    #     if k[0] == 0 or k[1] < 0:
    #         cell.set_text_props(weight='bold', color='w')
    #         if cell.get_text()._text == 'Risparmi':
    #             cell.set_facecolor('#A3DA8D') if saves else cell.set_facecolor('#FF5959')
    #         else:
    #             cell.set_facecolor('#40466e')
    #     else:
    #         cell.set_facecolor(['#f1f1f2', 'w'][k[0] % len(['#f1f1f2', 'w'])])
    #         cell.set_text_props(style='italic')
    #
    # return fig


# _____ Candles _____
def plot_candles(candles, o=3, h=2, lw=1, c=4, v=5, show=True, new=True, fig=None, log=False):

    ohlc_data = []

    for line in candles:
        # ohlc_data.append((dates.datestr2num(timestamp2date(int(line[0]))), line[o], line[h], line[lw], line[c], line[v]))
        ohlc_data.append((dates.datestr2num(timestamp2date(int(line['timestamp']))), line['open'], line['high'], line['low'], line['close'], line['volume']))

    if new:
        fig = plt.figure(figsize=(16, 10))

    ax = fig.add_subplot(111)

    factor = 50 / ((len(candles) // 100) * 100)

    candlestick_ohlc(ax, ohlc_data, width=factor, colorup='g', colordown='r', alpha=0.8)

    ax.xaxis.set_major_formatter(dates.DateFormatter('%d/%m/%Y %H:%M'))
    ax.xaxis.set_major_locator(ticker.MaxNLocator(10))
    if log:
        ax.set_yscale('log')
    # ax.grid(True)

    plt.xticks(rotation=30)
    plt.grid()
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('BTC-USD')
    plt.tight_layout()

    if show:
        plt.show()

    return fig, plt


#   ___ DataFrame MatplotLib Plot ___

def scatter_plot_2d(df_, fig=None, ax=None, show=False, grid=True, title=None, alpha=0.5, labels_=None, centroids_=None, **kwargs):
    df_columns = df_.columns
    if len(df_columns) < 2:
        print("Not enough columns in DataFrame")
        return None

    if ax is None:
        fig, ax = new_fig()

    # ___ scatter plot ___
    if labels_ is not None:
        ax.scatter(df_[df_columns[0]], df_[df_columns[1]], c=labels_, cmap="Paired", alpha=alpha)
    else:
        ax.scatter(df_[df_columns[0]], df_[df_columns[1]], alpha=alpha)

    # ___ scatter centroids ___
    if centroids_ is not None:
        centroids_columns = centroids_.columns
        ax.scatter(centroids_[centroids_columns[0]], centroids_[centroids_columns[1]], s=150, c="#355C7D", alpha=0.7)

    ax.grid() if grid else None
    ax.set_title(title) if title else None
    ax.set_xlabel(df_columns[0])
    ax.set_ylabel(df_columns[1])
    fig.show() if show and fig else None

    return fig


def scatter_plot_dataframe(df):
    from itertools import combinations
    from math import sqrt, ceil

    df_columns = df.columns
    if len(df_columns) < 2:
        print("Not enough columns in DataFrame")
        return None

    charts_dim = ceil(sqrt(len(df_columns) - 1))

    tuples = list(combinations(df_columns, 2))

    for column in df_columns:
        filtered_tuples = [(column, x[1] if x[0] == column else x[0]) for x in tuples if x[0] == column or x[1] == column]
        fig, axes_ = new_fig(fig_size=(16, 16), nrows=charts_dim, ncols=charts_dim)
        for index, tuple_ in enumerate(filtered_tuples):
            i_row = index // charts_dim
            i_col = index % charts_dim
            scatter_plot_2d(df[[tuple_[0], tuple_[1]]], ax=axes_[i_row][i_col])
        fig.suptitle(column, fontsize=24)
        fig.show()

    print('end')


def dendrogram_plot(df, link='ward'):
    # linkage available: 'single', 'average', 'complete', 'ward'
    z = linkage(df.to_numpy(), link)

    plt.figure(figsize=(25, 10))
    plt.title('Hierarchical Agglomerative Clustering Dendrogram')
    plt.xlabel('sample index')
    plt.ylabel('distance')
    dendrogram(z,
               leaf_rotation=90.,  # rotates the x axis labels
               leaf_font_size=8,  # font size for the x axis labels
               )
    plt.show()


def truncated_dendrogram_plot(df, link='ward'):
    # linkage available: 'single', 'average', 'complete', 'ward'
    z = linkage(df.to_numpy(), link)

    plt.figure(figsize=(25, 10))
    plt.title('HAC Dendrogram (truncated to the last 20 merged clusters)')
    plt.xlabel('sample index')
    plt.ylabel('distance')
    dendrogram(z,
               truncate_mode='lastp',  # show only the last p merged clusters
               p=20,  # show only the last p merged clusters
               show_leaf_counts=False,  # otherwise numbers in brackets are counts
               leaf_rotation=90.,
               leaf_font_size=12,
               show_contracted=True,  # to get a distribution impression in truncated branches
               )
    plt.show()


def parallel_coordinates_plot(df, label, max_clusters=10, show=False, opacity=1):
    if label not in df.columns:
        return None

    cluster_values = list(set(df[label].tolist()))
    num_clusters = len(cluster_values)

    if len(cluster_values) > max_clusters:
        print('Too many clusters')
        return None

    # figure_k, axes_list_k = new_fig(nrows=len(cluster_values), ncols=1)
    #
    # for index, cluster in enumerate(cluster_values):
    #     list_of_colors = [[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 0.6, 0.5], [0.4, 0.4, 0.5], [0.4, 0.5, 0.6], [0.6, 0.5, 0.4], [0.5, 0.6, 0.4], [0.5, 0.5, 0.5], [0.1, 0.6, 0.4]]
    #     colors = [list_of_colors[y] + [1.0 if y == index else 0.1] for y in range(num_clusters)]
    #     pd.plotting.parallel_coordinates(df, label, color=colors, ax=axes_list_k[index])
    #     # plt.show()

    figure_k = plt.figure(figsize=(9, 6))
    plt.xticks(rotation=45)
    list_of_colors = [[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 0.6, 0.5], [0.4, 0.4, 0.5], [0.4, 0.5, 0.6], [0.6, 0.5, 0.4], [0.5, 0.6, 0.4], [0.5, 0.5, 0.5], [0.1, 0.6, 0.4]]
    colors = [list_of_colors[y] + [opacity] for y in range(num_clusters)]
    pd.plotting.parallel_coordinates(df, label, color=colors)
    # plt.show()

    figure_k.show() if show else None
    return figure_k


def parallel_coordinates_plot_split(df, label, max_clusters=10, show=False):
    if label not in df.columns:
        return None

    cluster_values = list(set(df[label].tolist()))
    num_clusters = len(cluster_values)

    if len(cluster_values) > max_clusters:
        print('Too many clusters')
        return None

    figure_k, axes_list_k = new_fig(nrows=len(cluster_values), ncols=1)

    for index, cluster in enumerate(cluster_values):
        list_of_colors = [[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 0.6, 0.5], [0.4, 0.4, 0.5], [0.4, 0.5, 0.6], [0.6, 0.5, 0.4], [0.5, 0.6, 0.4], [0.5, 0.5, 0.5], [0.1, 0.6, 0.4]]
        colors = [list_of_colors[y] + [1.0 if y == index else 0.01] for y in range(num_clusters)]
        pd.plotting.parallel_coordinates(df, label, color=colors, ax=axes_list_k[index])
        # plt.show()

    figure_k.show() if show else None
    return figure_k
