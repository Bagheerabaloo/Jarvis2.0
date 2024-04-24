import matplotlib.pyplot as plt
import seaborn as sns


# ___ Seaborn Plot ___

class Seaborn:

    @staticmethod
    def set_theme(style=None, font_scale=1.25, palette=None):
        sns.set_theme() if style is None else sns.set_theme(style=style, font_scale=font_scale, palette=palette)

    @staticmethod
    def show():
        plt.show()

    @staticmethod
    def close():
        plt.close()

    def __init__(self, df, fig_size=(16, 9)):
        self.df = df
        self.plot = None
        sns.set(rc={'figure.figsize': fig_size})

    def line_plot(self, x, y, hue=None, style=None, size=None, show=True, **kwargs):
        self.plot = sns.lineplot(data=self.df, x=x, y=y, hue=hue, style=style, size=size, **kwargs)

    def relationship_plot(self, x, y, col=None, hue=None, style=None, size=None, show=True, **kwargs):
        self.plot = sns.relplot(data=self.df, x=x, y=y, col=col, hue=hue, style=style, size=size, **kwargs)

    def linear_regression_plot(self, x, y, col=None, hue=None, size=None, **kwargs):
        self.plot = sns.lmplot(data=self.df, x=x, y=y, col=col, hue=hue, size=size, **kwargs)

    def distribution_plot(self, x, y=None, col=None, hue=None, **kwargs):
        self.plot = sns.displot(data=self.df, x=x, y=y, col=col, hue=hue, **kwargs)

    def categorical_plot(self, x, y=None, col=None, hue=None, **kwargs):
        self.plot = sns.catplot(data=self.df, x=x, y=y, col=col, hue=hue, **kwargs)

    def joint_distribution_plot(self, x, y=None, hue=None, **kwargs):
        self.plot = sns.jointplot(data=self.df, x=x, y=y, hue=hue, **kwargs)

    def pair_plot(self, hue=None, **kwargs):
        self.plot = sns.pairplot(data=self.df, hue=hue, **kwargs)

    # ___ Plot Tuning ___

    def set_axis_labels(self, xlabel, ylabel, labelpad=10, **kwargs):
        self.plot.set_axis_labels(xlabel, ylabel, labelpad=labelpad, **kwargs) if self.plot else None

    def set_legend(self, title=None, **kwargs):
        self.plot.legend()
        self.set_legend_title(title=title) if title else None

    def set_legend_title(self, title, **kwargs):
        self.plot.legend.set_title(title, **kwargs) if self.plot else None

    def set_size_inches(self, w, h, **kwargs):
        self.plot.figure.set_size_inches(w, h, **kwargs) if self.plot else None

    def set_axis_margins(self, unit, **kwargs):
        self.plot.ax.margins(unit, **kwargs) if self.plot else None

    def despine(self, trim=True):
        self.plot.despine(trim=trim) if self.plot else None

    # ___ Save Figure ___

    def save(self, folder, name):
        self.plot.get_figure().savefig(f'{folder}/{name}.png') if self.plot else None


def seaborn_set_theme(theme_style=None, font_scale=1.25):
    sns.set_theme() if theme_style is None else sns.set_theme(style=theme_style, font_scale=font_scale)


def seaborn_relationship_plot(df, x, y, col=None, hue=None, style=None, size=None, show=True, **kwargs):
    g = sns.relplot(data=df, x=x, y=y, col=col, hue=hue, style=style, size=size, **kwargs)
    plt.show() if show else None

    return g


def seaborn_linear_regression_plot(df, x, y, col=None, hue=None, size=None, **kwargs):
    # Apply the default theme
    sns.set_theme()

    # Create a visualization
    sns.lmplot(data=df, x=x, y=y, col=col, hue=hue, size=size, **kwargs)
    plt.show()


def seaborn_distribution_plot(df, x, y=None, col=None, hue=None, **kwargs):
    # Apply the default theme
    sns.set_theme()

    # Create a visualization
    sns.displot(data=df, x=x, y=y, col=col, hue=hue, **kwargs)
    plt.show()


def seaborn_categorical_plot(df, x, y=None, col=None, hue=None, **kwargs):
    # Apply the default theme
    sns.set_theme()

    # Create a visualization
    sns.catplot(data=df, x=x, y=y, col=col, hue=hue, **kwargs)
    plt.show()


def seaborn_joint_distribution_plot(df, x, y=None, hue=None, **kwargs):
    # Apply the default theme
    sns.set_theme()

    # Create a visualization
    sns.jointplot(data=df, x=x, y=y, hue=hue, **kwargs)
    plt.show()


def seaborn_pair_plot(df, hue=None, **kwargs):
    seaborn_set_theme(**kwargs)

    g = sns.pairplot(data=df, hue=hue, **kwargs)
    plt.show()

    return g