import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from sklearn.cluster import KMeans, estimate_bandwidth, MeanShift
from sklearn.cluster import DBSCAN
from sklearn.cluster import AgglomerativeClustering

from .plot_matplotlib import new_fig, scatter_plot_2d, plot_line, parallel_coordinates_plot
from .plot_seaborn import *
from sklearn.preprocessing import StandardScaler, Normalizer, MinMaxScaler
from sklearn.neighbors import NearestNeighbors
from src.utils_dataframe import *
from src.utils import *
from scipy.interpolate import barycentric_interpolate

import matplotlib.cm as cm
from sklearn.metrics import silhouette_samples, silhouette_score

# create logger
logger = init_logger(name='Silhouette')

EMPTY_LIST = []


# __ Standard Scaler __
def standard_scaler(df, columns=None):
    # __ Split the dataFrame in cols to bypass and cols to scale __
    cols_to_scale = df.columns if columns is None else [x for x in columns if x in df.columns]
    cols_to_bypass = [x for x in df.columns if x not in cols_to_scale]
    df_to_bypass = df[cols_to_bypass]
    df_to_scale = df[cols_to_scale]

    # __ Standardize the data __
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df_to_scale)  # Scale only cols to scale
    df_scaled_features = pd.DataFrame(scaled_features, index=df_to_scale.index, columns=cols_to_scale)

    # __ Concatenate scaled data with data bypassed __
    final_df = pd.concat([df_to_bypass, df_scaled_features], axis=1)

    return move_columns_to_first_position(final_df, list(df.columns))


# __ Normalizer __
def normalize(df, columns=None):
    # __ Split the dataFrame in cols to bypass and cols to scale __
    cols_to_scale = df.columns if columns is None else [x for x in columns if x in df.columns]
    cols_to_bypass = [x for x in df.columns if x not in cols_to_scale]
    df_to_bypass = df[cols_to_bypass]
    df_to_scale = df[cols_to_scale]

    # __ Standardize the data __
    normalizer = Normalizer()
    normalized_features = normalizer.fit_transform(df_to_scale)  # Scale only cols to scale
    df_normalized_features = pd.DataFrame(normalized_features, index=df_to_scale.index, columns=cols_to_scale)

    # __ Concatenate scaled data with data bypassed __
    final_df = pd.concat([df_to_bypass, df_normalized_features], axis=1)

    return move_columns_to_first_position(final_df, list(df.columns))


# __ MinMax Scaler __
def min_max_scaler(df, columns=None, feature_range=None):
    if feature_range is None:
        feature_range = (0, 1)

    # __ Split the dataFrame in cols to bypass and cols to scale __
    cols_to_scale = df.columns if columns is None else [x for x in columns if x in df.columns]
    cols_to_bypass = [x for x in df.columns if x not in cols_to_scale]
    df_to_bypass = df[cols_to_bypass]
    df_to_scale = df[cols_to_scale]

    # __ Standardize the data __
    scaler = MinMaxScaler(feature_range=feature_range)
    scaled_features = scaler.fit_transform(df_to_scale)  # Scale only cols to scale
    df_scaled_features = pd.DataFrame(scaled_features, index=df_to_scale.index, columns=cols_to_scale)

    # __ Concatenate scaled data with data bypassed __
    final_df = pd.concat([df_to_bypass, df_scaled_features], axis=1)

    return move_columns_to_first_position(final_df, list(df.columns))


# ___ Cluster Analysis ___
def cluster_analysis(df_, columns, folder_path, index_columns, plot=True):
    pass


def plot_silhouette_score(df, columns=None):
    if columns is None:
        columns = df.columns

    # The silhouette_score gives the average value for all the samples. This gives a perspective into the density and separation of the formed clusters
    silhouette_coefficient = silhouette_score(df[columns].drop(columns='label', errors='ignore').to_numpy(),
                                              df['label'])

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(df[columns].drop(columns='label', errors='ignore').to_numpy(),
                                                  df['label'].to_numpy())

    # Create a subplot with 1 row and 1 column
    fig, ax1 = plt.subplots(1, 1)
    fig.set_size_inches(18, 7)

    ax1.set_xlim([min(-0.1, min(sample_silhouette_values) - 0.1), 1])  # The silhouette coefficient can range from -1, 1

    y_lower = 10
    for label in df['label'].unique():
        # Aggregate the silhouette scores for samples belonging to cluster i, and sort them
        ith_cluster_silhouette_values = sample_silhouette_values[df['label'].to_numpy() == label]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.nipy_spectral(float(label) / len(df['label'].unique()))
        ax1.fill_betweenx(np.arange(y_lower, y_upper),
                          0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(label))

        # Compute the new y_lower for next plot
        y_lower = y_upper + 10  # 10 for the 0 samples

    ax1.set_title("The silhouette plot for the various clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_coefficient, color="red", linestyle="--")

    return fig


def silhouette_analysis(df, max_k=6, use_ml=True, deterministic_algorithm=None):
    import matplotlib.cm as cm
    from sklearn.metrics import silhouette_samples, silhouette_score
    from sklearn.cluster import KMeans

    range_n_clusters = range(2, max_k)
    silhouette_coefficients = []
    figures = []

    logger.info(f'Dataset entries {df.shape}')

    for n_clusters in range_n_clusters:

        # Create a subplot with 1 row and 1 column
        fig, ax1 = plt.subplots(1, 1)
        fig.set_size_inches(18, 7)

        # The 1st subplot is the silhouette plot
        # The silhouette coefficient can range from -1, 1 but in this example all
        # lie within [-0.1, 1]
        ax1.set_xlim([-0.1, 1])
        # The (n_clusters+1)*10 is for inserting blank space between silhouette
        # plots of individual clusters, to demarcate them clearly.
        ax1.set_ylim([0, len(df.to_numpy()) + (n_clusters + 1) * 15])

        # Initialize the clusterer with n_clusters value and a random generator
        # seed of 10 for reproducibility.
        if use_ml:
            k_means_model = KMeans(n_clusters=n_clusters, random_state=5433)
            cluster_labels = k_means_model.fit_predict(df.to_numpy())
        else:
            cluster_labels = df.apply(lambda row: deterministic_algorithm(row, n_clusters), axis=1)

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        silhouette_coefficient = silhouette_score(df.to_numpy(), cluster_labels)
        silhouette_coefficients.append(silhouette_coefficient)
        print("For number of clusters =", n_clusters,
              "The average silhouette_score is :", silhouette_coefficient)

        # Compute the silhouette scores for each sample
        sample_silhouette_values = silhouette_samples(df.to_numpy(), cluster_labels)

        y_lower = 10
        for i in range(n_clusters):
            # Aggregate the silhouette scores for samples belonging to
            # cluster i, and sort them
            ith_cluster_silhouette_values = \
                sample_silhouette_values[cluster_labels == i]

            ith_cluster_silhouette_values.sort()

            size_cluster_i = ith_cluster_silhouette_values.shape[0]
            y_upper = y_lower + size_cluster_i

            color = cm.nipy_spectral(float(i) / n_clusters)
            ax1.fill_betweenx(np.arange(y_lower, y_upper),
                              0, ith_cluster_silhouette_values,
                              facecolor=color, edgecolor=color, alpha=0.7)

            # Label the silhouette plots with their cluster numbers at the middle
            ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

            # Compute the new y_lower for next plot
            y_lower = y_upper + 10  # 10 for the 0 samples

        ax1.set_title("The silhouette plot for the various clusters.")
        ax1.set_xlabel("The silhouette coefficient values")
        ax1.set_ylabel("Cluster label")

        # The vertical line for average silhouette score of all the values
        ax1.axvline(x=silhouette_coefficient, color="red", linestyle="--")

        figures.append(fig)

    return figures
    #
    #     ax1.set_yticks([])  # Clear the yaxis labels / ticks
    #     ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])
    #
    #     # 2nd Plot showing the actual clusters formed
    #     colors = cm.nipy_spectral(cluster_labels.astype(float) / n_clusters)
    #     ax2.scatter(df.to_numpy()[:, 0], df.to_numpy()[:, 1], marker='.', s=30, lw=0, alpha=0.4,
    #                 c=colors, edgecolor='k')
    #
    #     # Labeling the clusters
    #     centers = k_means_model.cluster_centers_
    #     # Draw white circles at cluster centers
    #     ax2.scatter(centers[:, 0], centers[:, 1], marker='o',
    #                 c="white", alpha=1, s=200, edgecolor='k')
    #
    #     for i, c in enumerate(centers):
    #         ax2.scatter(c[0], c[1], marker='$%d$' % i, alpha=1, s=50, edgecolor='k')
    #
    #     ax2.set_title("The visualization of the clustered data.")
    #     ax2.set_xlabel("Feature space for the 1st feature")
    #     ax2.set_ylabel("Feature space for the 2nd feature")
    #
    #     plt.suptitle(("Silhouette analysis for KMeans clustering on sample data "
    #                   "with n_clusters = %d" % n_clusters),
    #                  fontsize=14, fontweight='bold')
    #
    # plot_line(x=range_n_clusters, data=silhouette_coefficients, show=False, xlabel="Number of Clusters", ylabel="Silhouette Coefficient")
    #
    # plt.show()


# _____ K-Means _____
def k_means(df: pd.DataFrame, n_clusters: int = 3, columns=None, index_columns=None, random_state=1234):
    """Receives as input a dataFrame, performs a K-MEANS analysis on relevant columns and returns a dataFrame as output.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    If index_columns is specified then such columns are preserved in the output dataFrame"""

    if index_columns is None:
        index_columns = EMPTY_LIST
    if columns is None:
        columns = EMPTY_LIST

    k_means_ = KMeans(n_clusters=n_clusters, random_state=random_state)

    df_for_cluster = df[columns] if len(columns) > 0 else df
    df_k_means = df[
        list(set([x for x in columns if x in df.columns] + [x for x in index_columns if x in df.columns]))] if len(
        columns) > 0 else df
    df_k_means['label'] = k_means_.fit_predict(df_for_cluster.to_numpy())

    centroids_k = k_means_.cluster_centers_
    # centroids_k_df = pd.DataFrame(centroids_k, columns=['Column_A', 'Column_B'])
    centroids_k_df = pd.DataFrame(centroids_k, columns=columns if len(columns) > 0 else df.columns)
    centroids_k_df['label'] = centroids_k_df.index

    return df_k_means, centroids_k_df


def k_means_model(df: pd.DataFrame, n_clusters: int = 3, columns=None, random_state=1234):
    """Receives as input a dataFrame, performs a K-MEANS analysis on relevant columns and returns the K-MEANS model.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    """

    if columns is None:
        columns = EMPTY_LIST

    df_for_cluster = df[columns] if len(columns) > 0 else df
    return KMeans(n_clusters=n_clusters, random_state=random_state).fit(df_for_cluster.to_numpy())


def elbow_analysis(df, min_k=2, max_k=10, random_state=1234, columns=None, plot=False):
    k_values = range(min_k, max_k)

    df_to_use = df if columns is None else df[columns]

    # Create a k-means model for each k number of clusters
    k_means_models = [KMeans(n_clusters=k, random_state=random_state).fit(df_to_use.to_numpy()) for k in k_values]

    # Get the total within-cluster sum of square for each k-means
    wcss = [model.inertia_ for model in k_means_models]

    # ___ Compute derivative of elbow vector
    # wcss_perc = np.diff(wcss) / wcss[:-1]
    # derivative = np.diff(wcss_perc)
    # wss = [0.312, 0.236, 0.208, 0.191, 0.179, 0.169, 0.159, 0.152]
    # wcss_p = np.diff(wcss) / wcss[:-1]
    # d = np.diff(wcss_p)
    # plt.plot(range(2, 10), wss)
    # plt.plot(range(3, 10), [-n for n in wcss_p])
    # plt.plot(range(4, 10), d)
    # plt.grid()
    # plt.show()

    # ___ Plot the elbow curve ___
    fig = None
    if plot:
        # x = np.linspace(min(k_values), max(k_values), num=len(k_values)*8)
        # y = barycentric_interpolate(k_values, wcss, x)
        # d = np.diff(y)
        # range(0, len(x), int(len(x) / len(k_values)) + 1)

        fig = plt.figure(figsize=(6, 6))
        plt.xticks(k_values)
        plt.plot(k_values, wcss, 'bo-', color='red', label='WCSS')
        # plt.plot(x, y)
        plt.grid(True)
        plt.xlabel('Number of clusters')
        plt.legend()
        plt.title('Elbow method for K-Means')

    return wcss, fig


def k_means_analysis(df: pd.DataFrame, folder_path, columns=None, index_columns=None, plot_parallel_all=False, min_k=3, max_k=10,
                     random_state=1234, run_silhouette=True, manually_func=None):
    if columns is None:
        columns = df.columns

    if 'k_means' not in os.listdir(folder_path):
        os.mkdir('{}/k_means'.format(folder_path))

    folder_path = '{}/k_means'.format(folder_path)

    # ___ Elbow analysis ___
    wcss, fig = elbow_analysis(df, columns=columns, plot=True)
    fig.savefig('{}/{}.png'.format(folder_path, 'ElbowAnalysis'))

    # ___ K-Means clustering ___
    for n_clusters in range(min_k, max_k + 1):
        k_means_df, clusters_df = k_means(df, n_clusters=n_clusters, columns=columns, index_columns=index_columns)
        k_means_df = move_columns_to_first_position(k_means_df, index_columns + columns)

        # ___ Silhouette Score ___
        if run_silhouette:
            fig = plot_silhouette_score(k_means_df, columns=columns)
            fig.savefig('{}/{}.png'.format(folder_path, f'Silhouette_n{n_clusters}'))

        if manually_func:
            k_means_df, clusters_df = manually_func(k_means_df, clusters_df)

        # ___ Parallel plot for all the values ___
        if plot_parallel_all:
            fig = parallel_coordinates_plot(k_means_df[columns + ['label']], 'label', opacity=0.01)
            fig.savefig('{}/{}.png'.format(folder_path, f'KMeans_n{n_clusters}All'))

        # ___ Parallel plot for centroids ___
        fig = parallel_coordinates_plot(clusters_df, 'label')
        fig.savefig('{}/{}.png'.format(folder_path, f'KMeans_n{n_clusters}Centroids'))
        k_means_df.to_csv(f"{folder_path}/KMeans_n{n_clusters}Data.csv", index=False)

        for label in clusters_df.index:
            logger.info('Total label {} clients: {}'.format(label, len(filter_by_value(k_means_df, 'label', label))))


def k_means_scatter_plot_2d(df_k, fig=None, ax=None, n_clusters=3, show=False):
    if not ax:
        fig, ax = new_fig()

    k_means_ = KMeans(n_clusters=n_clusters)
    k_means_labels_ = k_means_.fit_predict(df_k.to_numpy())
    centroids_k = k_means_.cluster_centers_
    centroids_k_df = pd.DataFrame(centroids_k, columns=['Column_A', 'Column_B'])

    scatter_plot_2d(df_k, title='k= {}'.format(n_clusters), ax=ax, labels_=k_means_labels_, centroids_=centroids_k_df)

    fig.show() if show and fig else None


def k_means_scatter_plot_2d_iterate(df_k):
    figure_k, axes_list_k = new_fig(nrows=3, ncols=3)

    for i_row in range(3):
        for i_col in range(3):
            k = i_row * 3 + i_col + 1
            k_means_scatter_plot_2d(df_k=df_k, ax=axes_list_k[i_row][i_col], n_clusters=k)

    figure_k.show()
    return figure_k


# _____ Hierarchical Agglomerative Clustering _____

def hac(df: pd.DataFrame, n_clusters: int = 3, columns=None, index_columns=None, link='ward', random_state=1234):
    """Receives as input a dataFrame, performs a HIERARCHICAL AGGLOMERATIVE CLUSTERING analysis on relevant columns and returns a dataFrame as output.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    If index_columns is specified then such columns are preserved in the output dataFrame.
    Linkages available: 'single', 'average', 'complete', 'ward'
    """

    np.set_printoptions(suppress=True)  # suppress scientific float notation

    if index_columns is None:
        index_columns = EMPTY_LIST
    if columns is None:
        columns = list(df.columns)

    hac_ = AgglomerativeClustering(linkage=link, n_clusters=n_clusters)  # the distance metric is by-default euclidean

    df_for_cluster = df[columns]
    df_hac = df[
        list(set([x for x in columns if x in df.columns] + [x for x in index_columns if x in df.columns]))] if len(
        columns) > 0 else df
    df_hac['label'] = hac_.fit_predict(df_for_cluster.to_numpy())

    # ___ Manually calculate centroids ___
    centroids_k_df = pd.DataFrame()
    for label in sorted(list(df_hac['label'].unique())):
        centroids_k_df = centroids_k_df.append(
            pd.DataFrame(df_hac[df_hac['label'] == label][columns + ['label']].mean()).transpose(), ignore_index=True)
    centroids_k_df['label'] = centroids_k_df['label'].astype(int)
    centroids_k_df = move_columns_to_first_position(centroids_k_df, list(df_for_cluster.columns))

    return df_hac, centroids_k_df


def hac_model(df: pd.DataFrame, n_clusters: int = 3, columns=None, link='ward', random_state=1234):
    """Receives as input a dataFrame, performs a HAC analysis on relevant columns and returns the HAC model.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    Linkages available: 'single', 'average', 'complete', 'ward'
    """

    if columns is None:
        columns = EMPTY_LIST

    df_for_cluster = df[columns] if len(columns) > 0 else df
    return AgglomerativeClustering(n_clusters=n_clusters, linkage=link).fit(df_for_cluster.to_numpy())


def hac_analysis(df: pd.DataFrame, folder_path, columns=None, index_columns=None, plot_parallel_all=False, min_k=3,
                 max_k=7, random_state=1234):
    if columns is None:
        columns = df.columns

    if 'hac' not in os.listdir(folder_path):
        os.mkdir('{}/hac'.format(folder_path))

    folder_path = '{}/hac'.format(folder_path)

    # ___ K-Means clustering ___
    for n_clusters in range(min_k, max_k):
        hac_df, clusters_df = hac(df, n_clusters=n_clusters, columns=columns, index_columns=index_columns)

        # ___ Parallel plot for all the values ___
        if plot_parallel_all:
            fig = parallel_coordinates_plot(hac_df[columns + ['label']], 'label', opacity=0.01)
            fig.savefig('{}/{}.png'.format(folder_path, f'HAC_n{n_clusters}All'))

        # ___ Parallel plot for centroids ___
        fig = parallel_coordinates_plot(clusters_df, 'label')
        fig.savefig('{}/{}.png'.format(folder_path, f'HAC_n{n_clusters}Centroids'))
        hac_df.to_csv(f"{folder_path}/HAC_n{n_clusters}Data.csv", index=False)

        # ___ Silhouette Score ___
        fig = plot_silhouette_score(hac_df, columns=columns)
        fig.savefig('{}/{}.png'.format(folder_path, f'Silhouette_n{n_clusters}'))

        for label in clusters_df.index:
            logger.info('Total label {} clients: {}'.format(label, len(filter_by_value(hac_df, 'label', label))))


def hac_scatter_plot_2d(df_hac, link='ward', fig=None, ax=None, show=True):
    # linkage available: 'single', 'average', 'complete', 'ward'

    np.set_printoptions(suppress=True)  # suppress scientific float notation

    # z = linkage(df_hac.to_numpy(), link)
    # dendrogram_plot(z)

    # the distance metric is by-default euclidean
    hac_ = AgglomerativeClustering(linkage=link, n_clusters=3).fit(df_hac.to_numpy())

    if not ax:
        fig, ax = new_fig()

    scatter_plot_2d(df_hac, title='HAC - {} linkage'.format(link), labels_=hac_.labels_, ax=ax)

    fig.show() if show and fig else None


def hac_iterate(df_hac):
    linkages = ['single', 'average', 'complete', 'ward']

    figure_k, axes_list_k = new_fig(nrows=2, ncols=2)

    for i_row in range(2):
        for i_col in range(2):
            k = i_row * 2 + i_col
            hac_scatter_plot_2d(df_hac=df_hac, ax=axes_list_k[i_row][i_col], link=linkages[k], show=False)

    figure_k.show()
    return figure_k


# _____ DBScan _____

def db_scan(df: pd.DataFrame, columns=None, index_columns=None, eps=0.5, min_samples=5):
    """Receives as input a dataFrame, performs a DB-SCAN analysis on relevant columns and returns a dataFrame as output.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    If index_columns is specified then such columns are preserved in the output dataFrame
    """

    if index_columns is None:
        index_columns = EMPTY_LIST
    if columns is None:
        columns = df.columns

    dbscan_ = DBSCAN(eps=eps, min_samples=min_samples)

    df_for_cluster = df[columns]
    df_dbscan = df[list(set([x for x in columns if x in df.columns] + [x for x in index_columns if x in df.columns]))]
    df_dbscan['label'] = dbscan_.fit_predict(df_for_cluster.to_numpy())

    # ___ Manually calculate centroids ___
    centroids_k_df = pd.DataFrame()
    for label in sorted(list(df_dbscan['label'].unique())):
        centroids_k_df = centroids_k_df.append(
            pd.DataFrame(df_dbscan[df_dbscan['label'] == label][columns + ['label']].mean()).transpose(),
            ignore_index=True)
    centroids_k_df['label'] = centroids_k_df['label'].astype(int)
    centroids_k_df = move_columns_to_first_position(centroids_k_df, list(df_for_cluster.columns))

    num_outliers = dbscan_.labels_[dbscan_.labels_ == -1].size
    print("There's a total of {} outliers".format(num_outliers))

    return df_dbscan, centroids_k_df


def db_scan_model(df: pd.DataFrame, columns=None, eps=0.5, min_samples=5):
    """Receives as input a dataFrame, performs a HAC analysis on relevant columns and returns the HAC model.
    If columns is specified then the analysis is done only on them otherwise the analysis is carried on all the columns of the dataFrame.
    """

    if columns is None:
        columns = EMPTY_LIST

    df_for_cluster = df[columns] if len(columns) > 0 else df
    return DBSCAN(eps=eps, min_samples=min_samples).fit(df_for_cluster.to_numpy())


def dbscan_analysis(df: pd.DataFrame, folder_path, columns=None, index_columns=None, plot_parallel_all=False,
                    min_eps=0.5, max_eps=2, random_state=1234):
    if columns is None:
        columns = df.columns

    if 'dbscan' not in os.listdir(folder_path):
        os.mkdir('{}/dbscan'.format(folder_path))

    folder_path = '{}/dbscan'.format(folder_path)

    # ___ Nearest Neighbour analysis ___
    fig = nearest_neighbors(df, columns=columns, plot=True)
    fig.savefig('{}/{}.png'.format(folder_path, 'NearestNeighborsAnalysis'))

    # ___ DBScan clustering ___
    for eps in np.linspace(min_eps, max_eps, 4):
        dbscan_df, clusters_df = db_scan(df, eps=eps, columns=columns, index_columns=index_columns)

        # ___ Parallel plot for all the values ___
        if plot_parallel_all:
            fig = parallel_coordinates_plot(dbscan_df[columns + ['label']], 'label', opacity=0.01)
            fig.savefig('{}/{}.png'.format(folder_path, f'DBScan_eps{eps}All'))

        # ___ Parallel plot for centroids ___
        fig = parallel_coordinates_plot(clusters_df, 'label')
        fig.savefig('{}/{}.png'.format(folder_path, f'DBScan_eps{eps}Centroids')) if fig is not None else None
        dbscan_df.to_csv(f"{folder_path}/DBScan_eps{eps}Data.csv", index=False)

        # ___ Silhouette Score ___
        if len(clusters_df.index) > 1:
            fig = plot_silhouette_score(dbscan_df, columns=columns)
            fig.savefig('{}/{}.png'.format(folder_path, f'Silhouette_eps{eps}'))
        else:
            logger.warning('Skipping silhouette since there is only 1 cluster')

        for label in clusters_df.index:
            logger.info('Total label {} clients: {}'.format(label, len(filter_by_value(dbscan_df, 'label', label))))


def nearest_neighbors(df: pd.DataFrame, n_neighbors: int = 2, columns=None, plot=True):
    if columns is None:
        columns = EMPTY_LIST

    df_for_cluster = df[columns] if len(columns) > 0 else df

    neigh = NearestNeighbors(n_neighbors=2)
    neighbors = neigh.fit(df_for_cluster.to_numpy())
    distances, indices = neighbors.kneighbors(df_for_cluster.to_numpy())

    distances = np.sort(distances, axis=0)
    distances = distances[:, 1]

    fig = None
    if plot:
        # der_1 = np.diff(distances)
        # der_2 = np.diff(der_1)
        fig = plt.figure(figsize=(6, 6))
        plt.plot(range(len(distances)), distances)
        # plt.plot(range(len(distances))[1:], der_1)
        # plt.plot(range(len(distances))[2:], der_2)
        plt.grid()

    return fig


def db_scan_scatter_plot_2d(df_dbscan, eps=0.5, fig=None, ax=None, show=True):
    dbscan = DBSCAN(eps=eps).fit(df_dbscan.to_numpy())

    if not ax:
        fig, ax = new_fig()

    scatter_plot_2d(df_dbscan, title='DBScan eps={}'.format(round(eps, 1)), labels_=dbscan.labels_, ax=ax)

    fig.show() if show and fig else None

    num_outliers = dbscan.labels_[dbscan.labels_ == -1].size
    print("There's a total of {} outliers".format(num_outliers))


def db_scan_iterate(df_dbscan):
    eps_values = np.arange(0.2, 0.6, 0.1)

    figure_k, axes_list_k = new_fig(nrows=2, ncols=2)

    for i_row in range(2):
        for i_col in range(2):
            k = i_row * 2 + i_col
            db_scan(df_dbscan=df_dbscan, ax=axes_list_k[i_row][i_col], eps=eps_values[k], show=False)

    figure_k.show()
    return figure_k


# _____ Mean Shift _____

def mean_shift(df: pd.DataFrame, quantile_, n_samples_, columns=None, index_columns=None, random_state=1234):
    if index_columns is None:
        index_columns = EMPTY_LIST
    if columns is None:
        columns = EMPTY_LIST

    df_for_cluster = df[columns] if len(columns) > 0 else df
    bandwidth = estimate_bandwidth(df_for_cluster, quantile=quantile_, n_samples=n_samples_, random_state=random_state)

    ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
    df_ms = df[
        list(set([x for x in columns if x in df.columns] + [x for x in index_columns if x in df.columns]))] if len(
        columns) > 0 else df
    df_ms['label'] = ms.fit_predict(df_for_cluster.to_numpy())

    centroids_k = ms.cluster_centers_

    centroids_k_df = pd.DataFrame(centroids_k, columns=columns if len(columns) > 0 else df.columns)
    centroids_k_df['label'] = centroids_k_df.index

    return df_ms, centroids_k_df, bandwidth


def mean_shift_analysis(df: pd.DataFrame, folder_path, columns=None, index_columns=None, plot_parallel_all=False,
                        quantile=None, n_samples=None, random_state=1234):
    if columns is None:
        columns = df.columns

    if quantile is None:
        quantile = [0.1, 0.1, 0.1, 0.15, 0.15, 0.15, 0.2, 0.2, 0.2, 0.25, 0.25, 0.25]

    if n_samples is None:
        n_samples = [500, 1000, 1500, 500, 1000, 1500, 500, 1000, 1500, 500, 1000, 1500]

    if 'mean_shift' not in os.listdir(folder_path):
        os.mkdir('{}/mean_shift'.format(folder_path))

    folder_path = '{}/mean_shift'.format(folder_path)

    for q, n_sam in zip(quantile, n_samples):
        ms_df, clusters_df, bandwidth = mean_shift(df, q, n_sam, columns=columns, index_columns=index_columns)
        ms_df = move_columns_to_first_position(ms_df, index_columns + columns)

        n_clusters = len(clusters_df.index)
        # ___ Parallel plot for centroids ___
        fig = parallel_coordinates_plot(clusters_df, 'label')
        if fig is not None:
            fig.savefig('{}/{}.png'.format(folder_path, f'MeanShift_n{n_clusters}_{q}_{n_sam}_{bandwidth}Centroids'))
        ms_df.to_csv(f"{folder_path}/MeanShift_n{n_clusters}_{q}_{n_sam}_{bandwidth}Data.csv", index=False)

        # ___ Silhouette Score ___
        if n_clusters > 1:
            fig = plot_silhouette_score(ms_df, columns=columns)
            fig.savefig('{}/{}.png'.format(folder_path, f'Silhouette_n{n_clusters}_{q}_{n_sam}_{bandwidth}'))
        else:
            logger.warning('Skipping silhouette since there is only 1 cluster')

        for label in clusters_df.index:
            logger.info('Total label {} clients: {}'.format(label, len(filter_by_value(ms_df, 'label', label))))


# _____ EXAMPLES _____

def clustering_algorithm_comparison():
    import time
    import warnings

    from sklearn import cluster, datasets, mixture
    from sklearn.neighbors import kneighbors_graph
    from sklearn.preprocessing import StandardScaler
    from itertools import cycle, islice

    np.random.seed(0)

    # ============
    # Generate datasets. We choose the size big enough to see the scalability
    # of the algorithms, but not too big to avoid too long running times
    # ============
    n_samples = 1500
    noisy_circles = datasets.make_circles(n_samples=n_samples, factor=.5,
                                          noise=.05)
    noisy_moons = datasets.make_moons(n_samples=n_samples, noise=.05)
    blobs = datasets.make_blobs(n_samples=n_samples, random_state=8)
    no_structure = np.random.rand(n_samples, 2), None

    # Anisotropicly distributed data
    random_state = 170
    X, y = datasets.make_blobs(n_samples=n_samples, random_state=random_state)
    transformation = [[0.6, -0.6], [-0.4, 0.8]]
    X_aniso = np.dot(X, transformation)
    aniso = (X_aniso, y)

    # blobs with varied variances
    varied = datasets.make_blobs(n_samples=n_samples,
                                 cluster_std=[1.0, 2.5, 0.5],
                                 random_state=random_state)

    # ============
    # Set up cluster parameters
    # ============
    plt.figure(figsize=(9 * 2 + 3, 12.5))
    plt.subplots_adjust(left=.02, right=.98, bottom=.001, top=.96, wspace=.05,
                        hspace=.01)

    plot_num = 1

    default_base = {'quantile': .3,
                    'eps': .3,
                    'damping': .9,
                    'preference': -200,
                    'n_neighbors': 10,
                    'n_clusters': 3,
                    'min_samples': 20,
                    'xi': 0.05,
                    'min_cluster_size': 0.1}

    datasets = [
        (noisy_circles, {'damping': .77, 'preference': -240,
                         'quantile': .2, 'n_clusters': 2,
                         'min_samples': 20, 'xi': 0.25}),
        (noisy_moons, {'damping': .75, 'preference': -220, 'n_clusters': 2}),
        (varied, {'eps': .18, 'n_neighbors': 2,
                  'min_samples': 5, 'xi': 0.035, 'min_cluster_size': .2}),
        (aniso, {'eps': .15, 'n_neighbors': 2,
                 'min_samples': 20, 'xi': 0.1, 'min_cluster_size': .2}),
        (blobs, {}),
        (no_structure, {})]

    for i_dataset, (dataset, algo_params) in enumerate(datasets):
        # update parameters with dataset-specific values
        params = default_base.copy()
        params.update(algo_params)

        X, y = dataset

        # normalize dataset for easier parameter selection
        X = StandardScaler().fit_transform(X)

        # estimate bandwidth for mean shift
        bandwidth = cluster.estimate_bandwidth(X, quantile=params['quantile'])

        # connectivity matrix for structured Ward
        connectivity = kneighbors_graph(
            X, n_neighbors=params['n_neighbors'], include_self=False)
        # make connectivity symmetric
        connectivity = 0.5 * (connectivity + connectivity.T)

        # ============
        # Create cluster objects
        # ============
        ms = cluster.MeanShift(bandwidth=bandwidth, bin_seeding=True)
        two_means = cluster.MiniBatchKMeans(n_clusters=params['n_clusters'])
        ward = cluster.AgglomerativeClustering(
            n_clusters=params['n_clusters'], linkage='ward',
            connectivity=connectivity)
        spectral = cluster.SpectralClustering(
            n_clusters=params['n_clusters'], eigen_solver='arpack',
            affinity="nearest_neighbors")
        dbscan = cluster.DBSCAN(eps=params['eps'])
        affinity_propagation = cluster.AffinityPropagation(
            damping=params['damping'], preference=params['preference'])
        average_linkage = cluster.AgglomerativeClustering(
            linkage="average", affinity="cityblock",
            n_clusters=params['n_clusters'], connectivity=connectivity)
        gmm = mixture.GaussianMixture(
            n_components=params['n_clusters'], covariance_type='full')

        clustering_algorithms = (
            ('MiniBatchKMeans', two_means),
            ('AffinityPropagation', affinity_propagation),
            ('MeanShift', ms),
            ('SpectralClustering', spectral),
            ('HAC - ward', ward),
            ('HAC - average', average_linkage),
            ('DBSCAN', dbscan),
            ('GaussianMixture', gmm)
        )

        for name, algorithm in clustering_algorithms:
            t0 = time.time()

            # catch warnings related to kneighbors_graph
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="the number of connected components of the " +
                            "connectivity matrix is [0-9]{1,2}" +
                            " > 1. Completing it to avoid stopping the tree early.",
                    category=UserWarning)
                warnings.filterwarnings(
                    "ignore",
                    message="Graph is not fully connected, spectral embedding" +
                            " may not work as expected.",
                    category=UserWarning)
                algorithm.fit(X)

            t1 = time.time()
            if hasattr(algorithm, 'labels_'):
                y_pred = algorithm.labels_.astype(np.int)
            else:
                y_pred = algorithm.predict(X)

            plt.subplot(len(datasets), len(clustering_algorithms), plot_num)
            if i_dataset == 0:
                plt.title(name, size=18)

            colors = np.array(list(islice(cycle(['#377eb8', '#ff7f00', '#4daf4a',
                                                 '#f781bf', '#a65628', '#984ea3',
                                                 '#999999', '#e41a1c', '#dede00']),
                                          int(max(y_pred) + 1))))
            # add black color for outliers (if any)
            colors = np.append(colors, ["#000000"])
            plt.scatter(X[:, 0], X[:, 1], s=10, color=colors[y_pred], alpha=0.25)

            plt.xlim(-2.5, 2.5)
            plt.ylim(-2.5, 2.5)
            plt.xticks(())
            plt.yticks(())
            plt.text(.99, .01, ('%.2fs' % (t1 - t0)).lstrip('0'),
                     transform=plt.gca().transAxes, size=15,
                     horizontalalignment='right')
            plot_num += 1

    plt.show()


def blobs_examples():
    from sklearn.datasets import make_blobs

    random_state = 1234  # another interesting example can be generated using the seed 36
    no_clusters = 3
    no_samples = 1500

    X_blobs, y_blobs = make_blobs(centers=no_clusters, n_samples=no_samples, random_state=random_state)
    df_example = pd.DataFrame(X_blobs, columns=['Column_A', 'Column_B'])

    # k_means_scatter_plot_2d(df_k=df_example, show=True)
    # k_means_scatter_plot_2d_iterate(df_k=df_example)
    # hac_iterate(df_example)
    # db_scan(df_example, eps=0.5)
    # db_scan_iterate(df_example)
    # clustering_algorithm_comparison()
    silhouette_analysis(df_example)


def california_housing():
    from sklearn.datasets import fetch_california_housing

    houses = fetch_california_housing(as_frame=True)
    house_df = houses['frame']

    house_df['cluster'] = np.random.randint(1, 4, house_df.shape[0])

    # scatter_plot_dataframe(house_df)
    parallel_coordinates_plot(house_df, 'cluster')

    print('end')


def seaborn_examples():
    import seaborn as sns

    # Load tips dataset
    tips = sns.load_dataset("tips")
    seaborn_set_theme()
    # seaborn_relationship_plot(tips, x='total_bill', y='tip', col='time', hue='smoker', style='smoker', size='size')
    # seaborn_distribution_plot(tips, x='total_bill', y='tip', col='time', hue='smoker', facet_kws=dict(sharex=False))
    # seaborn_distribution_plot(tips, x='total_bill', col='time', hue='smoker', facet_kws=dict(sharex=False), kde=True)
    # seaborn_categorical_plot(tips, x='day', y='total_bill', hue='smoker')
    # seaborn_categorical_plot(tips, x='day', y='total_bill', hue='smoker', kind='swarm')
    # seaborn_categorical_plot(tips, x='day', y='total_bill', hue='smoker', kind='violin')
    # seaborn_categorical_plot(tips, x='day', y='total_bill', hue='smoker', kind='violin', split=True)
    # seaborn_categorical_plot(tips, x='day', y='total_bill', hue='smoker', kind='bar')

    # Load dots dataset
    dots = sns.load_dataset("dots")
    # seaborn_relationship_plot(dots, x='time', y='firing_rate', col='align', hue='choice', size='coherence', style='choice', kind='line', facet_kws=dict(sharex=False))
    # seaborn_linear_regression_plot(dots, x='time', y='firing_rate', col='align', hue='choice', facet_kws=dict(sharex=False))

    # Load penguins dataset
    penguins = sns.load_dataset("penguins")
    # seaborn_joint_distribution_plot(df=penguins, x="flipper_length_mm", y="bill_length_mm", hue="species")
    # seaborn_pair_plot(penguins)
    # seaborn_pair_plot(penguins, hue="species")
    seaborn_set_theme(theme_style='ticks')
    g = seaborn_relationship_plot(penguins, x="bill_length_mm", y="bill_depth_mm", hue="body_mass_g", palette="crest",
                                  marker="x", s=100, show=False)
    g.set_axis_labels("Bill length (mm)", "Bill depth (mm)", labelpad=10)
    g.legend.set_title("Body mass (g)")
    g.figure.set_size_inches(6.5, 4.5)
    g.ax.margins(.15)
    g.despine(trim=True)
    plt.show()


def seaborn_class_examples():
    # sb_tips = Seaborn(sns.load_dataset("tips"))
    # sb_tips.set_theme()
    # sb_tips.relationship_plot(x='total_bill', y='tip', col='time', hue='smoker', style='smoker', size='size')
    # sb_tips.distribution_plot(x='total_bill', y='tip', col='time', hue='smoker', facet_kws=dict(sharex=False))
    # sb_tips.distribution_plot(x='total_bill', col='time', hue='smoker', facet_kws=dict(sharex=False), kde=True)
    # sb_tips.categorical_plot(x='day', y='total_bill', hue='smoker')
    # sb_tips.categorical_plot(x='day', y='total_bill', hue='smoker', kind='swarm')
    # sb_tips.categorical_plot(x='day', y='total_bill', hue='smoker', kind='violin')
    # sb_tips.categorical_plot(x='day', y='total_bill', hue='smoker', kind='violin', split=True)
    # sb_tips.categorical_plot(x='day', y='total_bill', hue='smoker', kind='bar')
    # sb_tips.show()

    # sb_dots = Seaborn(sns.load_dataset("dots"))
    # sb_dots.set_theme()
    # sb_dots.relationship_plot(x='time', y='firing_rate', col='align', hue='choice', size='coherence', style='choice', kind='line', facet_kws=dict(sharex=False))
    # sb_dots.linear_regression_plot(x='time', y='firing_rate', col='align', hue='choice', facet_kws=dict(sharex=False))

    sb_penguins = Seaborn(sns.load_dataset("penguins"))
    sb_penguins.joint_distribution_plot(x="flipper_length_mm", y="bill_length_mm", hue="species")
    sb_penguins.pair_plot()
    sb_penguins.pair_plot(hue="species")
    sb_penguins.set_theme(theme_style='ticks')
    sb_penguins.relationship_plot(x="bill_length_mm", y="bill_depth_mm", hue="body_mass_g", palette="crest", marker="x",
                                  s=100, show=False)
    sb_penguins.set_axis_labels("Bill length (mm)", "Bill depth (mm)", labelpad=10)
    sb_penguins.set_legend_title("Body mass (g)")
    sb_penguins.set_size_inches(6.5, 4.5)
    sb_penguins.set_axis_margins(.15)
    sb_penguins.despine(trim=True)
    sb_penguins.show()


if __name__ == '__main__':
    # blobs_examples()
    # california_housing()
    # seaborn_examples()
    seaborn_class_examples()

    print('end')
