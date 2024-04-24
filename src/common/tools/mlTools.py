import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
from time import time
from typing import List
from abc import ABC, abstractmethod

from matplotlib import cm
from sklearn.cluster import KMeans, MeanShift, AffinityPropagation
from sklearn.metrics import silhouette_score, silhouette_samples
from sklearn.preprocessing import StandardScaler

from src.utils import *
from src.utils_dataframe import *

logger = init_logger('ml')

PLOT_FOLDER = f"plots/{timestamp2date(time(), frmt='%m_%d_%H_%M_%S')}/"

# at startup initialize the folder path
Path(PLOT_FOLDER).mkdir(parents=True, exist_ok=True)


class ClusterEstimator(ABC):
    """
    Generic estimator, which takes a model and a generic number of arguments as input.
    It creates the model and provide common helper function to fit, predict and extract
    obtained clustering information
    """

    def __init__(self, model, name, **kwargs):
        self.model = model(**kwargs)
        self.name = name
        self.k = kwargs.get('n_clusters')
        self.columns = None
        self.labels = None

    def fit_predict(self, x: pd.DataFrame):
        self.columns = x.columns
        self.labels = self.model.fit_predict(x.to_numpy())
        return self.labels

    def fit(self, x: pd.DataFrame):
        self.columns = x.columns
        return self.model.fit(x.to_numpy())

    def predict(self, x: pd.DataFrame):
        self.labels = self.model.predict(x.to_numpy())
        return self.labels

    @abstractmethod
    def centroids(self):
        pass


class KMeansEstimator(ClusterEstimator):

    def __init__(self, n_clusters, random_state=1234):
        super().__init__(KMeans, name='kmeans', n_clusters=n_clusters, random_state=random_state)

    def centroids(self, pandas=True):
        centroids_k = self.model.cluster_centers_
        centroids_k_df = pd.DataFrame(centroids_k, columns=self.columns)
        centroids_k_df['label'] = centroids_k_df.index
        return centroids_k_df


class MeanShiftEstimator(ClusterEstimator):

    def __init__(self, random_state=1234):
        super().__init__(MeanShift, name='mean-shift', random_state=random_state)

    def centroids(self, pandas=True):
        centroids_k = self.model.cluster_centers_
        centroids_k_df = pd.DataFrame(centroids_k, columns=self.columns)
        centroids_k_df['label'] = centroids_k_df.index
        return centroids_k_df


class AffinityPropagationEstimator(ClusterEstimator):

    def __init__(self, damping, random_state=1234):
        super().__init__(AffinityPropagation, name='affinity-propagation', damping=damping, random_state=random_state)
        self.k = damping

    def centroids(self, pandas=True):
        centroids_k = self.model.cluster_centers_
        centroids_k_df = pd.DataFrame(centroids_k, columns=self.columns)
        centroids_k_df['label'] = centroids_k_df.index
        return centroids_k_df


def scale(df_: pd.DataFrame, columns_to_scale: List[str] = None, scaler=None) -> pd.DataFrame:
    """
    Apply a scaler to the input dataset.
    If `columns` is provided the scaler will be applied only in that subset.
    If the `scaler` is provided use that one, otherwise use standard one.

    Returns the modified data set, including all original columns modified accordingly
    """

    if columns_to_scale is None:
        columns_to_scale = df_.columns.tolist()

    if scaler is None:
        scaler = StandardScaler()

    # split the dataFrame in cols to bypass and cols to scale
    columns_to_bypass = [x for x in df_.columns if x not in columns_to_scale]
    df_to_bypass = df_[columns_to_bypass]
    df_to_scale = df_[columns_to_scale]

    # scale data
    scaled_features = scaler.fit_transform(df_to_scale)  # Scale only cols to scale
    df_scaled_features = pd.DataFrame(scaled_features, index=df_to_scale.index, columns=columns_to_scale)

    # concatenate scaled data with data bypassed
    result = pd.concat([df_to_bypass, df_scaled_features], axis=1)

    return result


def elbow_method(estimators: List[ClusterEstimator], plot=True):
    logger.info('Performing ELBOW method..')
    k_values = list(map(lambda m: m.k, estimators))
    # get the total within-cluster sum of square for each k-means
    wcss = [estimator.model.inertia_ for estimator in estimators]

    fig = None
    if plot:
        fig = plt.figure(figsize=(6, 6))
        plt.xticks(k_values)
        plt.plot(k_values, wcss, 'bo-', color='red', label='WCSS')
        plt.grid(True)
        plt.xlabel('Number of clusters')
        plt.legend()
        plt.title(f'Elbow method for {estimators[0].name}')

    return wcss, fig


def silhouette(df_: pd.DataFrame, estimators: List[ClusterEstimator], plot=True):
    logger.info('Performing SILHOUETTE method..')

    silhouette_coefficients = []
    figures = []
    for estimator in estimators:
        n_clusters = estimator.k
        df_raw = df_.to_numpy()

        cluster_labels = estimator.labels

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        silhouette_coefficient = silhouette_score(df_raw, cluster_labels)
        silhouette_coefficients.append(silhouette_coefficient)

        # Compute the silhouette scores for each sample
        sample_silhouette_values = silhouette_samples(df_raw, cluster_labels)

        if plot:
            # Create a subplot with 1 row and 1 column
            fig, ax1 = plt.subplots(1, 1)
            fig.set_size_inches(18, 7)

            # The 1st subplot is the silhouette plot
            # The silhouette coefficient can range from -1, 1 but in this example all
            # lie within [-0.1, 1]
            ax1.set_xlim([-0.1, 1])
            # The (n_clusters+1)*10 is for inserting blank space between silhouette
            # plots of individual clusters, to demarcate them clearly.
            ax1.set_ylim([0, len(df_raw) + (n_clusters + 1) * 15])

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

    return sample_silhouette_values, silhouette_coefficients, figures