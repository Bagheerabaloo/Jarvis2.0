import numpy as np
import pandas as pd


# ____ ndarray _____

#   ___ generate array ___
np.array([[-1, -1, 1, 3], [4, -1, 0, -1], [8, -1, 1, 0]])

#   ___ generate sequence ___
np.arange(1, 10.1, .25) ** 2

#   ___ generate evenly spaced numbers over a specified interval ___
np.linspace(0, 100, 3 + 1)

#   ___ random seed ___
np.random.seed(2)

#   ___ generate random bool matrix ___
np.random.randint(0, 2, size=[1, 2]).astype(np.bool)

#   ___ generate random float matrix ___
np.random.randint(-100, 100, 1000).astype(float)

#   ___ generate random array normally(standard) distributed ___
np.random.randn(37)
# np.random.random(100) # ??

#   ___ generate random array uniformly distributed ___
np.random.uniform(size=100)

#   ___ generate random array from distributions ___
np.random.chisquare(8, 1000)
np.random.beta(8, 2, 1000) * 40
np.random.normal(50, 3, 1000)
np.random.RandomState(616).lognormal(size=(3, 3))

#   ___ sort ndarrays ___
np.sort(np.random.uniform(size=100))

#   ___ sum ndarrays ___
np.arange(1, 10.1, .25) ** 2 + np.random.randn(37)

#   ___ compute percentile ___
np.percentile(np.random.uniform(size=10000), q=[0, 25, 50, 75, 100])


#   ___ logarithmic ___
def hist_edges_equal_n(x, nbin):
    npt = len(x)
    return np.interp(np.linspace(0, npt, nbin + 1),
                     np.arange(npt),
                     np.sort(x))


# histogram for same number of points
x = np.random.randn(1000)
n = 1000
np.logspace(np.log10(x.min()), np.log10(x.max()), n, base=10)

# _____ create dataFrames from ndarrays _____

df = pd.DataFrame({
    'x1': np.random.randint(-100, 100, 1000).astype(float),
    'y1': np.random.randint(-80, 80, 1000).astype(float)
})

df = pd.DataFrame(data=np.c_[df['data'], df['target']],
                  columns=df['feature_names'] + df['target_names'])
