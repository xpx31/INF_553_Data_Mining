import pandas as pd
import numpy as np
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split


path_train = "..\Data\yelp_train.csv"
path_val = "..\Data\yelp_val.csv"
path_train_csv = "..\Data\shuffle_train.csv"
path_val_csv = "..\Data\shuffle_val.csv"

# Read in dataframe
df_train = pd.read_csv(path_train)
df_val = pd.read_csv(path_val)
df = pd.concat([df_train, df_val])
df = shuffle(df)

# df_n.to_csv(path_test_csv, index=False)
df_n_train, df_n_val = train_test_split(df, test_size=0.235)
df_n_train.to_csv(path_train_csv, index=False)
df_n_val.to_csv(path_val_csv, index=False)
