import pandas as pd
import string
import sys
from collections import Counter
from nltk import word_tokenize
from nltk import everygrams
from nltk.corpus import stopwords
import matplotlib.pyplot as plt

url = "https://www.ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv?2d214f774ee2c1533ac47f2a3bce3222"
df = pd.read_csv(url, sep='|', parse_dates=['date'])

#print(df.info())

# Check correct columns
#check_manually = df[df.isnull().any(axis='columns')]
#check_manually.to_csv("./ECB_Tweets/check_manually.csv")

df.to_csv("./ECB_Tweets/All_ECB_Speeches.csv")

# Only look at column with speeches and contents / no presentation

df = df[~df.isnull().any(axis='columns')]
#print(df.info())

df = df[df['date'] >= '2016-01-01']

def process_speech(speech, stopwords = []):
    tokens = word_tokenize(speech)
    tokens = [tok for tok in tokens if tok not in stopwords and not tok.isdigit() ]
    list_of_ngrams = everygrams(tokens, 1, 2)
    return [tok for tok in list_of_ngrams]

punct = list(string.punctuation)
stopword_list = stopwords.words('english') + punct + []


df['tokens'] = df['contents'].apply(process_speech, stopwords = stopword_list)
print(df.head())
#df['relative_use'] = df['tokens'].apply(lambda x: ((x.count("proportionate") + x.count("proportionality"))/len(x)))
df['relative_use'] = df['tokens'].apply(lambda x: ((x.count(('climate', 'change')))/len(x)))
#tf = Counter()
#for idx, row in df.iterrows():
#    tokens = process_speech(row['contents'], stopword_list)
#
#
#for tag, count in tf.most_common(50):
#    print("{0}: {1}".format(tag,count))

fix, ax = plt.subplots()

ax.plot(df['date'], df['relative_use'])
plt.show()
