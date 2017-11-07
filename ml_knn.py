# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 19:27:56 2017

@author: Bao Ning
"""
import pandas
from sklearn import model_selection
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import LinearSVC
from sklearn import preprocessing
import time
# Load dataset
#print(inode, index, fileType, file[inode], req, minr, 
#                      maxr, avg, lR, lW, lR+lW, len(pidList), min(l[0]), max(l[0]), sum(l[0])/len(l[0]),
#                      min(l[1]), max(l[1]), sum(l[1])/len(l[1]),
#                      min(l[2]), max(l[2]), sum(l[2])/len(l[2]),
##                      min(l[3]), max(l[3]), sum(l[3])/len(l[3]), sep=',',file=fout)
s = time.time()
names = ['inode', 'index', 'filetype', 'filename', 'latest access time', 
         'min access distance','max access distance', 'avg access distance', 'read req', 'write req', 
         'total req', 'process number', 'min process read', 'max process read','avg process read',
          'min process write', 'max process write', 'avg process write', 'min process per page read', 'max process per page read',
         'avg process per page read', 'min process per page write', 'max process per page write','avg process per page write', 'future read',
          'future write', 'future total', 'access type', 'data type']
dataset = pandas.read_csv("fileserver_10m_60s_4.parse", names=names, header=None)
#print(dataset.columns)
#print(dataset.head(20))
deletedcolumns = ['index', 'future read', 'future write', 'future total', 'access type']
for col in deletedcolumns:
	dataset.drop(col,axis=1,inplace=True)
columns = ['inode', 'filetype', 'filename']
for col in columns:
    le = preprocessing.LabelEncoder()
    le.fit(dataset[col])
    dataset[col] = le.transform(dataset[col])

#url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
#names = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']
#dataset = pandas.read_csv(url, names=names)
#print(dataset.shape)
#print(dataset.head(20))
#print(dataset.describe())
#print(dataset.groupby('data type').size())
#print(dataset.groupby('access type').size())
#array = dataset.values
#X = array[:,:-1]
#Y = list(array[:,-1])
##print(len(X[0]))
##print(Y)
#validation_size = 0.5
array = dataset.values
X = array[0:565033,0:-1]
Y = list(array[0:565033,-1])
validation_size = 0.50
seed = 7
X_train, X_validation, Y_train, Y_validation = model_selection.train_test_split(X, Y, test_size=validation_size, random_state=seed)
seed = 7
print("data management finished")
#knn = KNeighborsClassifier()
#knn.fit(X_train, Y_train)
#predictions = knn.predict(X_validation)
#print(accuracy_score(Y_validation, predictions))
#print(confusion_matrix(Y_validation, predictions))
#print(classification_report(Y_validation, predictions))
#knn = LogisticRegression()
#knn.fit(X_train, Y_train)
#predictions = knn.predict(X_validation)
#print(accuracy_score(Y_validation, predictions))
#print(confusion_matrix(Y_validation, predictions))
#print(classification_report(Y_validation, predictions))
model = LinearSVC(class_weight={True:100})
print("initial finished")
model.fit(X_train, Y_train)
print("train finished")
predictions = model.predict(X_validation)
print(accuracy_score(Y_validation, predictions))
print(confusion_matrix(Y_validation, predictions))
print(classification_report(Y_validation, predictions))
e = time.time()
print("consumed", e-s)