# Spark Cross Validator

## Abstract
The traditional use of Python/R is typically limited to processing data on a single machine, which can be time-consuming and require extensive re-engineering. Spark is a powerful, unified platform that provides data scientists and engineers with a fast and easy way to process large amounts of data and provides solution to the machine learning problems. With Spark mllib, data scientists can focus on their problems instead of the complexity of handling distributed data. For distributing workload on a Spark cluster, Spark provides a different implementation of the cross-validation algorithm.
## Table of contents

**I. Introduction of Spark ML library and Spark cross validator**
     
     1.1 Spark MLlib
     1.2 Spark Cross Validator

**II. Implementation of Cross validation technique**
     
     2.1 Serial Cross validation
     2.2 Parallel Cross validation
     2.3 Cross validation process

**III. Parameters of Spark cross validatork**
     
**IV. Advantages of Spark cross validation over manual implementation** 

**V. Conclusion**

**VI. References** 

## **I. Introduction of Spark ML library and Spark cross validator**

### *1.1 Spark MLlib*
Spark's machine learning (ML) library is MLlib. Its goal is to make scalable and simple practical machine learning. It provides, at a high level, tools such as
* ML Algorithms: These are common learning algorithms such as classification, regression, clustering, and collaborative filtering.
*	Featurization includes feature extraction, transformation, dimensionality reduction, and selection.
*	Pipelines are tools for building, evaluating, and fine-tuning systems. 
*	Persistence: saving and loading algorithms, models, and data Pipelines
* Utilities: linear algebra, statistics, data handling, and so on.


### *1.2 Spark Cross Validator:*
K-fold cross validation selects a model by splitting the dataset into a set of non-overlapping randomly partitioned folds that are used as separate training and test datasets, for example, with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs, each of which uses 2/3 of the data for training and 1/3 for testing. Each fold serves as the test set exactly once.

<img src="https://databricks.com/wp-content/uploads/2016/02/scikit-learn-without-spark-training-diagram.png" width="800"/>

## **II. Implementation of Cross validation technique**
There are two types of cross validation techniques in spark.
1.	Serial cross validation technique
2.	Parallel cross validation technique

### *2.1 Serial Cross Validation*
Cross validation was done serially until Spark 2.3. That is, for each cross-validation combination, Spark runs one set of jobs before moving on to the next combination.

### *2.2 Parallel cross validation*
Each cross-validation combination is independent of the others. That means we can run them concurrently. This will use more resources, but it will get us results much faster.

### *2.3 Cross validation process*
Initially, the data is divided into folds that will be used as separate training and testing datasets. If K-Folds = 3, three training and test dataset pairs with (train -2/3 portion and test – 1/3 portion) will be created.

Cross validator computes the average evaluation metric for the three Models produced by fitting the Estimator on the three different (training, test) dataset pairs to evaluate a specific ParamMap. Cross Validator finally re-fits the Estimator using the best ParamMap and the entire dataset after identifying the best ParamMap.

<img src="https://developer-blogs.nvidia.com/wp-content/uploads/2020/09/ml-cross-validation-process.png" width="800"/>

## **III. Parameters of Spark cross validator:**
Cross validator requires Estimator, Estimator ParamMaps and an Evaluator. ParamGridBuilder to construct a grid of parameters to search over. An Estimator is a type of algorithm that can be fitted to a Data Frame to generate a Transformer.  In pyspark ML pipeline is treated as Estimator for cross validator instance. ML pipeline consists of three stages tokenizer (splits the data based on the white spaces), hashingTF (Using the hashing trick, maps a sequence of terms to their term frequencies. Because a simple modulo is used to convert the hash function to a column index, a power of two should be used as the numFeatures parameter; otherwise, the features will not be mapped evenly to the columns.), and lr (Logistic Regression).   An Estimator Param Map

<img src="https://spark.apache.org/docs/1.6.1/img/ml-Pipeline.png" width="800"/>

## **IV. Advantages of Spark cross validator over the manual cross validation technique:**
Manual implementation of cross validation requires long hours to fine tune a model. Typically, this tuning entails running a massive set of different Machine Learning (ML) tasks written in Python or R. Spark provides a different implementation of the cross-validation algorithm for distributing workload on a Spark cluster. Spark includes a general machine learning library called MLlib, which is designed for ease of use, scalability, and integration with other tools. Data scientists can solve and iterate through data problems more quickly thanks to Spark's scalability, language compatibility, and speed.


### *4.1 Scalability:*
Manual cross validators process the data on single machine, where data movement becomes time consuming, analysis necessitates sampling, and transitioning from development to production environments necessitates extensive re-engineering. Spark can distribute the data across multiple machines (distributed machines) with its easy-to-use API’s. Spark MLlib has the ability to run the same machine learning code on your laptop and on a large cluster without failure. Businesses can continue to use the same workflows as their user base and data sets expand. 

### *4.2 Simplicity:*
Spark has simple APIs that data scientists are familiar with from tools like R and Python. Experts can easily tune the system by adjusting important knobs and switches, while novices can run algorithms right away (parameters).

### *4.3 Compatibility:*
Spark MLlib has compatibility with the most used data science tools such as R, python pandas, scikit learn. It provides easy integration with existing tools.  

### *4.4 Streamlined end-to-end:*
Creating machine learning models is a multistep process that begins with data ingest and progresses through trial and error to production. Because MLlib is built on top of Spark, it is possible to address these disparate needs with a single tool rather than a collection of disparate ones. Lower learning curves, less complex development and production environments, and ultimately shorter time to deliver high-performing models are the benefits.

## **V. CONCLUSION:**
The Spark cross validator efficiently processes large amounts of data. It facilitates and accelerates data processing to tune the model. Spark facilitates model integration from development to production. Data engineers can concentrate on distributed systems, whereas data scientists can concentrate on machine learning algorithms and models. Spark improves machine learning by allowing data scientists to focus on the data problems that are most important to them while transparently leveraging the speed, ease of use, and integration of Spark's unified platform.

## **VI. References:**

1.	https://spark.apache.org/docs/latest/ml-guide.html
2.	https://george-jen.gitbook.io/data-science-and-apache-spark/cross-validation
3.	https://medium.com/@srinivasugaddam/machine-learning-model-selection-and-hyperparameter-tuning-using-pyspark-80dd8c1bfc56
4.	https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.tuning.CrossValidator.html?highlight=crossvalidator
5.	https://www.infoworld.com/article/3141605/review-spark-lights-up-machine-learning.html
6.	https://www.infoworld.com/article/3031690/why-you-should-use-spark-for-machine-learning.html
7.	https://www.analyticsvidhya.com/blog/2018/05/improve-model-performance-cross-validation-in-python-r/
8.	https://databricks.com/blog/2016/02/08/auto-scaling-scikit-learn-with-apache-spark.html



