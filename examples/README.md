# Examples
We have built an example application `Foodista` which uses the Django-LibSpark. Foodista mines data of restaurant and food review using Apache Spark and gives
out the frequently used words in the database.

# Dependencies
The dependencies for building the example project are:
* [MongoDB](https://www.mongodb.org)
* [MongoKit](http://namlook.github.io/mongokit/)
* [NLTK Library](http://www.nltk.org)

# Build Instructions
1. The first step is to setup the MongoDB Server on your local machine.
MongoDB site provides installation instructions for all major OS.
2. You will need to populate the MongoDB with the data we have collected. The dump of the data is provided in the root folder of the `Django-LibSpark` library.
3. Run the following code to push the data dump to your instance of MongoDB:
```bash
$ mongorestor --verbose path-to-dump
```
4. Next you will need to setup NLTK. Install NLTK using pip -
```bash
$ pip install nltk
```

Once NLTK is installed, fire up your python interpreter and run the following
code-
```python
>>> import nltk
>>> nltk.download()
```
A window will pop open. Go on the All Packages tab and search for `stopwords` and download it.
5. Your development depdency is done. Now is the time to fire up the Django Development Server -
```bash
$ python manage.py runserver
```
and point your browser to http://localhost:8000/FUW
