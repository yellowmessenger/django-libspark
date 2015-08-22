from django.shortcuts import render
from models import *
from libspark.SparkRDD import SparkRDD
from django.http import JsonResponse
from nltk.corpus import stopwords
cachedStopWords = stopwords.words("english")
rawReviewRDD = None
reviewsWithMessages = None
tokenizedMessages = None
def find():
    reviews = []
    for review in connection.Review.find():
        newReview = {
            'message': review['message'],
            'fbId': review['fbId']
        }
        reviews.append(newReview)
    global rawReviewRDD
    rawReviewRDD= SparkRDD(data=reviews).cache()
    return reviews

def find_all_review(request):
    reviews = find()
    return JsonResponse(reviews,safe=False)

def removePunctuations(message):
    message = message.encode('utf-8')
    return message.translate(None, string.punctuation)

def find_reviews_with_messages():
    find()
    global reviewsWithMessages
    global tokenizedMessages
    reviewsWithMessages = rawReviewRDD.filter(lambda x: x['message'] != None).map(lambda x: (x['fbId'],x['message'].lower()))
    tokenizedMessages = reviewsWithMessages.map(lambda (k,v): (k,removePunctuations(v))).map(lambda (k,v): (k,v.split(' '))).cache()

def frequently_used_words(request):
    find()
    reviewsWithMessages = rawReviewRDD.filter(lambda x: x['message'] != None).map(lambda x: (x['fbId'],x['message'].lower()))
    tokenizedMessages = reviewsWithMessages.map(lambda (k,v): (k,removePunctuations(v))).map(lambda (k,v): (k,v.split(' '))).cache()
    def removeStopWords(tokens):
        newTokens = []
        for word in tokens:
            if word.lower() not in cachedStopWords:
                newTokens.append(word)
        return newTokens

    def removeEmpty(tokens):
        return [token for token in tokens if token != '']

    withoutStopWordsRDD = (tokenizedMessages.map(lambda (k,v): (k,removeStopWords(v)))
                       .map(lambda (k,v): (k,removeEmpty(v)))
                       .cache())
    messageTokenMap = (withoutStopWordsRDD
                       .flatMap(lambda (k,v): v)
                       .map(lambda v: (v,1))
                       .reduceByKey(lambda a,b:a+b))

    topWords = messageTokenMap.takeOrdered(100,lambda (k,v): -v)
    frequentWords = []
    for wordCount in topWords:
        print '{0:30}{1}' . format(wordCount[0],wordCount[1])
        fw = {
            wordCount[0]:wordCount[1]
        }
        frequentWords.append(fw)
    return JsonResponse(frequentWords,safe=False)
