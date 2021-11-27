# Requires the PyMongo package.
# https://api.mongodb.com/python/current
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
result = client['timeseriesdemo']['weather'].aggregate([
    {
        '$project': {
            'ts': {
                '$dateToParts': {
                    'date': '$ts'
                }
            }, 
            'source': 1, 
            'temp': 1, 
            'windSpeed': 1
        }
    }, {
        '$group': {
            '_id': {
                'source': '$source', 
                'year': '$ts.year', 
                'month': '$ts.month', 
                'day': '$ts.day', 
                'hour': '$ts.hour', 
                'minute': '$ts.minute', 
                'second': {
                    '$multiply': [
                        {
                            '$floor': {
                                '$divide': [
                                    '$ts.second', 5
                                ]
                            }
                        }, 5
                    ]
                }
            }, 
            'avgTemp': {
                '$avg': '$temp'
            }, 
            'avgWindSpeed': {
                '$avg': '$windSpeed'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'source': '$_id.source', 
            'temp': {
                '$round': [
                    '$avgTemp', 1
                ]
            }, 
            'windSpeed': {
                '$round': [
                    '$avgWindSpeed', 1
                ]
            }, 
            'ts': {
                '$dateFromParts': {
                    'year': '$_id.year', 
                    'month': '$_id.month', 
                    'day': '$_id.day', 
                    'hour': '$_id.hour', 
                    'minute': '$_id.minute', 
                    'second': '$_id.second'
                }
            }
        }
    }, {
        '$merge': {
            'into': 'weather_10s', 
            'whenMatched': 'replace', 
            'whenNotMatched': 'insert'
        }
    }
])