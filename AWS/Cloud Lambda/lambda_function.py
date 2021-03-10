import json
import boto3
import time
import os

def matchPersonsAndVests(personsList, vestsList):

    persons = []
    vests = []
    personsWithvests = []

    for person in personsList:
        persons.append(person)
    for vest in vestsList:
        vests.append(vest)

    h = 0
    matched = 0
    totalvests = len(vests)
    while(h < totalvests):
        vest = vests[h-matched]
        totalPersons = len(persons)
        p = 0
        while(p < totalPersons):
            person = persons[p]
            if(not (vest['BoundingBoxCoordinates']['x2'] < person['BoundingBoxCoordinates']['x1']
                or vest['BoundingBoxCoordinates']['x1'] > person['BoundingBoxCoordinates']['x2']
                or vest['BoundingBoxCoordinates']['y4'] < person['BoundingBoxCoordinates']['y1']
                    or vest['BoundingBoxCoordinates']['y1'] > person['BoundingBoxCoordinates']['y4']
                )):

                personsWithvests.append({'Person' : person, 'Vest' : vest})

                del persons[p]
                del vests[h - matched]

                matched = matched + 1

                break
            p = p + 1
        h = h + 1

    return (personsWithvests, persons, vests)

def getBoundingBoxCoordinates(boundingBox, imageWidth, imageHeight):
    x1 = 0
    y1 = 0
    x2 = 0
    y2 = 0
    x3 = 0
    y3 = 0
    x4 = 0
    y4 = 0

    boxWidth = boundingBox['Width']*imageWidth
    boxHeight = boundingBox['Height']*imageHeight

    x1 = boundingBox['Left']*imageWidth
    y1 = boundingBox['Top']*imageWidth

    x2 = x1 + boxWidth
    y2 = y1

    x3 = x2
    y3 = y1 + boxHeight

    x4 = x1
    y4 = y3

    return({'x1': x1, 'y1' : y1, 'x2' : x2, 'y2' : y2, 'x3' : x3, 'y3' : y3, 'x4' : x4, 'y4' : y4})

def getPersonsAndVests(labelsResponse, imageWidth, imageHeight):

    persons = []
    vests = []

    for label in labelsResponse['Labels']:
        if label['Name'] == 'Person' and 'Instances' in label:
            for person in label['Instances']:
                    persons.append({'BoundingBox' : person['BoundingBox'], 'BoundingBoxCoordinates' : getBoundingBoxCoordinates(person['BoundingBox'], imageWidth, imageHeight), 'Confidence' : person['Confidence']})
        elif label['Name'] == 'Vest' and 'Instances' in label:
            for vest in label['Instances']:
                vests.append({'BoundingBox' : vest['BoundingBox'], 'BoundingBoxCoordinates' : getBoundingBoxCoordinates(vest['BoundingBox'], imageWidth, imageHeight), 'Confidence' : vest['Confidence']})

    return (persons, vests)

def detectWorkerSafety(bucketName, imageName, imageWidth, imageHeight):

    rekognition = boto3.client('rekognition', region_name='us-east-1')
    labelsResponse = rekognition.detect_labels(
    Image={
        'S3Object': {
            'Bucket': bucketName,
            'Name': imageName,
        }
    },
    MaxLabels=20,
    MinConfidence=60)

    persons, vests = getPersonsAndVests(labelsResponse, imageWidth, imageHeight)

    return matchPersonsAndvests(persons, vests)

def sendMessageToIoTTopic(iotMessage):
    topicName = "worker-safety"
    if "iot_topic" in os.environ:
        topicName = os.environ['iot_topic']
    iotClient = boto3.client('iot-data', region_name='us-east-1')
    response = iotClient.publish(
            topic=topicName,
            qos=1,
            payload=json.dumps(iotMessage)
        )
    print("Send message to topic: " + topicName)

def pushToCloudWatch(name, value):
    try:
        cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
        response = cloudwatch.put_metric_data(
            Namespace='string',
            MetricData=[
                {
                    'MetricName': name,
                    'Value': value,
                    'Unit': 'Count'
                },
            ]
        )
        #print("Metric pushed: {}".format(response))
    except Exception as e:
        print("Unable to push to cloudwatch\n e: {}".format(e))
        return True

def lambda_handler(event, context):

    bucketName = event['Records'][0]['s3']['bucket']['name']
    imageName = event['Records'][0]['s3']['object']['key']
    scaleFactor = 4
    imageWidth = 2688/scaleFactor
    imageHeight = 1520/scaleFactor

    personsWithVests, personsWithoutVests, vestsWihoutPerson = detectWorkerSafety(bucketName, imageName, imageWidth, imageHeight)

    personsWithVestsCount = len(personsWithVests)
    personsWithoutVestsCount = len(personsWithoutVests)
    vestsWihoutPersonCount = len(vestsWihoutPerson)

    pushToCloudWatch('PersonsWithSafetyVest', personsWithVestsCount)
    pushToCloudWatch('PersonsWithoutSafetyVest', personsWithoutVestsCount)

    outputMessage = "Person(s): {}".format(personsWithVestsCount+personsWithoutVestsCount)
    outputMessage = outputMessage + "\nPerson(s) With Safety vest: {}\nPerson(s) Without Safety vest: {}".format(personsWithVestsCount, personsWithoutVestsCount)
    print(outputMessage)

    imageUrl = "https://s3.amazonaws.com/{}/{}".format(bucketName, imageName)
    iotMessage = {'ImageUrl' :imageUrl, 'PersonsWithVest' : personsWithVests, 'PersonsWithoutVest' : personsWithoutVests, 'Message' : outputMessage}

    sendMessageToIoTTopic(iotMessage)

    return {
        'statusCode': 200,
        'body': json.dumps(outputMessage)
    }

def localTest():
    bucketName = "ki-worker-safety"
    #imageName = "persons/11_11/4_49/1541929754_0.jpg"
    #imageName = "worker-safety/00.jpg"
    imageName = "persons/1541974066_0.jpg"
    #imageName = "persons/yard-work.jpg"

    event = {
    "Records": [
        {
          "s3": {
            "bucket": {
              "name": bucketName,
            },
            "object": {
              "key": imageName,
            }
          }
        }
      ]
    }
    lambda_handler(event, None)

#localTest()
