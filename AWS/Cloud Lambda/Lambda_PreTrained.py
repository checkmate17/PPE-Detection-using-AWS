import json
import time
import os

from collections import defaultdict
from typing import List, Dict, Tuple, Set

import boto3


class Config:
    """ This class represents configuration to be used further. """
    # Rekognition will look for any of these words if they belong to its catalogue
    PERSON_KEYS: List[str] = [ 'Person', 'Human']

    # The same applies to potential PPEs, whose words are grouped
    # in the generic PPE name
    TARGET_PPE_GROUP_KEYS: Dict[str, List[str]] = {
            'helmet' : [ 'Helmet', 'Hardhat'],
            #'vest'   : [ 'Vest', 'Waistcoat', 'Safetyjacket'],
            #The vest label is not recognized as much as the other PPEs. Discarded.
            'boot'   : [ 'Boot', 'Shoe' ]
    }

    MAX_LABELS: int = 30 # This can be changed
    MIN_CONFIDENCE: int = 70 # It has work well after testing

#A Lambda handler function processes events
# It is ran by the runtime
#Once it retunrs an answer, it will be available to handle another event
def lambda_handler(event: dict, context: object) -> dict:

    bucket_name: str = event['Records'][0]['s3']['bucket']['name']
    image_name: str = event['Records'][0]['s3']['object']['key']

    stats: Stats = build_stats(bucket_name, image_name)

    push_to_cloud_watch(
            'PersonsWithSafetyEquip1', stats.equipped_size())
    push_to_cloud_watch(
            'PersonsWithoutSafetyEquip1', stats.unequipped_size())

    output_message: str = stats.prepare_message()
    print(output_message)

    s3_client: object = boto3.client('s3')
    image_url: str = s3_client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket_name, 'Key': image_name })
    iot_message: dict = stats.prepare_response(image_url)
    send_message_to_iot_topic(iot_message)

    return {
        'statusCode': 200,
        'body': json.dumps(output_message)
    }


class Interval:
    """ Represents 1-dimensional segments. """

    def __init__(self):
        self.min: int = 0
        self.max: int = 0


class BoundingBox:
    """ Represents bounding box as horizontal
    and vertical intervals.
    """

    def __init__(self):
        self.x_interval: Interval = Interval()
        self.y_interval: Interval = Interval()


def make_bounding_box(box_data: dict) -> BoundingBox:
    """ It creates a bounding box based on above classes """

    box: BoundingBox = BoundingBox()

    width: float; height: float; left: float;
    right: float; top: float; bottom: float

    width, height = box_data['Width'], box_data['Height']
    left, top = box_data['Left'], box_data['Top']
    right, bottom = left + width, top + height

    box.x_interval.min = min(left, right)
    box.x_interval.max = max(left, right)
    box.y_interval.min = min(top, bottom)
    box.y_interval.max = max(top, bottom)

    return box


def if_intervals_intersect(lhs: Interval, rhs: Interval) -> bool:
    """ Checks if two intervals intersect. """
    if lhs.min > rhs.min:
        lhs, rhs = rhs, lhs
    return rhs.min < lhs.max


def if_boxes_intersect(lhs_box: BoundingBox, rhs_box: BoundingBox) -> bool:
    """ Checks if two boxes intersect. """
    if_x_intersect: bool = if_intervals_intersect(lhs_box.x_interval, rhs_box.x_interval)
    if_y_intersect: bool = if_intervals_intersect(lhs_box.y_interval, rhs_box.y_interval)
    return if_x_intersect and if_y_intersect


class Person:
    """ This class stores a person's
    index, bounding box, equipment and confidence. """

    def __init__(self):
        self.equipment: Set[str] = set()
        self.ind: int = None
        self.box: BoundingBox = None
        self.confidence: float = None


class Stats:
    """ This class provides population size,
    who is wearing a specific type of PPE
    and who is unequipped
    """
    def __init__(self, population: List[Person]):
        self.population: List[Person] = list()
        self.equipment2person: Dict[str, List[Person]] = defaultdict(list)
        person: Person
        for person in population:
            self._add_person(person)

    def population_size(self) -> int:
        """ Amount of people detected"""
        return len(self.population)

    def unequipped_size(self) -> int:
        """ Amount of unequipped people detected"""
        return sum([1 for entry in self.population if len(entry.equipment) == 0])

    def equipped_size(self) -> int:
        """ Amount of equipped people detected. """
        return self.population_size() - self.unequipped_size()

    def prepare_message(self) -> str:
        """ It outputs all the results from an image"""
        response: str = "Found {} persons.\n".format(self.population_size())
        response += "Without equipment: {}.\n".format(self.unequipped_size())
        equipment_group: str
        for equipment_group in Config.TARGET_PPE_GROUP_KEYS:
            response += "Persons with {}: {}.\n".format(
                    equipment_group, len(self.equipment2person[equipment_group]))
        idx: int; person: Person
        for idx, person in enumerate(self.population):
            if len(person.equipment) > 0:
                response += ("Person {} wears: " + ", ".join(person.equipment) + ".\n").format(idx)
            else:
                response += "Person {} is unequipped.\n".format(idx)
        return response

    def prepare_response(self, presigned_url: str) -> dict():
        """ Transforms summary into JSON """
        result: dict = {}
        result['Message'] = self.prepare_message()
        result['ImageUrl'] = presigned_url
        result['PopulationSize'] = self.population_size()
        result['UnequippedSize'] = self.unequipped_size()
        result['EquipmentPerPerson'] = [
            {'ind' : person.ind, 'equipment' : list(person.equipment)}
            for person in self.population
        ]
        result['PersonsPerEquipment'] = {}
        key: str
        for key in Config.TARGET_PPE_GROUP_KEYS:
            result['PersonsPerEquipment'][key] = [str(person.ind) for person in self.equipment2person[key]]
        return result

    def _add_person(self, person: Person) -> None:
        # Auxiliary function to add a person to the structure.
        person.ind = len(self.population)
        self.population.append(person)
        key: str
        for key in person.equipment:
            self.equipment2person[key].append(person)


def build_stats(bucket_name: str, image_name: str) -> Stats:
    """ Send request to the API and get bounding boxes
    """
    rekognition: object = boto3.client('rekognition', region_name='us-east-1')
    response_labels: list = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_name,
            }
        },
        MaxLabels=Config.MAX_LABELS,
        MinConfidence=Config.MIN_CONFIDENCE
    )

    population: List[Person]
    equipment_boxes: Dict[str, List[BoundingBox]]
    population, equipment_boxes = parse_objects(response_labels)

    person: Person
    for person in population:
        group: str
        boxes: List[BoundingBox]
        for group, boxes in equipment_boxes.items():
            for box in boxes:
                if if_boxes_intersect(person.box, box):
                    person.equipment.add(group)

    stats: Stats = Stats(population)
    return stats


def parse_objects(response_labels: dict) -> Tuple[List[Person], Dict[str, List[BoundingBox]]]:
    """ Parses objects in the response labels provided
    by rekognition API.
    """
    population: List[Person] = list()
    equipment_boxes: List[BoundingBox] = defaultdict(list)

    label: dict
    for label in response_labels['Labels']:
        if 'Instances' in label:
            if label['Name'] in Config.PERSON_KEYS:
                entry: dict
                for entry in label['Instances']:
                    if 'BoundingBox' in entry and 'Confidence' in entry:
                        person: Person = Person()
                        confidence: float = entry['Confidence']
                        box_data: dict = entry['BoundingBox']
                        box: BoundingBox = make_bounding_box(box_data)
                        person.box = box
                        person.confidence = confidence
                        population.append(person)
            else:
                group: str
                names: List[str]
                for group, names in Config.TARGET_PPE_GROUP_KEYS.items():
                    entry: dict
                    if label['Name'] not in names:
                        continue
                    for entry in label['Instances']:
                        if 'BoundingBox' in entry and 'Confidence' in entry:
                            box_data: dict = entry['BoundingBox']
                            box: BoundingBox = make_bounding_box(box_data)
                            confidence: float = entry['Confidence']
                            equipment_boxes[group].append(box)
    return population, equipment_boxes


def send_message_to_iot_topic(iot_message: str) -> None:
    """ Sends message to IoT topic. """
    topic_name: str = "worker-safety"
    if "iot_topic" in os.environ:
        topic_name: str = os.environ['iot_topic']
    iot_client: object = boto3.client('iot-data', region_name='us-east-1')
    response: object = iot_client.publish(
            topic=topic_name,
            qos=1,
            payload=json.dumps(iot_message)
        )
    print("Send message to topic: " + topic_name)


def push_to_cloud_watch(name: object, value: object) -> None:
    """ Pushes key-value pairs to CloudWatch. """
    try:
        cloudwatch: object = boto3.client('cloudwatch', region_name='us-east-1')
        response: object = cloudwatch.put_metric_data(
            Namespace='string',
            MetricData=[
                {
                    'MetricName': name,
                    'Value': value,
                    'Unit': 'Count'
                },
            ]
        )
    except Exception as e:
        print("Unable to push to cloudwatch\n e: {}".format(e))
        return True
