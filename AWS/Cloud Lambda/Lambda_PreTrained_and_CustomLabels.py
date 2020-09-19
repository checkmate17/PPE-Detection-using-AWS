""" This file contains lambda function for detecting PPE labels.

Current version supports:
    - multiple label detection
    - custom labels

Label configuration can be done via Config class. The Constant
class contains keys that are not expected to be changed while
there is no need to change internal and external API.

The entry point is the lambda_handler function.
"""


import json
import os
import logging

from collections import defaultdict
from typing import List
from typing import Dict
from typing import Tuple
from typing import Type
from typing import Set
from typing import Any
from typing import Union
from typing import DefaultDict

import boto3


class Config:
    """ This structure represents configuration to be used further. """
    # These are the ways Amazon identifies person.
    PERSON_KEYS: List[str] = ['person', 'human', 'child']
    MAX_LABELS: int = 40  # Limit on the amount of labels returned by Amazon.
    MIN_CONFIDENCE: int = 60  # Confidence threshold for all Amazon returns.

    class GroupsPPE:
        """ Names for the supported groups. """
        HELMET: str = 'helmet'
        VEST: str = 'vest'
        MASK: str = 'mask'
        BOOT: str = 'boot'

    # For custom label detection, custom module should be running.
    TURNED_ON_CUSTOM_MODULES: List[str] = [
        GroupsPPE.MASK
    ]

    # For all custom labels, ARNs are supposed to be specified below.
    CUSTOM_LABEL_ARNS = {
        GroupsPPE.VEST:
            'arn:aws:rekognition:us-east-1:322876129806:project/capstone-aug30'
            '/version/capstone-aug30.2020-08-30T18.31.51/1598826713522',
        GroupsPPE.MASK:
            'arn:aws:rekognition:us-east-1:488302376239:project'
            '/masks-training-aug19/version'
            '/masks-training-aug19.2020-08-19T15.33.34/1597865614999'
    }

    # The dict below follows the structure of first presenting
    # the group name, and then the ways the group can be named
    # in the list.
    TARGET_PPE_GROUP_KEYS: Dict[str, List[str]] = {
            GroupsPPE.HELMET: ['helmet', 'hardhat'],
            GroupsPPE.VEST: ['vest', 'waistcoat', 'safetyjacket'],
            GroupsPPE.MASK: ['mask', 'masks'],
            GroupsPPE.BOOT: ['boot', 'shoe']
    }


class Constants:
    """ This structure contains keys and constant values,
    that are not supposed to be changed.
    """

    class LambdaKeys:
        """ Keys to be used for lambda execution. """

        STATUS_CODE_KEY: str = 'statusCode'
        BODY_KEY: str = 'body'

        class EventKeys:
            """ Event-related keys. """

            RECORDS_KEY: str = 'Records'
            S3_KEY: str = 's3'
            BUCKET_OBJECT_KEY: str = 'bucket'
            BUCKET_NAME_KEY: str = 'name'
            IMAGE_OBJECT_KEY: str = 'object'
            IMAGE_NAME_KEY: str = 'key'

        class MetricNames:
            """ Metric Names at the cloud watch. """

            EQUIPPED_PERSONS_COUNT: str = 'PersonsWithSafetyEquip1'
            UNEQUIPPED_PERSONS_COUNT: str = 'PersonsWithoutSafetyEquip1'

        class LambdaResponseKeys:
            """ Keys for the fields provided in the response. """

            MESSAGE_KEY: str = 'Message'
            IMAGE_URL_KEY: str = 'ImageUrl'
            POPULATION_SIZE_KEY: str = 'PopulationSize'
            UNEQUIPPED_SIZE_KEY: str = 'UnequippedSize'
            EQUIPMENT_PER_PERSON_KEY: str = 'EquipmentPerPerson'

            PERSONAL_IND_KEY: str = 'ind'
            PERSONAL_EQUIPMENT_KEY: str = 'equipment'

            PERSONS_PER_EQUIPMENT_KEY: str = 'PersonsPerEquipment'

        class StatusCodes:
            OK_HTTP_CODE = 200

    class BotoKeys:
        """ Keys to retrieve responses provided by boto3 library. """

        REKOGNITION_CLIENT_NAME: str = 'rekognition'
        IOT_DATA_CLIENT_NAME: str = 'iot-data'
        CLOUDWATCH_CLIENT_NAME: str = 'cloudwatch'
        S3_CLIENT_NAME: str = 's3'

        REGION_NAME: str = 'us-east-1'

        class ImageKeys:
            """ Keys to get image and bucket from S3. """
            S3_OBJECT_KEY: str = 'S3Object'
            BUCKET_NAME_KEY: str = 'Bucket'
            IMAGE_NAME_KEY: str = 'Name'

        class RekognitionResponseKeys:
            """ Keys to get label arrays from Rekognition. """
            GENERAL_LABELS: str = 'Labels'
            CUSTOM_LABELS: str = 'CustomLabels'

        class LabelKeys:
            """ Keys to retrieve name and boudning box. """
            LABEL_NAME_KEY: str = 'Name'
            INSTANCES_KEY: str = 'Instances'
            BOUNDING_BOX_KEY: str = 'BoundingBox'
            GEOMETRY_KEY: str = 'Geometry'

        class BoundingBoxKeys:
            """ Keys to retrieve coordinates for bounding box. """
            WIDTH_KEY = 'Width'
            HEIGHT_KEY = 'Height'
            LEFT_KEY = 'Left'
            TOP_KEY = 'Top'

        class CloudWatchKeys:
            """ Keys to work with CloudWatch. """
            VALUE_KEY = 'Value'
            METRIC_NAME_KEY = 'MetricName'
            UNIT_KEY = 'Unit'

            COUNT_FIELD = 'Count'
            STRING_NAMESPACE_FIELD = 'string'

        class IotKeys:
            """ Keys to work with Iot. """
            TOPIC_NAME = 'worker-safety'
            ENVIRONMENT_VARIABLE = 'iot_topic'

        class S3ClientKeys:
            """ Keys for providing Rekognition an image from S3. """
            GET_OBJECT_URL_KEY = 'get_object'
            BUCKET_PARAM_KEY = 'Bucket'
            IMAGE_PARAM_KEY = 'Key'


# Class naming for easier further use.
BotoKeys: Type[Constants.BotoKeys] = Constants.BotoKeys
S3ClientKeys: Type[BotoKeys.S3ClientKeys] = BotoKeys.S3ClientKeys
BoundingBoxKeys: Type[BotoKeys.BoundingBoxKeys] = BotoKeys.BoundingBoxKeys
RekognitionResponseKeys: Type[BotoKeys.RekognitionResponseKeys]
RekognitionResponseKeys = BotoKeys.RekognitionResponseKeys
ImageKeys: Type[BotoKeys.ImageKeys] = BotoKeys.ImageKeys
LabelKeys: Type[BotoKeys.LabelKeys] = BotoKeys.LabelKeys
CloudWatchKeys: Type[BotoKeys.CloudWatchKeys] = BotoKeys.CloudWatchKeys
IotKeys: Type[BotoKeys.IotKeys] = BotoKeys.IotKeys

LambdaKeys: Type[Constants.LambdaKeys] = Constants.LambdaKeys
MetricNames: Type[LambdaKeys.MetricNames] = LambdaKeys.MetricNames
EventKeys: Type[LambdaKeys.EventKeys] = LambdaKeys.EventKeys
StatusCodes: Type[LambdaKeys.StatusCodes] = LambdaKeys.StatusCodes
LambdaResponseKeys: Type[LambdaKeys.LambdaResponseKeys]
LambdaResponseKeys = LambdaKeys.LambdaResponseKeys


def lambda_handler(event: dict, context: Any) -> Dict[str, Any]:
    """ Entry point: lambda execution begins here.  """
    setup_logging()
    logging.debug("Starting lambda handler. Invoked Version: {}.".format(
        context.function_version))

    key_entry: str
    for key_entry in list(Config.TARGET_PPE_GROUP_KEYS.keys()):
        if key_entry in Config.CUSTOM_LABEL_ARNS:
            if key_entry not in Config.TURNED_ON_CUSTOM_MODULES:
                del Config.TARGET_PPE_GROUP_KEYS[key_entry]

    s3_storage = event[EventKeys.RECORDS_KEY][0][EventKeys.S3_KEY]
    bucket_name: str = s3_storage[EventKeys.BUCKET_OBJECT_KEY][
            EventKeys.BUCKET_NAME_KEY]
    image_name: str = s3_storage[EventKeys.IMAGE_OBJECT_KEY][
            EventKeys.IMAGE_NAME_KEY]

    stats: Stats = build_stats(bucket_name, image_name)

    push_to_cloud_watch(
            MetricNames.EQUIPPED_PERSONS_COUNT,
            stats.equipped_size())
    push_to_cloud_watch(
            MetricNames.UNEQUIPPED_PERSONS_COUNT,
            stats.unequipped_size())

    output_message: str = stats.prepare_message()
    logging.info(output_message)

    s3_client: Any = boto3.client(
        BotoKeys.S3_CLIENT_NAME,
        region_name=BotoKeys.REGION_NAME)
    image_url: str = s3_client.generate_presigned_url(
            S3ClientKeys.GET_OBJECT_URL_KEY,
            Params={
                S3ClientKeys.BUCKET_PARAM_KEY: bucket_name,
                S3ClientKeys.IMAGE_PARAM_KEY: image_name})
    iot_message: dict = stats.prepare_response(image_url)
    send_message_to_iot_topic(iot_message)

    logging.debug("Lambda Handler job is over.")
    return {
        LambdaKeys.STATUS_CODE_KEY: StatusCodes.OK_HTTP_CODE,
        LambdaKeys.BODY_KEY: json.dumps(output_message)
    }


def setup_logging() -> None:
    """ Setups logging Python library for use with Amazon. """

    logger: logging.Logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    default_handler: logging.Handler
    for default_handler in logger.handlers:
        logger.removeHandler(default_handler)

    handler: logging.Handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    formatter: logging.Formatter
    formatter = logging.Formatter('%(levelname)s - %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)


class Interval:
    """ Represents 1-dimensional segments.

    Attributes:
        min: where interval begins, left side
        max: where interval ends, right side
    """

    def __init__(self) -> None:
        self.min: float = 0.
        self.max: float = 0.


class BoundingBox:
    """ Represents bounding box as horizontal
    and vertical intervals.

    Attributes:
        x_interval: Projection to the x-axis.
        y_interval: Projection to the y-axis.
    """

    def __init__(self) -> None:
        self.x_interval: Interval = Interval()
        self.y_interval: Interval = Interval()


def make_bounding_box(box_data: Dict[str, float]) -> BoundingBox:
    """ Makes bounding box from the Amazon response.

    Arguments:
        box_data: dictionary in the format returning by Rekognition,
            containing box width, box height, and the top left corner
            coordinates.

    Returns:
        Filled and ready-to-use bounding box.
    """

    box: BoundingBox = BoundingBox()

    width: float; height: float; left: float
    right: float; top: float; bottom: float

    width = box_data[BoundingBoxKeys.WIDTH_KEY]
    height = box_data[BoundingBoxKeys.HEIGHT_KEY]
    left = box_data[BoundingBoxKeys.LEFT_KEY]
    top = box_data[BoundingBoxKeys.TOP_KEY]

    right, bottom = left + width, top + height

    box.x_interval.min = min(left, right)
    box.x_interval.max = max(left, right)
    box.y_interval.min = min(top, bottom)
    box.y_interval.max = max(top, bottom)

    return box


def if_intervals_intersect(lhs: Interval, rhs: Interval) -> bool:
    """ Checks if two intervals intersect. No assumption
    on the interval relative order.

    Arguments:
        lhs: first interval
        rhs: second interval

    Returns:
        True if intervals intersect, False otherwise
    """

    if lhs.min > rhs.min:
        lhs, rhs = rhs, lhs
    return rhs.min < lhs.max


def if_boxes_intersect(lhs_box: BoundingBox, rhs_box: BoundingBox) -> bool:
    """ Checks if two boxes intersect. Two 2D rectangles intersect
    iff their projections on both axes intersect. No assumption on
    the bounding box relative positions.

    Arguments:
        lhs_box: first box
        rhs_box: second box

    Returns:
        True if bounding boxes intersect, False otherwise.
    """

    if_x_intersect: bool = if_intervals_intersect(lhs_box.x_interval, rhs_box.x_interval)
    if_y_intersect: bool = if_intervals_intersect(lhs_box.y_interval, rhs_box.y_interval)
    return if_x_intersect and if_y_intersect


class Person:
    """ Represents a person, storing its
    index, bounding box and equipment.

    Attributes:
        equipment: set with equipment group names
        ind: relative index of a person, does not mean
            or imply any position in the image (i.e. person
            with index 0 can be between persons with indices
            1 and 2 on the image with 3 persons)
        box: bounding box, if any
    """

    def __init__(self) -> None:
        self.equipment: Set[str] = set()
        self.ind: Union[int, None] = None
        self.box: Union[BoundingBox, None] = None


class Stats:
    """ Structure to store processed data and to
    provide answers to a frequent queries, like
    the population size, or who wears what type
    of equipment.

    Attributes:
        population: list of persons found at the image
        equipment2person: lists all persons wearing
            particular key equipment group

    """
    def __init__(self, population: List[Person]):
        self.population: List[Person] = list()
        self.equipment2person: Dict[str, List[Person]] = defaultdict(list)
        person: Person
        for person in population:
            self._add_person(person)

    def population_size(self) -> int:
        """ Returns amount of persons detected.  """
        return len(self.population)

    def unequipped_size(self) -> int:
        """ Returns amount of unequipped persons."""
        return sum(
                [1 for entry in self.population if len(entry.equipment) == 0])

    def equipped_size(self) -> int:
        """ Returns amount of equipped population members. """
        return self.population_size() - self.unequipped_size()

    def prepare_message(self) -> str:
        """ Prepares and returns message with structure summary. """
        message: str = "Report: \nFound {} persons.\n".format(self.population_size())
        message += "Without equipment: {}.\n".format(self.unequipped_size())
        equipment_group: str
        for equipment_group in Config.TARGET_PPE_GROUP_KEYS:
            message += "Persons with {}: {}.\n".format(
                    equipment_group, len(self.equipment2person[equipment_group]))
        idx: int; person: Person
        for idx, person in enumerate(self.population):
            if len(person.equipment) > 0:
                message += ("Person {} wears: " + ", ".join(
                    person.equipment) + ".\n").format(idx)
            else:
                message += "Person {} is unequipped.\n".format(idx)
        return message

    def prepare_response(self, presigned_url: str) -> Dict[str, Any]:
        """ Prepares and returns summary as a JSON-serializable object.
        Arguments:
            presigned_url: URL prepared by s3_client.generate_presigned_url()

        """
        response: Dict[str, Any] = dict()
        response[LambdaResponseKeys.MESSAGE_KEY] = self.prepare_message()
        response[LambdaResponseKeys.IMAGE_URL_KEY] = presigned_url
        response[
            LambdaResponseKeys.POPULATION_SIZE_KEY] = self.population_size()
        response[
            LambdaResponseKeys.UNEQUIPPED_SIZE_KEY] = self.unequipped_size()
        response[LambdaResponseKeys.EQUIPMENT_PER_PERSON_KEY] = [
            {
                LambdaResponseKeys.PERSONAL_IND_KEY: person.ind,
                LambdaResponseKeys.PERSONAL_EQUIPMENT_KEY: list(
                    person.equipment)
            }
            for person in self.population
        ]
        response[LambdaResponseKeys.PERSONS_PER_EQUIPMENT_KEY] = dict()
        key: str
        for key in Config.TARGET_PPE_GROUP_KEYS:
            response[LambdaResponseKeys.PERSONS_PER_EQUIPMENT_KEY][key] = [
                    str(person.ind) for person in self.equipment2person[key]]
        return response

    def _add_person(self, person: Person) -> None:
        """ Auxiliary function to add a person to the structure.
        Assigns ind attribute for each person.

        Arguments:
            person: Instance of Person with filled
                equipment and box fields.
        """

        person.ind = len(self.population)
        self.population.append(person)
        key: str
        for key in person.equipment:
            self.equipment2person[key].append(person)


def build_stats(bucket_name: str, image_name: str) -> Stats:
    """ Sends request to the API, gets bounding boxes and makes
    data structure to response queries. Equipment is assigned to
    person if their bounding boxes intersect.

    Arguments:
        bucket_name: The name of the bucket invoked, retrieved from
            the event triggering lambda.
        image_name: The name of the processed image, retrieved from
            the event triggering lambda.

    Returns:
        Filled statistics structure, having information on what type
        of equipment each detected person wears.

    """
    logging.debug("build_stats: Execution Started.")
    rekognition: Any = boto3.client(
        BotoKeys.REKOGNITION_CLIENT_NAME,
        region_name=BotoKeys.REGION_NAME)
    image_description: Dict[str, Dict[str, Any]] = {
        ImageKeys.S3_OBJECT_KEY: {
            ImageKeys.BUCKET_NAME_KEY: bucket_name,
            ImageKeys.IMAGE_NAME_KEY: image_name,
        }
    }

    # Processing general labels.
    logging.debug("build_stats: Processing general labels.")
    general_response: Dict[str, Any] = rekognition.detect_labels(
        Image=image_description,
        MaxLabels=Config.MAX_LABELS,
        MinConfidence=Config.MIN_CONFIDENCE
    )
    general_response_labels: List[Dict[str, Any]] = general_response[
            RekognitionResponseKeys.GENERAL_LABELS]

    equipment_boxes: DefaultDict[str, List[BoundingBox]]
    population: List[Person]
    population, equipment_boxes = parse_general_objects(general_response_labels)
    logging.debug("build_stats: General labels are processed.")

    # Processing custom labels.
    logging.debug("build_stats: Processing custom labels.")
    custom_response_labels: List[Dict[str, Any]] = list()
    group: str; arn_key: str
    for group, arn_key in Config.CUSTOM_LABEL_ARNS.items():
        if group not in Config.TURNED_ON_CUSTOM_MODULES:
            logging.debug(
                "build_stats: Skip group {} since not ON.".format(group))
            continue
        logging.debug(
            "build_stats: Detect for custom group: {}.".format(group))
        logging.debug("build_stats: ARN = {}".format(arn_key))
        response: Dict[str, Any] = rekognition.detect_custom_labels(
            Image=image_description,
            MinConfidence=Config.MIN_CONFIDENCE,
            ProjectVersionArn=arn_key
        )
        group_custom_labels: List[Dict[str, Any]] = response[
            RekognitionResponseKeys.CUSTOM_LABELS]
        custom_response_labels += group_custom_labels
        logging.debug(
            "build_stats: Custom group {} response obtained.".format(group))
    parse_custom_objects(custom_response_labels, equipment_boxes)
    logging.debug("build_stats: Custom labels are processed.")

    # Assigning equipment to person if bounding boxes intersect.
    logging.debug("build_stats: Matching population and equipment.")
    person: Person; group_boxes: List[BoundingBox]; box: BoundingBox
    for group_name, group_boxes in equipment_boxes.items():
        for box in group_boxes:
            intersected_count: int = 0
            for person in population:
                if if_boxes_intersect(person.box, box):
                    person.equipment.add(group_name)
                    intersected_count += 1
            if intersected_count > 1:
                logging.warning(
                    "build_stats: One {} assigned to {} persons.".format(
                        group_name, intersected_count))
    logging.debug("build_stats: Population and equipment matched.")

    stats: Stats = Stats(population)
    logging.debug("build_stats: Execution finished.")
    return stats


def parse_general_objects(
        response_labels: List[Dict[str, Any]]
        ) -> Tuple[List[Person], DefaultDict[str, List[BoundingBox]]]:
    """ Parses objects in the response labels provided
    by rekognition API.

    Arguments:
        response_labels: List of detected objects by Rekognition
            general label API. Relevant description link:
            https://docs.aws.amazon.com/rekognition/latest/dg/API_DetectLabels.html

    Returns:
        List of detected persons, and map from group name to
        detected group member bounding boxes.
    """

    logging.debug("parse_general_objects: Starting.")
    population: List[Person] = list()
    equipment_boxes: DefaultDict[str, List[BoundingBox]] = defaultdict(list)

    label: Dict[str, Any]; entry: Dict[str, Any]
    for label in response_labels:
        if LabelKeys.INSTANCES_KEY in label:
            if label[LabelKeys.LABEL_NAME_KEY].lower() in Config.PERSON_KEYS:
                for entry in label[LabelKeys.INSTANCES_KEY]:
                    if LabelKeys.BOUNDING_BOX_KEY in entry:
                        logging.debug("parse_general_objects: Person Detected!")
                        person: Person = Person()
                        box_data: Dict[str, Any] = entry[
                                LabelKeys.BOUNDING_BOX_KEY]
                        box: BoundingBox = make_bounding_box(box_data)
                        person.box = box
                        population.append(person)
            else:
                group: str; names: List[str]
                for group, names in Config.TARGET_PPE_GROUP_KEYS.items():
                    if label[LabelKeys.LABEL_NAME_KEY].lower() not in names:
                        continue
                    for entry in label[LabelKeys.INSTANCES_KEY]:
                        if LabelKeys.BOUNDING_BOX_KEY in entry:
                            box_data: Dict[str, Any] = entry[
                                    LabelKeys.BOUNDING_BOX_KEY]
                            box: BoundingBox = make_bounding_box(box_data)
                            equipment_boxes[group].append(box)
    logging.debug("parse_general_objects: Finish.")
    return population, equipment_boxes


def parse_custom_objects(
        response_labels: List[Dict[str, Any]],
        equipment_boxes: DefaultDict[str, List[BoundingBox]]) -> None:
    """ Processes objects in the output provided by the custom
    labels rekognition API.
    All relevant detections are written back to equipment_boxes.

    Arguments:
        response_labels: List of detected objects by Rekognition
            custom label API. Relevant description link:
            https://docs.aws.amazon.com/rekognition/latest/dg/API_DetectCustomLabels.html

        equipment_boxes: Structure provided as an output after
            processing general labels. Maps group name to a list
            with bounding boxes of all detected items from the
            group. The result of the execution is written to
            this structure back.
    """

    entry: Dict[str, Any]
    for entry in response_labels:
        group: str; names: List[str]
        for group, names in Config.TARGET_PPE_GROUP_KEYS.items():
            if entry[LabelKeys.LABEL_NAME_KEY].lower() not in names:
                continue
            if LabelKeys.GEOMETRY_KEY in entry:
                geometry: Dict[str, Any] = entry[LabelKeys.GEOMETRY_KEY]
                if LabelKeys.BOUNDING_BOX_KEY in geometry:
                    box_data: Dict[str, Any] = geometry[
                        LabelKeys.BOUNDING_BOX_KEY]
                    box: BoundingBox = make_bounding_box(box_data)
                    equipment_boxes[group].append(box)


def send_message_to_iot_topic(iot_message: Dict[str, Any]) -> None:
    """ Sends message to iot topic.

    Arguments:
        iot_message: Any message for Iot.
    """
    topic_name: str = IotKeys.TOPIC_NAME
    if IotKeys.ENVIRONMENT_VARIABLE in os.environ:
        topic_name: str = os.environ[
                IotKeys.ENVIRONMENT_VARIABLE]
    iot_client: Any = boto3.client(
            BotoKeys.IOT_DATA_CLIENT_NAME,
            region_name=BotoKeys.REGION_NAME)
    iot_client.publish(
            topic=topic_name, qos=1,
            payload=json.dumps(iot_message)
        )
    logging.info("Send message to topic: " + topic_name)


def push_to_cloud_watch(name: str, value: Any) -> None:
    """ Pushes key-value pair to cloud watch.

    Arguments:
        name: Name of the metric.
        value: Any JSON-serializable object.

    """
    cloudwatch: Any = boto3.client(
        BotoKeys.CLOUDWATCH_CLIENT_NAME,
        region_name=BotoKeys.REGION_NAME)

    try:
        cloudwatch.put_metric_data(
            Namespace=CloudWatchKeys.STRING_NAMESPACE_FIELD,
            MetricData=[
                {
                    CloudWatchKeys.METRIC_NAME_KEY: name,
                    CloudWatchKeys.VALUE_KEY: value,
                    CloudWatchKeys.UNIT_KEY: CloudWatchKeys.COUNT_FIELD
                },
            ]
        )
    except Exception as exception:
        logging.error(
            "Unable to push to cloudwatch\n e: {}".format(exception))
