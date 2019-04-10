"""
Script tool for ArcGIS which geocodes a table of addresses and produces a new table of the results.
"""
from urllib import parse, request, error
import csv
import json
import os
import time
import random
import re
from google.cloud import storage
import logging
import sys
import argparse

VERSION_NUMBER = "4.0.0"
BRANCH = "pro-python-3"
VERSION_CHECK_URL = "https://raw.githubusercontent.com/agrc/geocoding-toolbox/{}/tool-version.json".format(BRANCH)
RATE_LIMIT_SECONDS = (0.015, 0.03)
UNIQUE_RUN = time.strftime("%Y%m%d%H%M%S")
GEOCODE_HOST = 'http://webapi-api/'


def api_retry(api_call):
    """Retry and api call if calling method returns None."""
    def retry(*args, **kwargs):
        response = api_call(*args, **kwargs)
        back_off = 1
        while response is None and back_off <= 8:
            time.sleep(back_off + random.random())
            response = api_call(*args, **kwargs)
            back_off += back_off
        return response
    return retry


@api_retry
def get_version(check_url):
    """Get current version number."""
    try:
        r = request.urlopen(check_url)
        response = json.load(r)
    except:
        return None
    if r.getcode() is 200:
        currentVersion = response['VERSION_NUMBER']
        return currentVersion
    else:
        return None


class Configs(object):
    """Store input and output configs."""

    def __init__(self, apiKey, inputTable, inputBucket, idField, addressField, zoneField, locator, spatialRef, outputDir, outputBucket, outputFileName):
        """Ctor."""
        pass


class Geocoder(object):
    """Geocode and address and check api keys."""

    _api_key = None
    _url_template = GEOCODE_HOST + "api/v1/geocode/{}/{}?{}"

    def __init__(self, api_key, spatialReference, locator):
        """Constructor."""
        self._api_key = api_key
        self._spatialRef = spatialReference
        self._locator = locator

    def _formatJsonData(self, formattedAddresses):
        jsonArray = {"addresses": []}
        for address in formattedAddresses:
            jsonArray["addresses"].append({"id": address.id,
                                           "street": address.address,
                                           "zone": address.zone})

        return jsonArray

    @api_retry
    def isApiKeyValid(self):
        """Check api key against known address."""
        apiCheck_Url = GEOCODE_HOST + "api/v1/geocode/{}/{}?{}"
        params = parse.urlencode({"apiKey": self._api_key})
        url = apiCheck_Url.format(parse.quote("270 E CENTER ST"), "LINDON", params)
        try:
            r = request.urlopen(url)
            response = json.load(r)
        except Exception as e:
            return None

        # check status code
        if r.getcode() >= 500:
            return None
        elif r.getcode() is not 200 or response["status"] is not 200:
            return "Error: " + str(response["message"])
        else:
            return "Api key is valid"

    @api_retry
    def locateAddress(self, formattedAddress):
        """Create URL from formatted address and send to api."""
        apiCheck_Url = GEOCODE_HOST + "api/v1/geocode/{}/{}?{}"
        params = parse.urlencode({"spatialReference": self._spatialRef,
                                  "locators": self._locator,
                                  "apiKey": self._api_key,
                                  "pobox": "true"})
        url = apiCheck_Url.format(parse.quote(formattedAddress.address),
                                  parse.quote(formattedAddress.zone),
                                  params)
        response = None
        try:
            r = request.urlopen(url)
            response = json.load(r)
        except error.HTTPError as httpError:
            if httpError.code >= 500:
                response = None
            elif httpError.code == 404:
                response = json.load(httpError)
        except:
            response = None

        return response


class AddressResult(object):
    """
    Store the results of a single geocode.

    Also contains static methods for writing a list AddressResults to different formats.
    """

    outputFields = ("INID", "INADDR", "INZONE",
                    "MatchAddress", "Zone", "Score",
                    "XCoord", "YCoord", "Geocoder")
    outputFieldTypes = ["TEXT", "TEXT", "TEXT",
                        "TEXT", "TEXT", "FLOAT",
                        "DOUBLE", "DOUBLE", "TEXT"]
    outputTextLengths = [100, 200, 100,
                         200, 100, None,
                         None, None, 50]

    def __init__(self, idValue, inAddr, inZone, matchAddr, zone, score, x, y, geoCoder):
        """ctor."""
        self.id = idValue
        self.inAddress = inAddr
        self.inZone = inZone
        self.matchAddress = matchAddr
        self.zone = zone
        self.score = score
        self.matchX = x
        self.matchY = y
        self.geoCoder = geoCoder

    def __str__(self):
        """str."""
        return "{},{},{},{},{},{},{},{},{}".format(*self.get_fields())

    def get_fields(self):
        """Get fields in output table order."""
        return (self.id, self.inAddress, self.inZone,
                self.matchAddress, self.zone, self.score,
                self.matchX, self.matchY, self.geoCoder)

    def getResultRow(self):
        """Get tuple of fields for InsertCursor."""
        outRow = []
        for f in self.get_fields():
            if f == "":
                outRow.append(None)
            else:
                outRow.append(f)
        return outRow

    @staticmethod
    def addHeaderResultCSV(outputFilePath):
        """Add header to CSV."""
        with open(outputFilePath, "a") as outCSV:
            outCSV.write(",".join(AddressResult.outputFields))

    @staticmethod
    def appendResultCSV(addrResult, outputFilePath):
        """Append a result to CSV."""
        with open(outputFilePath, "a") as outCSV:
            outCSV.write("\n" + str(addrResult))


class AddressFormatter(object):
    """Address formating utility."""
    spaceReplaceMatcher = re.compile(r'(\s\d/\d\s)|/|(\s#.*)|%|(\.\s)|\?')

    def __init__(self, idNum, inAddr, inZone):
        """Ctor."""
        self.id = idNum
        self.address = self._formatAddress(inAddr)
        self.zone = self._formatZone(inZone)

    def _formatAddress(self, inAddr):
        addrString = str(inAddr)

        formattedAddr = AddressFormatter.spaceReplaceMatcher.sub(" ", addrString)

        for c in range(0, 31):
            formattedAddr = formattedAddr.replace(chr(c), " ")
        for c in range(33, 37):
            formattedAddr = formattedAddr.replace(chr(c), " ")

        formattedAddr = formattedAddr.replace(chr(38), "and")

        for c in range(39, 47):
            formattedAddr = formattedAddr.replace(chr(c), " ")
        for c in range(58, 64):
            formattedAddr = formattedAddr.replace(chr(c), " ")
        for c in range(91, 96):
            formattedAddr = formattedAddr.replace(chr(c), " ")
        for c in range(123, 255):
            formattedAddr = formattedAddr.replace(chr(c), " ")

        return formattedAddr

    def _formatZone(self, inZone):
        formattedZone = AddressFormatter.spaceReplaceMatcher.sub(" ", str(inZone)).strip()
        if len(formattedZone) > 0 and formattedZone[0] == "8":
            formattedZone = formattedZone.strip()[:5]

        return formattedZone

    def isValid(self):
        """Test for address validity after formatting."""
        if len(self.address.replace(" ", "")) == 0 or len(self.zone.replace(" ", "")) == 0:
            return False
        elif self.id is None or self.address == 'None' or self.zone == 'None':
            return False
        else:
            return True


class TableGeocoder(object):
    """
    Script tool user interface allows for.

    -table of addresses
    -Fields to use
    -Geocoder paramater
    """

    locatorMap = {"Address points and road centerlines (default)": "all",
                  "Road centerlines": "roadCenterlines",
                  "Address points": "addressPoints"}
    spatialRefMap = {"NAD 1983 UTM Zone 12N": 26912,
                     "NAD 1983 StatePlane Utah North(Meters)": 32142,
                     "NAD 1983 StatePlane Utah Central(Meters)": 32143,
                     "NAD 1983 StatePlane Utah South(Meters)": 32144,
                     "GCS WGS 1984": 4326}

    def __init__(self, apiKey, inputTable, idField, addressField, zoneField, locator, spatialRef, outputDir, outputFileName, outputGeodatabase):
        """ctor."""
        self._apiKey = apiKey
        self._inputTable = inputTable
        self._idField = idField
        self._addressField = addressField
        self._zoneField = zoneField
        self._locator = locator
        self._spatialRef = spatialRef
        self._outputDir = outputDir
        self._outputFileName = outputFileName
        self._outputGdb = outputGeodatabase

    #
    # Helper Functions
    #
    def _HandleCurrentResult(self, addressResult, outputFullPath, outputCursor):
        """Handle appending a geocoded address to the output CSV."""
        currentResult = addressResult
        AddressResult.appendResultCSV(currentResult, outputFullPath)

    def _processMatch(self, coderResponse, formattedAddr, outputFullPath, outputCursor):
        """Handle an address that has been returned by the geocoder."""
        locatorErrorText = "Error: Locator error"
        addressId = formattedAddr.id
        # Locator response Error
        if coderResponse is None:
            log.info("Address ID {} failed".format(addressId))
            # Handle bad response
            currentResult = AddressResult(addressId, "", "", locatorErrorText, "", "", "", "", "")
            self._HandleCurrentResult(currentResult, outputFullPath, outputCursor)
        else:
            if coderResponse["status"] == 404:
                # address not found error
                inputAddress = formattedAddr.address
                inputZone = formattedAddr.zone
                currentResult = AddressResult(addressId, inputAddress, inputZone,
                                              "Error: " + coderResponse["message"], "", "", "", "", "")
                self._HandleCurrentResult(currentResult, outputFullPath, outputCursor)
            # Matched address
            else:
                coderResult = coderResponse["result"]
                #: if address grid in zone remove it
                matchAddress = coderResult["matchAddress"]
                matchZone = coderResult["addressGrid"]

                if ',' in matchAddress:
                    matchAddress = coderResult["matchAddress"].split(",")[0]

                splitInputAddress = coderResult["inputAddress"].split(",")
                inputAddress = splitInputAddress[0]
                inputZone = ""
                if len(splitInputAddress) > 1:
                    inputZone = splitInputAddress[1].strip()
                else:
                    inputZone = ""

                currentResult = AddressResult(addressId, inputAddress, inputZone,
                                              matchAddress, matchZone, coderResult["score"],
                                              coderResult["location"]["x"], coderResult["location"]["y"],
                                              coderResult["locator"])
                self._HandleCurrentResult(currentResult, outputFullPath, outputCursor)

    def start(self):
        """Entery point into geocoding process."""
        outputFullPath = os.path.join(self._outputDir, self._outputFileName)

        geocoder = Geocoder(self._apiKey, self._spatialRef, self._locator)
        # Test api key before we get started
        apiKeyMessage = geocoder.isApiKeyValid()
        if apiKeyMessage is None:
            log.info("Geocode service failed to respond on api key check")
            return
        elif "Error:" in apiKeyMessage:
            log.info(apiKeyMessage)
            return
        else:
            log.info(apiKeyMessage)

        log.info("Begin Geocode")
        AddressResult.addHeaderResultCSV(outputFullPath)
        sequentialBadRequests = 0
        rowNum = 1
        one_k_start = time.time()
        outCursor = None
        with open(self._inputTable) as csvInput:
            reader = csv.DictReader(csvInput)
            for row in reader:
                record = (row[self._idField], row[self._addressField], row[self._zoneField])
                try:
                    inFormattedAddress = AddressFormatter(record[0], record[1], record[2])
                except UnicodeEncodeError:
                    currentResult = AddressResult(record[0], "", "",
                                                  "Error: Unicode special character encountered", "", "", "", "", "")
                    self._HandleCurrentResult(currentResult, outputFullPath, outCursor)

                # Check for major address format problems before sending to api
                if inFormattedAddress.isValid():
                    throttleTime = random.uniform(RATE_LIMIT_SECONDS[0], RATE_LIMIT_SECONDS[1])
                    time.sleep(throttleTime)
                    matchedAddress = geocoder.locateAddress(inFormattedAddress)

                    if matchedAddress is None:
                        sequentialBadRequests += 1
                        if sequentialBadRequests <= 5:
                            currentResult = AddressResult(record[0], inFormattedAddress.address, inFormattedAddress.zone,
                                                          "Error: Geocode failed", "", "", "", "", "")
                            self._HandleCurrentResult(currentResult, outputFullPath, outCursor)
                        else:
                            error_msg = "Geocode Service Failed to respond{}"
                            if rowNum > 1:
                                error_msg = error_msg.format(
                                    "\n{} adresses completed\nCheck: {} for partial table".format(rowNum - 1,
                                                                                                  outputFullPath))
                            else:
                                error_msg = error_msg.format("")
                            log.info(error_msg)

                            return

                        continue

                    self._processMatch(matchedAddress, inFormattedAddress, outputFullPath, outCursor)

                else:
                        currentResult = AddressResult(record[0], inFormattedAddress.address, inFormattedAddress.zone,
                                                      "Error: Address invalid or NULL fields", "", "", "", "", "")
                        self._HandleCurrentResult(currentResult, outputFullPath, outCursor)

                if rowNum % 1000 == 0:
                    one_k_end = time.time() - one_k_start
                    one_k_end = round(one_k_end, 3)
                    log.info('Rows geocoded %d | seconds %f', rowNum, one_k_end)
                    one_k_start = time.time()
                rowNum += 1
                sequentialBadRequests = 0


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        log.info(blob.name)

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def _setup_logging():
    log = logging.getLogger('geocoder')
    log.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter(fmt='%(message)s')
    log.logThreads = 0
    log.logProcesses = 0

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_formatter)
    log.addHandler(console_handler)


if __name__ == "__main__":
    # ex command: python geocode-gcs-csv.py --apikey AGRC-Explorer --input_bucket geocoder-csv-storage-95728 --input_csv GeocodeResults_20180924170752.csv --output_bucket geocoder-csv-results-98576 --no_download --no_upload

    inputTable = './tmp/inputdata.csv'
    locator = TableGeocoder.locatorMap['Address points and road centerlines (default)']
    spatialRef = TableGeocoder.spatialRefMap['NAD 1983 UTM Zone 12N']
    outputDir = r'./tmp'
    outputFileName = "GeocodeResults_" + UNIQUE_RUN + ".csv"

    parser = argparse.ArgumentParser(description='Geocode some addresses')

    parser.add_argument('--apikey', action='store', dest='apikey',
                        help='Api key for geocoding')
    parser.add_argument('--input_bucket', action='store', dest='input_bucket',
                        help='GCS bucket with input data')
    parser.add_argument('--input_csv', action='store', dest='input_csv',
                        help='Name of the CSV in input_bucket')
    parser.add_argument('--id_field', action='store', dest='id_field',
                        help='ID field in the csv. d')
    parser.add_argument('--address_field', action='store', dest='address_field',
                        help='Address field in the csv.')
    parser.add_argument('--zone_field', action='store', dest='zone_field',
                        help='Zone field in the csv.')
    parser.add_argument('--output_bucket', action='store', dest='output_bucket',
                        help='Name of the CSV in input_bucket')
    parser.add_argument('--no_download', action='store_true', dest='no_dl',
                        help='Do not download from GCS. Downloaded data must already be local.')
    parser.add_argument('--no_upload', action='store_true', dest='no_ul',
                        help='Do not upload to GCS.')
    args = parser.parse_args()
    apiKey = args.apikey
    inputBucket = args.input_bucket
    inputCsv = args.input_csv
    idField = args.id_field
    addressField = args.address_field
    zoneField = args.zone_field
    outputBucket = args.output_bucket

    _setup_logging()
    global log
    log = logging.getLogger('geocoder')

    if not args.no_dl:
        download_blob(inputBucket,
                      inputCsv,
                      inputTable)
        log.info('Downloading %s complete', inputCsv)

    outputGeodatabase = None
    version = VERSION_NUMBER
    log.info("Geocode Table Version " + version)
    currentVersion = get_version(VERSION_CHECK_URL)
    if currentVersion and VERSION_NUMBER != currentVersion:
        log.info('Current version is: {}'.format(currentVersion))
        log.info('Please download at: https://github.com/agrc/geocoding-toolbox/raw/{}/AGRC Geocode Tools.tbx'.format(BRANCH))

    Tool = TableGeocoder(apiKey,
                         inputTable,
                         idField,
                         addressField,
                         zoneField,
                         locator,
                         spatialRef,
                         outputDir,
                         outputFileName,
                         outputGeodatabase)
    Tool.start()
    log.info("Geocode completed")

    if not args.no_ul:
        upload_blob(outputBucket,
                    os.path.join(outputDir, outputFileName),
                    outputFileName)
        log.info("Uploading %s complete", outputFileName)

    logging.shutdown()
