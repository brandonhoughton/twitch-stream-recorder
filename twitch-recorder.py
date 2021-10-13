import concurrent
import datetime
import enum
import getopt
import logging
import os
import subprocess
import sys
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import filelock
import requests

import config
import coloredlogs


class TwitchResponseStatus(enum.Enum):
    ONLINE = 0
    OFFLINE = 1
    NOT_FOUND = 2
    UNAUTHORIZED = 3
    ERROR = 4


class TwitchRecorder:
    def __init__(self,
                 username: Optional[str] = None,
                 logger: Optional[logging.Logger] = None,
                 access_token: Optional[dict] = None):
        # global configuration
        self.ffmpeg_path = "ffmpeg"
        self.disable_ffmpeg = False
        self.refresh = 15
        self.root_path = config.root_path

        # user configuration
        self.username = username if username is not None else config.username
        self.quality = "best"

        # twitch configuration
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self.token_url = "https://id.twitch.tv/oauth2/token?client_id=" + self.client_id + "&client_secret=" \
                         + self.client_secret + "&grant_type=client_credentials"
        self.url = "https://api.twitch.tv/helix/streams"
        self.access_token = access_token if access_token is not None else TwitchRecorder.fetch_access_token()

        # logger
        self.logger = logger

    @staticmethod
    def fetch_access_token():
        client_id = config.client_id
        client_secret = config.client_secret
        token_url = "https://id.twitch.tv/oauth2/token?client_id=" + client_id + "&client_secret=" \
                         + client_secret + "&grant_type=client_credentials"
        token_response = requests.post(token_url, timeout=15)
        token_response.raise_for_status()
        token = token_response.json()
        return token["access_token"]


    def run(self, once = False):
        # path to recorded stream
        recorded_path = os.path.join(self.root_path, "recorded", self.username)
        # path to finished video, errors removed
        processed_path = os.path.join(self.root_path, "processed", self.username)

        # create directory for recordedPath and processedPath if not exist
        if os.path.isdir(recorded_path) is False:
            os.makedirs(recorded_path)
        if os.path.isdir(processed_path) is False:
            os.makedirs(processed_path)

        # make sure the interval to check user availability is not less than 15 seconds
        if self.refresh < 15:
            self.logger.warning("check interval should not be lower than 15 seconds")
            self.refresh = 15
            self.logger.info("system set check interval to 15 seconds")

        # fix videos from previous recording session
        try:
            video_list = [f for f in os.listdir(recorded_path) if os.path.isfile(os.path.join(recorded_path, f))]
            if len(video_list) > 0:
                self.logger.info("processing previously recorded files")
            for f in video_list:
                recorded_filename = os.path.join(recorded_path, f)
                processed_filename = os.path.join(processed_path, f)
                self.process_recorded_file(recorded_filename, processed_filename)
        except Exception as e:
            logging.error(e)

        if once:
            self.logger.debug("checking for %s, recording with %s quality",
                         self.username, self.quality)
            self.check(recorded_path, processed_path)
        else:
            logging.info("checking for %s every %s seconds, recording with %s quality",
                         self.username, self.refresh, self.quality)
            self.loop_check(recorded_path, processed_path)

    def process_recorded_file(self, recorded_filename, processed_filename):
        if self.disable_ffmpeg:
            logging.info("moving: %s", recorded_filename)
            shutil.move(recorded_filename, processed_filename)
        else:
            logging.info("fixing %s", recorded_filename)
            self.ffmpeg_copy_and_fix_errors(recorded_filename, processed_filename)

    def ffmpeg_copy_and_fix_errors(self, recorded_filename, processed_filename):
        try:
            subprocess.call(
                [self.ffmpeg_path, "-err_detect", "ignore_err", "-i", recorded_filename, "-c", "copy",
                 processed_filename])
            os.remove(recorded_filename)
        except Exception as e:
            logging.error(e)

    def check_user(self):
        info = None
        status = TwitchResponseStatus.ERROR
        try:
            headers = {"Client-ID": self.client_id, "Authorization": "Bearer " + self.access_token}
            r = requests.get(self.url + "?user_login=" + self.username, headers=headers, timeout=15)
            r.raise_for_status()
            info = r.json()
            if info is None or not info["data"]:
                status = TwitchResponseStatus.OFFLINE
            else:
                status = TwitchResponseStatus.ONLINE
        except requests.exceptions.RequestException as e:
            if e.response:
                if e.response.status_code == 401:
                    status = TwitchResponseStatus.UNAUTHORIZED
                if e.response.status_code == 404:
                    status = TwitchResponseStatus.NOT_FOUND
        return status, info

    def loop_check(self, recorded_path, processed_path):
        while True:
            self.check(recorded_path, processed_path)
            time.sleep(self.refresh)

    def check(self, recorded_path, processed_path):
        try:
            with filelock.FileLock(lock_file=recorded_path + '.lock', timeout=0.05) as lock:
                status, info = self.check_user()
                if status == TwitchResponseStatus.NOT_FOUND:
                    logging.error("username not found, invalid username or typo")
                    time.sleep(self.refresh)
                elif status == TwitchResponseStatus.ERROR:
                    logging.error("%s unexpected error. will try again in 5 minutes",
                                  datetime.datetime.now().strftime("%Hh%Mm%Ss"))
                    time.sleep(300)
                elif status == TwitchResponseStatus.OFFLINE:
                    logging.info("%s currently offline, checking again in %s seconds", self.username, self.refresh)
                    time.sleep(self.refresh)
                elif status == TwitchResponseStatus.UNAUTHORIZED:
                    logging.warning("unauthorized, will attempt to log back in immediately")
                    self.access_token = TwitchRecorder.fetch_access_token()
                elif status == TwitchResponseStatus.ONLINE:
                    logging.info("%s online, stream recording in session", self.username)

                    channels = info["data"]
                    channel = next(iter(channels), None)
                    filename = self.username + " - " + datetime.datetime.now() \
                        .strftime("%Y-%m-%d %Hh%Mm%Ss") + " - " + channel.get("title") + ".mp4"

                    # clean filename from unnecessary characters
                    filename = "".join(x for x in filename if x.isalnum() or x in [" ", "-", "_", "."])

                    recorded_filename = os.path.join(recorded_path, filename)
                    processed_filename = os.path.join(processed_path, filename)

                    # start streamlink process - lock on recorded_filename.lock
                    self.logger.info("Recording user %s", self.username)
                    subprocess.run(
                        ["streamlink", "--twitch-disable-ads", "twitch.tv/" + self.username, self.quality,
                         "-o", recorded_filename],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE)

                    logging.info("recording stream is done, processing video file")
                    if os.path.exists(recorded_filename) is True:
                        self.process_recorded_file(recorded_filename, processed_filename)
                    else:
                        logging.info("skip fixing, file not found")

                    logging.info("processing is done, going back to checking...")
        except filelock.Timeout:
            return


def main(argv):
    twitch_recorder = TwitchRecorder()
    specified_username = False
    usage_message = "twitch-recorder.py -u <username> -q <quality>"
    logging.basicConfig(filename="twitch-recorder.log", level=logging.DEBUG)
    # logging.getLogger().addHandler(logging.StreamHandler())
    logger = logging.getLogger(__name__)
    coloredlogs.install(level="DEBUG", logger=logger)

    try:
        opts, args = getopt.getopt(argv, "hu:q:l:", ["username=", "quality=", "log=", "logging=", "disable-ffmpeg"])
    except getopt.GetoptError:
        print(usage_message)
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(usage_message)
            sys.exit()
        elif opt in ("-u", "--username"):
            twitch_recorder.username = arg
            specified_username = True
        elif opt in ("-q", "--quality"):
            twitch_recorder.quality = arg
        elif opt in ("-l", "--log", "--logging"):
            logging_level = getattr(logging, arg.upper(), None)
            if not isinstance(logging_level, int):
                raise ValueError("invalid log level: %s" % logging_level)
            logging.basicConfig(level=logging_level)
            logging.info("logging configured to %s", arg.upper())
        elif opt == "--disable-ffmpeg":
            twitch_recorder.disable_ffmpeg = True
            logging.info("ffmpeg disabled")

    if specified_username:
        twitch_recorder.run()
    else:
        num_workers = 4

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            list_of_futures = []
            while True:
                # Block until load is low (ignore fairness for now)
                while len(list_of_futures) > num_workers:
                    future = concurrent.futures.as_completed(list_of_futures).__next__()
                    list_of_futures.remove(future)

                # Update the usernames every time
                with open('streamers.txt') as f:
                    access_token = TwitchRecorder.fetch_access_token()
                    twitch_usernames = [line.rstrip() for line in f if
                                        line.rstrip() is not None and len(line.rstrip()) > 1]
                    recorders = [TwitchRecorder(username=username, logger=logger, access_token=access_token) for username in twitch_usernames]
                    new_futures = [pool.submit(recorder.run, True) for recorder in recorders]

                    list_of_futures.extend(new_futures)



if __name__ == "__main__":
    main(sys.argv[1:])
