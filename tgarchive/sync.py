from io import BytesIO
from sys import exit
import json
import logging
import os
import subprocess
import tempfile
import shutil
import time
import random
import threading
import re

from telethon import TelegramClient, errors, sync
import telethon.tl.types

from  tgarchive.db import Message, Media

Config_mp3Quolity = 7

class Sync:
    """
    Sync iterates and receives messages from the Telegram group to the
    local SQLite DB.
    """
    config = {}
    db = None
    
    lazyMedia = Media(-1, 'lazy-media', '', '', '', '')
    #  lazyMedia = "lazy-media"

    def __init__(self, config, session_file, db):
        self.config = config
        self.db = db

        self.client = self.new_client(session_file, config)

        if not os.path.exists(self.config["media_dir"]):
            os.mkdir(self.config["media_dir"])

    def sync(self, ids=None, from_id=None, randomFactor=None):
        """
        Sync syncs messages from Telegram from the last synced message
        into the local SQLite DB.
        """

        if ids:
            last_id, last_date = (ids, None)
            logging.info("fetching message id={}".format(ids))
        elif from_id:
            last_id, last_date = (from_id, None)
            logging.info("fetching from last message id={}".format(last_id))
        else:
            last_id, last_date = self.db.get_last_message_id()
            logging.info("fetching from last message id={} ({})".format(
                last_id, last_date))

        group_id = self._get_group_id(self.config["group"])

        n = 0
        isDl = 10
        while True:
            has = False
            for m in self._get_messages(group_id,
                                        offset_id=last_id if last_id else 0,
                                        ids=ids):
                if not m:
                    continue
                #  print(m)
                if m.type != "message":
                    continue
                if m.content != '':
                    # print("found title")
                    isDl = 10
                if not m.media:
                    continue

                has = True

                if 0 < isDl and (randomFactor != None and random.randint(0, randomFactor) == 1) and m.media.url == "lazy-media":
                # if 0 < isDl and m.media.url == "lazy-media":
                    #  print("dl media!")
                    logging.info("downloading media #{}".format(m.id))
                    try:
                        basename, fname, thumb = self._download_media(m.media.thumb)
                        m = m._replace(media = Media(
                            id=m.id,
                            type="photo",
                            url=fname,
                            title=basename,
                            description=None,
                            thumb=thumb
                        ))
                        isDl -= 1
                    finally:
                        pass
                    
                if m.media and m.media.url != "lazy-media":
                    self.db.insert_media(m.media)
                
                self.db.insert_message(m)

                last_date = m.date
                n += 1
                if n % 300 == 0:
                    logging.info("fetched {} messages".format(n))
                    self.db.commit()

                if 0 < self.config["fetch_limit"] <= n or ids:
                    has = False
                    break

            self.db.commit()
            if has:
                last_id = m.id
                logging.info("fetched {} messages. sleeping for {} seconds".format(
                    n, self.config["fetch_wait"]))
                time.sleep(self.config["fetch_wait"])
            else:
                break

        self.db.commit()
        if self.config.get("use_takeout", False):
            self.finish_takeout()
        logging.info(
            "finished. fetched {} messages. last message = {}".format(n, last_date))

    def new_client(self, session, config):
        if "proxy" in config and config["proxy"].get("enable"):
            proxy = config["proxy"]
            client = TelegramClient(session, config["api_id"], config["api_hash"], proxy=(proxy["protocol"], proxy["addr"], proxy["port"]))
        else:
            client = TelegramClient(session, config["api_id"], config["api_hash"])
        # hide log messages
        # upstream issue https://github.com/LonamiWebs/Telethon/issues/3840
        client_logger = client._log["telethon.client.downloads"]
        client_logger._info = client_logger.info

        def patched_info(*args, **kwargs):
            if (
                args[0] == "File lives in another DC" or
                args[0] == "Starting direct file download in chunks of %d at %d, stride %d"
            ):
                return client_logger.debug(*args, **kwargs)
            client_logger._info(*args, **kwargs)
        client_logger.info = patched_info

        client.start()
        if config.get("use_takeout", False):
            for retry in range(3):
                try:
                    takeout_client = client.takeout(finalize=True).__enter__()
                    # check if the takeout session gets invalidated
                    takeout_client.get_messages("me")
                    return takeout_client
                except errors.TakeoutInitDelayError as e:
                    logging.info(
                        "please allow the data export request received from Telegram on your device. "
                        "you can also wait for {} seconds.".format(e.seconds))
                    logging.info(
                        "press Enter key after allowing the data export request to continue..")
                    input()
                    logging.info("trying again.. ({})".format(retry + 2))
                except errors.TakeoutInvalidError:
                    logging.info("takeout invalidated. delete the session.session file and try again.")
            else:
                logging.info("could not initiate takeout.")
                raise(Exception("could not initiate takeout."))
        else:
            return client

    def finish_takeout(self):
        self.client.__exit__(None, None, None)

    def _get_messages(self, group, offset_id, ids=None) -> Message:
        messages = self._fetch_messages(group, offset_id, ids)
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message
        for m in messages:
            if not m or not m.sender:
                continue

            # Media.
            sticker = None
            med = None
            if m.media:
                # If it's a sticker, get the alt value (unicode emoji).
                if isinstance(m.media, telethon.tl.types.MessageMediaDocument) and \
                        hasattr(m.media, "document") and \
                        m.media.document.mime_type == "application/x-tgsticker":
                    alt = [a.alt for a in m.media.document.attributes if isinstance(
                        a, telethon.tl.types.DocumentAttributeSticker)]
                    if len(alt) > 0:
                        sticker = alt[0]
                elif isinstance(m.media, telethon.tl.types.MessageMediaPoll):
                    med = None
                else:
                    med = self._get_media(m)
                    # med = self.lazyMedia

            # Message.
            typ = "message"
            if m.action:
                if isinstance(m.action, telethon.tl.types.MessageActionChatAddUser):
                    typ = "user_joined"
                elif isinstance(m.action, telethon.tl.types.MessageActionChatDeleteUser):
                    typ = "user_left"

            yield Message(
                type=typ,
                id=m.id,
                date=m.date,
                edit_date=m.edit_date,
                content=sticker if sticker else m.raw_text,
                reply_to=m.reply_to_msg_id if m.reply_to and m.reply_to.reply_to_msg_id else None,
                user=None,
                media=med
            )

    def _fetch_messages(self, group, offset_id, ids=None) -> Message:
        try:
            if self.config.get("use_takeout", False):
                wait_time = 0
            else:
                wait_time = None
            messages = self.client.get_messages(group, offset_id=offset_id,
                                                limit=self.config["fetch_batch_size"],
                                                wait_time=wait_time,
                                                ids=ids,
                                                reverse=True)
            return messages
        except errors.FloodWaitError as e:
            logging.info(
                "flood waited: have to wait {} seconds".format(e.seconds))

    def _get_media(self, msg):
        if isinstance(msg.media, telethon.tl.types.MessageMediaWebPage) and \
                not isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
            return None
        elif isinstance(msg.media, telethon.tl.types.MessageMediaDocument):
            if self.config["download_media"]:
                # Filter by extensions?
                if len(self.config["media_mime_types"]) > 0:
                    if hasattr(msg, "file") and hasattr(msg.file, "mime_type") and msg.file.mime_type:
                        if msg.file.mime_type not in self.config["media_mime_types"]:
                            logging.info(
                                "skipping media #{} / {}".format(msg.file.name, msg.file.mime_type))
                            return

                #  logging.info("downloading media #{}".format(msg.id))
                try:
                    # basename, fname, thumb = self._download_media(msg)
                    #  basename, fname, thumb = ["","lazy-media", None]
                    basename, fname, thumb = ["", "lazy-media", msg]
                    return Media(
                        id=msg.id,
                        type="photo",
                        url=fname,
                        title=basename,
                        description=None,
                        thumb=thumb
                    )
                except Exception as e:
                    logging.error(
                        "error downloading media: #{}: {}".format(msg.id, e))

    _extractDigitsRe = re.compile('^(\\d+)')
    # [https://stackoverflow.com/a/34193591]
    _unixPathBlacklistChars = re.compile('[' + re.escape('\\/\0*`|;"\':') + ']')
    _idPadDigitsCount = 6
    def _idToStr(self, msgId):
        return str(msgId).zfill(self._idPadDigitsCount)
    def _escapeFileName(self, f):
      # [https://stackoverflow.com/a/34193591]
        if f.startswith("-"):
            f = "_" + f
        return re.sub(self._unixPathBlacklistChars, '_', f)
    def _format_media_name(self, fpath, msgId):
        name = self._idToStr(msgId) + ". " + os.path.basename(fpath)
        metadata = None
        with subprocess.Popen(["ffprobe", "-v", "quiet", "-of", "json", "-show_entries", "format", fpath], stdout=subprocess.PIPE) as proc:
            metadata = json.loads(proc.stdout.read())
            # print(metadata)
            format_data = metadata.get('format', None)
            if format_data is None:
                return name
            tags = format_data.get('tags', None)
            if tags is None:
                return name
            validKeysCount = 0
            for i in tags.keys():
                if i in ['artist', 'album', 'title']:
                    validKeysCount += 1
            if validKeysCount != 3:
                return name
            track = "00"
            trackMatch = self._extractDigitsRe.match(tags.get('track', "00"))
            if not trackMatch is None: 
                track = trackMatch.group(0).zfill(2)
            name = self._idToStr(msgId) + ". " + tags.get('artist', "Unknown").strip() + " - " + tags.get('date', "0000").strip() + " - " + tags.get('album', "Unknown").strip() + " - "  + track.strip() + " - " + tags.get('title', "").strip()
        return name
    def _download_media(self, msg) -> [str, str, str]:
        """
        Download a media / file attached to a message and return its original
        filename, sanitized name on disk, and the thumbnail (if any). 
        """
        newname = "{}".format(msg.id).zfill(6)
        dirnewname_byId = os.path.join(self.config["media_dir"], "by-id", self._idToStr(msg.id))
        #  dirnewname_byName = os.path.join(self.config["media_dir"], "by-name", newname)
        basename = ''
        tname = None

        if not os.path.isfile(dirnewname_byId):
            # Download the media to the temp dir and copy it back as
            # there does not seem to be a way to get the canonical
            # filename before the download.
            fpath = self.client.download_media(msg, file=tempfile.gettempdir())
            basename = os.path.basename(fpath)
            # dirnewname_byName = os.path.join(self.config["media_dir"], "by-name", basename + ".m4a")
            dirnewname_byName_basename = os.path.join(self.config["media_dir"], "by-name", self._escapeFileName(self._format_media_name(fpath, msg.id)))
            dirnewname_byName = dirnewname_byName_basename + ".m4a"
            dirnewname_byName_part = dirnewname_byName_basename + ".part.m4a"
            
            #  fpath = fpath_tg + ".mp3"
            def thread():
                isFail = False
                # p = subprocess.Popen(["ffmpeg", "-y", "-vn", "-i", fpath, "-codec:a", "libmp3lame", "-qscale:a", str(Config_mp3Quolity), dirnewname_byName], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                # p = subprocess.Popen(["ffmpeg", "-y", "-vn", "-i", fpath, "-codec:a", "libmp3lame", "-qscale:a", str(Config_mp3Quolity), dirnewname_byName])
                # p = subprocess.Popen(["ffmpeg", "-y", "-vn", "-i", fpath, '-c:a', 'libfdk_aac', '-vbr', '3', '-movflags', '+faststart', dirnewname_byName], stdout=subprocess.DEVNULL)

                try:
                    p = subprocess.Popen(["ffmpeg", "-y", "-vn", "-hide_banner", "-i", fpath, '-c:a', 'libfdk_aac', '-vbr', '3', '-afterburner', '1', '-profile:a', 'aac_he', dirnewname_byName_part], stdout=subprocess.DEVNULL)
                    p.wait()
                except:
                    isFail = True
                os.remove(fpath)
                if isFail:
                    # TODO report to some log
                    os.remove(dirnewname_byName_part)
                else:
                    os.rename(dirnewname_byName_part, dirnewname_byName)
            x = threading.Thread(target=thread)
            x.start()


            #  newname = "{}.{}".format(msg.id, self._get_file_ext(basename))
            #  shutil.move(fpath, dirnewname_byId)
            #  os.symlink(dirnewname_byId, os.path.join('..', 'by-name', basename))
            #  os.symlink(os.path.relpath(dirnewname_byId, os.path.join(self.config["media_dir"], "by-name")), os.path.join('by-name', basename))
            os.symlink(os.path.relpath(dirnewname_byName, os.path.join(self.config["media_dir"], "by-id")), dirnewname_byId)

        return basename, newname, tname

    def _get_file_ext(self, f) -> str:
        if "." in f:
            e = f.split(".")[-1]
            if len(e) < 6:
                return e

        return ".file"

    def _get_group_id(self, group):
        """
        Syncs the Entity cache and returns the Entity ID for the specified group,
        which can be a str/int for group ID, group name, or a group username.

        The authorized user must be a part of the group.
        """
        # Get all dialogs for the authorized user, which also
        # syncs the entity cache to get latest entities
        # ref: https://docs.telethon.dev/en/latest/concepts/entities.html#getting-entities
        _ = self.client.get_dialogs()

        try:
            # If the passed group is a group ID, extract it.
            group = int(group)
        except ValueError:
            # Not a group ID, we have either a group name or
            # a group username: @group-username
            pass

        try:
            entity = self.client.get_entity(group)
        except ValueError:
            logging.critical("the group: {} does not exist,"
                             " or the authorized user is not a participant!".format(group))
            # This is a critical error, so exit with code: 1
            exit(1)

        return entity.id
