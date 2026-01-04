import imaplib
import email
from email.header import decode_header
from typing import List, Tuple
from app.config.loader import settings
from app.utils.logger import logger


class EmailListener:
    def __init__(self):
        self.config = settings.load_yaml("email_listener.yaml")
        self.imap_config = self.config["imap"]
        self.rules = self.config["rules"]

    def connect(self) -> imaplib.IMAP4_SSL:
        try:
            mail = imaplib.IMAP4_SSL(
                self.imap_config["server"], self.imap_config["port"]
            )
            mail.login(self.imap_config["username"], self.imap_config["password"])
            mail.select(self.imap_config["folder"])
            return mail
        except Exception as e:
            logger.critical(f"IMAP Connection failed: {e}")
            raise

    def fetch_unprocessed_ids(self, mail, processed_manager) -> List[bytes]:
        keyword = self.rules["subject_keyword"]
        status, messages = mail.search(None, f'(SUBJECT "{keyword}")')

        if status != "OK":
            return []

        email_ids = messages[0].split()
        valid_ids = []

        # TODO: In a real high-volume system, we would optimize this to not fetch headers for all
        # But for daily reports, this is fine.
        for e_id in email_ids:
            res, msg_data = mail.fetch(e_id, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    msg_id_clean = msg["Message-ID"].strip()

                    if not processed_manager.is_processed(msg_id_clean):
                        valid_ids.append(e_id)

        return valid_ids

    def get_email_content(self, mail, email_id: bytes):
        status, msg_data = mail.fetch(email_id, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                return email.message_from_bytes(response_part[1])
        return None
