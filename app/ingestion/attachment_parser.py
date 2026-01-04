import os
from pathlib import Path
from email.message import Message
from app.config.loader import settings
from app.utils.logger import logger


class AttachmentParser:
    def __init__(self):
        config = settings.load_yaml("email_listener.yaml")
        self.save_dir = Path(config["rules"]["save_dir"])
        self.allowed_ext = config["rules"]["allowed_extensions"]

    def extract_csv(self, email_msg: Message) -> str:

        self.save_dir.mkdir(parents=True, exist_ok=True)

        for part in email_msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get("Content-Disposition") is None:
                continue

            filename = part.get_filename()
            if not filename:
                continue

            # validation : extension
            ext = Path(filename).suffix.lower()
            if ext not in self.allowed_ext:
                continue

            # save the File
            filepath = self.save_dir / filename
            with open(filepath, "wb") as f:
                f.write(part.get_payload(decode=True))

            logger.info(f"Extracted attachment: {filename}")

            if os.path.getsize(filepath) == 0:
                logger.warning(f"File {filename} is empty. Skipping.")
                os.remove(filepath)
                continue

            return str(filepath)

        return None
