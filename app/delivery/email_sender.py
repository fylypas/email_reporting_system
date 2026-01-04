import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.config.loader import settings
from app.utils.logger import logger


class EmailSender:
    def __init__(self):
        self.config = settings.load_yaml("email.yaml")
        self.smtp_config = self.config["smtp"]
        self.sender_config = self.config["sender"]

    def send(self, recipient: str, subject: str, html_body: str, dry_run: bool = False):
        if dry_run:
            logger.info(f"[DRY RUN] Would send to {recipient}")
            return

        msg = MIMEMultipart()
        msg["From"] = self.sender_config["email"]
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(
                self.smtp_config["server"], self.smtp_config["port"]
            ) as server:
                server.starttls()
                server.login(self.smtp_config["username"], self.smtp_config["password"])
                server.send_message(msg)

            logger.info(f"Email sent successfully to {recipient}")
        except Exception as e:
            logger.error(f"Failed to send email to {recipient}: {e}")
