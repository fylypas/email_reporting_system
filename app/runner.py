import time
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict
from app.config.loader import settings
from app.utils.logger import logger
from app.state.processed_emails import ProcessedStateManager
from app.ingestion.email_listener import EmailListener
from app.ingestion.attachment_parser import AttachmentParser
from app.ingestion.csv_loader import CsvLoader
from app.validation.schema_validator import SchemaValidator
from app.segmentation.platform_splitter import PlatformSplitter
from app.aggregation.aggregator import Aggregator
from app.rendering.html_renderer import HtmlRenderer
from app.delivery.email_sender import EmailSender


class PipelineRunner:
    def __init__(self):
        self.state_manager = ProcessedStateManager(
            settings.app_config["state"]["processed_ids_file"]
        )
        self.listener = EmailListener()
        self.parser = AttachmentParser()
        self.stakeholders = settings.load_yaml("stakeholders.yaml")["stakeholders"]
        self.output_dir = Path(settings.app_config["files"]["output_path"])

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_forever(self):
        poll_interval = settings.load_yaml("email_listener.yaml")["polling"][
            "interval_seconds"
        ]
        logger.info(f"Service started. Polling every {poll_interval} seconds.")

        while True:
            try:
                self.process_cycle()
            except Exception as e:
                logger.error(f"Cycle failed: {e}")

            time.sleep(poll_interval)

    def process_cycle(self):
        # 1. Connect & Search
        mail = self.listener.connect()
        new_ids = self.listener.fetch_unprocessed_ids(mail, self.state_manager)

        if not new_ids:
            logger.debug("No new valid emails found.")
            return

        logger.info(f"Found {len(new_ids)} new emails to process.")

        for email_bytes_id in new_ids:
            try:
                # 2. Extract
                msg = self.listener.get_email_content(mail, email_bytes_id)
                msg_id_clean = msg["Message-ID"].strip()

                csv_path = self.parser.extract_csv(msg)

                if not csv_path:
                    logger.warning(f"Email {msg_id_clean} had no valid CSV. Skipping.")
                    self.state_manager.mark_processed(msg_id_clean)
                    continue

                # 3. Process the File
                self.execute_pipeline(csv_path)

                # 4. Mark Complete
                self.state_manager.mark_processed(msg_id_clean)
                logger.info(f"Successfully processed email {msg_id_clean}")

            except Exception as e:
                logger.error(f"Failed to process specific email: {e}")

    def execute_pipeline(self, file_path: str):
        df = CsvLoader(file_path).load()
        SchemaValidator.validate(df)

        splitter = PlatformSplitter(df)
        splitter.split()
        df_tagged = splitter.df

        sender = EmailSender()
        renderer = HtmlRenderer()

        for user in self.stakeholders:
            logger.info(f"Generating report for {user['name']}")

            allowed = user["allowed_platforms"]
            user_df = df_tagged[df_tagged["__platform_key"].isin(allowed)].copy()

            if user_df.empty:
                logger.info(f"No data for user {user['name']}. Skipping.")
                continue

            aggregator = Aggregator(user_df)
            summary_df = aggregator.process()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            debug_filename = f"debug_{user['id']}_{timestamp}.csv"
            debug_path = self.output_dir / debug_filename

            summary_df.to_csv(debug_path, index=False)
            logger.info(f"[DEBUG] Saved processed data to: {debug_path}")

            total_sales = (
                summary_df["Total Sales"].sum() if not summary_df.empty else 0.0
            )
            avg_risk = summary_df["Avg Risk Score"].mean()
            if pd.isna(avg_risk):
                avg_risk = 0.0

            kpis = {
                "Total Sales": f"₹{total_sales:,.2f}",
                "Total Orders": f"{len(user_df)}",
                "Avg RTO Score": f"{avg_risk:.2f}",
            }

            html_body = renderer.render_stakeholder_report(
                stakeholder_name=user["name"],
                kpis=kpis,
                summary_df=summary_df,
                breakdown_df=summary_df if user.get("include_breakdown") else None,
            )

            sender.send(user["email"], "Your Daily Report", html_body)
