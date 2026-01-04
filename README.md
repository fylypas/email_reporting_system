# Automated Platform-Specific Reporting System

An event-driven ETL pipeline that ingests order data via email, segments it by advertising platform (Meta/Google), calculates risk/revenue metrics, and dispatches personalized HTML reports to stakeholders.

## 🚀 Key Features
* **Event-Driven:** Polls IMAP for emails matching specific subject triggers.
* **Idempotent:** Tracks `Message-ID` to prevent duplicate processing of the same file.
* **Platform Segmentation:** Auto-tags orders (Meta vs. Google) based on referrer keywords.
* **Risk Modeling:** Computes RTO (Return-to-Origin) scores and deciles.
* **HTML Reporting:** Renders responsive, inline-CSS tables with "Top N" summarization to avoid email clipping.

## 🛠️ Prerequisites
* **Python 3.10+**
* **Node.js & npm** (Used for task running/scripts)
* **Gmail Account** with 2-Step Verification enabled and an **App Password** generated.

## 📦 Installation

1.  **Clone the repository**
    ```bash
    git clone <repo_url>
    cd reporting_system
    ```

2.  **Install Dependencies**
    ```bash
    npm run setup
    # Equivalent to: pip install -r requirements.txt
    ```

## ⚙️ Configuration

Update the YAML files in `app/config/` with your credentials and rules.

1.  **`email_listener.yaml`** (IMAP Settings)
    * `username`: Your email address.
    * `password`: Your 16-char App Password.
    * `subject_keyword`: The email subject to listen for (e.g., "Data Dump - Orders").

2.  **`email.yaml`** (SMTP Settings)
    * Use the same credentials as above to enable reply capabilities.

3.  **`stakeholders.yaml`**
    * Define recipients and their access rights (e.g., User A sees only 'Meta' data).

## 🏃 Usage

### Start the Service
Run the bot in polling mode (checks every 60s by default):
```bash
npm start
# or: python -m app.main