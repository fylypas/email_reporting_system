import sys
from app.runner import PipelineRunner
from app.utils.logger import logger

if __name__ == "__main__":
    try:
        runner = PipelineRunner()
        runner.run_forever()
    except KeyboardInterrupt:
        logger.info("Stopping service...")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
