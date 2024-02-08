"""WIP
"""
from src.components.data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logger


class DataIngestionPipeline:
    """_summary_"""

    def __init__(self):
        pass

    def main(self):
        """_summary_

        Raises:
            CustomException: _description_
        """
        try:
            logger.info("Data ingestion started")
            data_ingestion = DataIngestion()
            data_ingestion.download_data()
            data_ingestion.save_dataset()
            logger.info("Data ingestion completed successfully")
        except Exception as excp:
            logger.error(CustomException(excp))
            raise CustomException(excp) from excp


if __name__ == "__main__":
    STAGE_NAME = "Data Ingestion Stage"

    try:
        logger.info(">>>>>> %s started <<<<<<", STAGE_NAME)
        obj = DataIngestionPipeline()
        obj.main()
        logger.info(">>>>>> %s completed <<<<<<\n\nx==========x", STAGE_NAME)
    except Exception as e:
        logger.error(CustomException(e))
        raise CustomException(e) from e
