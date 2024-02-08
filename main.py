"""
summary
"""

from src.exception import CustomException
from src.logger import logger
from src.pipelines.stage_01_data_ingestion import DataIngestionPipeline
from src.pipelines.stage_03_data_preprocessor import DataPreprocessorPipeline

STAGE_NAME = "Data Ingestion stage"

try:
    logger.info(">>>>>> %s started <<<<<<", STAGE_NAME)
    obj = DataIngestionPipeline()
    obj.main()
    logger.info(">>>>>> %s completed <<<<<<\n\nx==========x", STAGE_NAME)
except Exception as e:
    logger.error(CustomException(e))
    raise CustomException(e) from e

STAGE_NAME = "Data Preprocessor Stage"

try:
    logger.info(">>>>>> %s started <<<<<<", STAGE_NAME)
    obj = DataPreprocessorPipeline()
    obj.main()
    logger.info(">>>>>> %s completed <<<<<<\n\nx==========x", STAGE_NAME)
except Exception as e:
    logger.error(CustomException(e))
    raise CustomException(e) from e
