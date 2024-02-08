"""WIP
"""
from src.components.data_preprocessor import DataPreprocessor
from src.exception import CustomException
from src.logger import logger


class DataPreprocessorPipeline:
    """_summary_"""

    def __init__(self):
        pass

    def main(self):
        """_summary_

        Raises:
            CustomException: _description_
        """
        try:
            logger.info("Data preprocessing started")
            preprocessor = DataPreprocessor()
            preprocessor.save_processed_data()
            logger.info("Data preprocessing completed successfully")
        except Exception as excp:
            logger.error(CustomException(excp))
            raise CustomException(excp) from excp


if __name__ == "__main__":
    STAGE_NAME = "Data Preprocessor Stage"

    try:
        logger.info(">>>>>> %s started <<<<<<", STAGE_NAME)
        obj = DataPreprocessorPipeline()
        obj.main()
        logger.info(">>>>>> %s completed <<<<<<\n\nx==========x", STAGE_NAME)
    except Exception as e:
        logger.error(CustomException(e))
        raise CustomException(e) from e
