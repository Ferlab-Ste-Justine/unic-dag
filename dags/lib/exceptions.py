from airflow.exceptions import AirflowFailException


class AirflowInputParsingException(AirflowFailException):
    """
    Custom exception for input parsing errors in Airflow tasks or DAGs.
    This exception is raised when the parsing of input data fails
    """
    def __init__(self, input_id: str):
        message = f"Failed to parse input {input_id}."
        super().__init__(message)

class MinioFileNotFoundException(AirflowFailException):
    """
    Custom exception for file not found errors in Minio.
    This exception is raised when a file is not found in the specified Minio bucket.
    """
    def __init__(self, bucket: str, key: str):
        message = f"File {key} not found in bucket {bucket}."
        super().__init__(message)