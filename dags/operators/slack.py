from airflow.models.baseoperator import BaseOperator
from core.slack import Slack

class SlackOperator(BaseOperator):

    def __init__(
            self,
            markdown: str,
            type=Slack.INFO,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.markdown = markdown
        self.type = type

    def execute(self, context):
        Slack.notify(self.markdown, self.type)
