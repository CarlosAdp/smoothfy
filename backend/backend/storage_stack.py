import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import constructs


class StorageStack(cdk.Stack):

    def __init__(
            self,
            scope: constructs.Construct,
            construct_id: str,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table_user_saved_tracks = dynamodb.Table(
            self, 'UserSavedTracks',
            table_name='SmoothFyUserSavedTracks',
            partition_key=dynamodb.Attribute(
                name='UserId',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='ResultOffset',
                type=dynamodb.AttributeType.NUMBER
            ),
            time_to_live_attribute='TTL',
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
