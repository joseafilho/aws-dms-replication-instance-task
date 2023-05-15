import boto3

# Consts
MYSQL_READ_REPLICA_ARN = 'arn:aws:dms:xxxx'
REDSHIFT_ARN = 'arn:aws:dms:xxxx'

client = boto3.client('dms')

def lambda_handler(event, context):
    replication_instance_arn = create_replication_instance()
    create_replication_tasks(replication_instance_arn)

def create_replication_instance() -> str:
    replication_instance_response = client.create_replication_instance(
        ReplicationInstanceIdentifier = 'dms-redshift',
        AllocatedStorage = 50,
        ReplicationInstanceClass = 'dms.t3.small',
        MultiAZ = False,
        EngineVersion='3.4.7',
        Tags=[
            {
                'Key': 'IaC',
                'Value': 'True'
            },
            {
                'Key': 'Team',
                'Value': 'DataAnalytics'
            },
        ]
    )

    waiter = client.get_waiter('replication_instance_available')
    waiter.wait(
        Filters = [
            {
                'Name': 'replication-instance-id',
                'Values': [
                    'dms-redshift',
                ]
            },
        ]
    )

    print(replication_instance_response)
    return replication_instance_response['ReplicationInstance']['ReplicationInstanceArn']

def create_replication_tasks(arn_replication_instance: str):
    task_example(arn_replication_instance)
    task_example_backend(arn_replication_instance)
    task_example_backend_picking(arn_replication_instance)

def task_example(arn_replication_instance: str):
    replication_task_response = client.create_replication_task(
        ReplicationTaskIdentifier = 'example',
        SourceEndpointArn = MYSQL_READ_REPLICA_ARN,
        TargetEndpointArn = REDSHIFT_ARN,
        ReplicationInstanceArn = arn_replication_instance,
        MigrationType = 'full-load',
        TableMappings = """{
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "612989512",
                    "rule-name": "example-1-rule",
                    "object-locator": {
                        "schema-name": "example",
                        "table-name": "%"
                    },
                    "rule-action": "include",
                    "filters": []
                },
                {
                    "rule-type": "selection",
                    "rule-id": "613012196",
                    "rule-name": "example-2-rule",
                    "object-locator": {
                        "schema-name": "example",
                        "table-name": "t1"
                    },
                    "rule-action": "exclude",
                    "filters": []
                },
                {
                    "rule-type": "selection",
                    "rule-id": "612997837",
                    "rule-name": "example-3-rule",
                    "object-locator": {
                        "schema-name": "example",
                        "table-name": "t2"
                    },
                    "rule-action": "exclude",
                    "filters": []
                }
            ]
        }"""
    )

    print(replication_task_response)

def task_example_backend(arn_replication_instance: str):
    replication_task_response = client.create_replication_task(
        ReplicationTaskIdentifier = 'example-backend',
        SourceEndpointArn = MYSQL_READ_REPLICA_ARN,
        TargetEndpointArn = REDSHIFT_ARN,
        ReplicationInstanceArn = arn_replication_instance,
        MigrationType = 'full-load',
        TableMappings = """{
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "634490742",
                    "rule-name": "example-backend-1-rule",
                    "object-locator": {
                        "schema-name": "example_backend",
                        "table-name": "%"
                },
                    "rule-action": "include",
                    "filters": []
                },
                {
                    "rule-type": "selection",
                    "rule-id": "634463880",
                    "rule-name": "example-backend-2-rule",
                    "object-locator": {
                        "schema-name": "example_backend",
                        "table-name": "t3"
                },
                    "rule-action": "exclude",
                    "filters": []
                }
            ]
        }""",
    )

    print(replication_task_response)

def task_example_backend_picking(arn_replication_instance: str):
    replication_task_response = client.create_replication_task(
        ReplicationTaskIdentifier = 'example-backend-picking',
        SourceEndpointArn = MYSQL_READ_REPLICA_ARN,
        TargetEndpointArn = REDSHIFT_ARN,
        ReplicationInstanceArn = arn_replication_instance,
        MigrationType = 'full-load',
        TableMappings = """{
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "634807363",
                    "rule-name": "example-backend-picking-1-rule",
                    "object-locator": {
                        "schema-name": "example_backend",
                        "table-name": "t4"
                },
                    "rule-action": "include",
                    "filters": [
                        {
                            "filter-type": "source",
                            "column-name": "col1",
                            "filter-conditions": [
                                {
                                "filter-operator": "notnull"
                                }
                            ]
                        }
                    ]
                }
            ]
        }""",
    )

    print(replication_task_response)

if __name__ == '__main__':
    lambda_handler(None, None)