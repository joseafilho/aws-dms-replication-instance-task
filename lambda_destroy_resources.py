import boto3

client = boto3.client('dms')

def lambda_handler(event, context):
    replication_instance_arn = get_replication_instance_arn()
    delete_replications_tasks(replication_instance_arn)
    delete_replication_instance(replication_instance_arn)

def delete_replications_tasks(arn_replication_instance: str):
    print(f'Deleting replication tasks.')

    tasks_response = client.describe_replication_tasks(
        Filters = [
            {
                'Name': 'replication-instance-arn',
                'Values': [
                    arn_replication_instance,
                ]
            },
        ]
    )

    tasks = tasks_response['ReplicationTasks']
    
    for task in tasks:
        task_arn = task['ReplicationTaskArn']
        client.delete_replication_task(
            ReplicationTaskArn = task_arn
        )

        task_waiter = client.get_waiter('replication_task_deleted')
        task_waiter.wait(
            Filters = [
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        task_arn,
                    ]
                },
            ]
        )

        print(f'Replication task deleted: {task_arn}')

def delete_replication_instance(arn_replication_instance: str):    
    print(f'Deleting replication instance.')
    
    client.delete_replication_instance(
        ReplicationInstanceArn = arn_replication_instance
    )
    
    waiter = client.get_waiter('replication_instance_deleted')
    waiter.wait(
        Filters = [
            {
                'Name': 'replication-instance-arn',
                'Values': [
                    arn_replication_instance,
                ]
            },
        ]
    )

    print(f'Replication instance deleted: {arn_replication_instance}')    

def get_replication_instance_arn() -> str:
    replication_instance_response = client.describe_replication_instances(
        Filters = [
            {
                'Name': 'replication-instance-id',
                'Values': [
                    'dms-redshift',
                ]
            },
        ]
    )

    return replication_instance_response['ReplicationInstances'][0]['ReplicationInstanceArn']

if __name__ == '__main__':
    lambda_handler(None, None)