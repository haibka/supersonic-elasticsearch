import boto3

# name = 'supersonic'
# sqs = boto3.resource('sqs')
# queue = sqs.get_queue_by_name(QueueName=name)
sqs_url = 'https://sqs.ap-northeast-1.amazonaws.com/898942717313/supersonic'
sqs = boto3.resource('sqs')
queue = sqs.Queue(sqs_url)
while True:
    msg_list = queue.receive_messages(MaxNumberOfMessages=10)
    if msg_list:
        for message in msg_list:
            print(message.body)
            # message.delete()
    else:
        break
