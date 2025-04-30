# Auto-Scaling Configuration for Web Crawler System

This document explains how to configure and deploy the auto-scaling feature for the distributed web crawler system. Auto-scaling allows the system to dynamically adjust the number of crawler nodes based on workload, ensuring efficient resource utilization.

## Prerequisites

1. AWS CLI installed and configured
2. Python 3.6+ with boto3 installed (`pip install boto3`)
3. An EC2 key pair for SSH access
4. AWS IAM permissions for CloudFormation, EC2, Auto Scaling, SQS, S3, and CloudWatch

## Files

- `ec2_auto_scaling.yaml`: CloudFormation template for auto-scaling configuration
- `deploy_autoscaling.py`: Helper script to deploy the auto-scaling group
- `masterNode.py`: Modified to process more URLs and run longer

## How to Deploy

1. **Prepare your environment**

   Ensure you have all required AWS resources set up:
   - SQS queues (crawler-url-queue, crawler-status-queue)
   - S3 bucket (web-crawler-data-storage)

2. **Modify the CloudFormation template (optional)**

   If needed, update the `ec2_auto_scaling.yaml` file to match your specific requirements:
   - Change the AMI ID to match your region
   - Adjust the user data script to match your repository URL
   - Modify security group settings

3. **Deploy the auto-scaling group**

   Run the deployment script:
   ```bash
   python deploy_autoscaling.py --key-name YOUR_KEY_PAIR_NAME
   ```

   Additional options:
   ```
   --min-instances 1        # Minimum number of crawler nodes
   --max-instances 5        # Maximum number of crawler nodes
   --desired-capacity 2     # Initial number of crawler nodes
   --instance-type t2.micro # EC2 instance type
   --sqs-queue crawler-url-queue          # Task queue name
   --status-queue crawler-status-queue    # Status queue name
   --s3-bucket web-crawler-data-storage  # S3 bucket name
   ```

## How Auto-Scaling Works

The system uses two scaling policies:

1. **Queue-based scaling**: Adds or removes crawler nodes based on the number of messages in the SQS queue. It targets approximately 10 messages per crawler instance.

2. **CPU-based scaling**: Adds or removes crawler nodes based on CPU utilization. It targets 70% CPU utilization.

## Testing the Auto-Scaling

1. Start the master node:
   ```bash
   python main.py --role master
   ```

2. Start a single indexer node:
   ```bash
   python main.py --role indexer
   ```

3. Monitor the auto-scaling group in the AWS Console:
   - Go to EC2 > Auto Scaling Groups
   - Select your auto-scaling group
   - View the instances and activity

4. Generate load by adding more URLs to the queue:
   ```bash
   python debug_queue.py add
   ```

5. Observe the auto-scaling behavior as the queue grows

## Monitoring

Auto-scaled crawler nodes send metrics and logs to CloudWatch:

1. **CloudWatch Metrics**:
   - CPU utilization
   - Memory usage
   - Disk usage
   - SQS queue size

2. **CloudWatch Logs**:
   - Crawler node logs are available in the "crawler-logs" log group
   - Each instance has its own log stream

## Cleanup

To remove the auto-scaling group and associated resources:

```bash
aws cloudformation delete-stack --stack-name web-crawler-autoscaling
```

## Troubleshooting

1. **Instances failing to start**:
   - Check the CloudWatch logs for instance bootstrap errors
   - Verify IAM permissions
   - Check security group settings

2. **Instances not scaling out**:
   - Verify CloudWatch alarms are being triggered
   - Check scaling policy settings
   - Ensure SQS queue has messages

3. **Crawler nodes not processing URLs**:
   - Check instance logs in CloudWatch
   - Verify the crawler node can access the SQS queues
   - Check for network connectivity issues 