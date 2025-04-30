#!/usr/bin/env python3
import boto3
import argparse
import time
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def deploy_crawler(stack_name, template_file, parameters):
    """
    Deploy a single crawler node using CloudFormation
    """
    try:
        # Read the CloudFormation template
        with open(template_file, 'r') as file:
            template_body = file.read()
        
        # Create CloudFormation client
        cf_client = boto3.client('cloudformation')
        
        # Convert parameters to CloudFormation format
        cf_parameters = []
        for key, value in parameters.items():
            cf_parameters.append({
                'ParameterKey': key,
                'ParameterValue': str(value)
            })
        
        # Check if stack exists
        try:
            cf_client.describe_stacks(StackName=stack_name)
            stack_exists = True
        except:
            stack_exists = False
        
        if stack_exists:
            logging.info(f"Updating existing stack: {stack_name}")
            cf_client.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=cf_parameters,
                Capabilities=['CAPABILITY_IAM']
            )
        else:
            logging.info(f"Creating new stack: {stack_name}")
            cf_client.create_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=cf_parameters,
                Capabilities=['CAPABILITY_IAM']
            )
        
        # Wait for stack to complete
        logging.info("Waiting for stack deployment to complete...")
        waiter = cf_client.get_waiter('stack_create_complete' if not stack_exists else 'stack_update_complete')
        waiter.wait(StackName=stack_name)
        
        # Get stack outputs
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response['Stacks'][0].get('Outputs', [])
        
        logging.info("Stack deployment successful!")
        for output in outputs:
            logging.info(f"{output['OutputKey']}: {output['OutputValue']}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error deploying stack: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Deploy a single crawler node')
    parser.add_argument('--stack-name', default='web-crawler-single-instance',
                        help='Name of the CloudFormation stack')
    parser.add_argument('--template', default='simple_crawler_instance.yaml',
                        help='Path to the CloudFormation template file')
    parser.add_argument('--key-name', required=True,
                        help='EC2 key pair name for SSH access')
    parser.add_argument('--instance-type', default='t2.micro',
                        help='EC2 instance type for crawler node')
    parser.add_argument('--sqs-queue', default='crawler-url-queue',
                        help='Name of the SQS queue for task distribution')
    parser.add_argument('--status-queue', default='crawler-status-queue',
                        help='Name of the SQS queue for status updates')
    parser.add_argument('--s3-bucket', default='web-crawler-data-storage',
                        help='Name of the S3 bucket for crawler data')
    
    args = parser.parse_args()
    
    # Check if template file exists
    if not os.path.isfile(args.template):
        logging.error(f"Template file not found: {args.template}")
        return 1
    
    # Prepare parameters
    parameters = {
        'KeyName': args.key_name,
        'InstanceType': args.instance_type,
        'SQSQueueName': args.sqs_queue,
        'StatusQueueName': args.status_queue,
        'S3BucketName': args.s3_bucket
    }
    
    # Deploy the stack
    success = deploy_crawler(args.stack_name, args.template, parameters)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 