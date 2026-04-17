import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { TurnstileStack } from '../lib/turnstile-stack';

test('Stack creates core ETL resources', () => {
  const app = new cdk.App();
  const stack = new TurnstileStack(app, 'TurnstileTestStack');
  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::StepFunctions::StateMachine', 1);
  template.resourceCountIs('AWS::Lambda::Function', 1);
  template.resourceCountIs('AWS::DynamoDB::Table', 1);
  template.resourceCountIs('AWS::Events::Rule', 1);
  template.resourceCountIs('AWS::SNS::Topic', 1);
  template.resourceCountIs('AWS::CloudWatch::Alarm', 2);

  template.hasResourceProperties('AWS::S3::Bucket', {
    BucketEncryption: Match.objectLike({
      ServerSideEncryptionConfiguration: Match.arrayWith([
        Match.objectLike({
          ServerSideEncryptionByDefault: Match.objectLike({
            SSEAlgorithm: 'AES256',
          }),
        }),
      ]),
    }),
    VersioningConfiguration: {
      Status: 'Enabled',
    },
  });
});
