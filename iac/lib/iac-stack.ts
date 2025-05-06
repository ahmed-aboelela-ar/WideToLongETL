import * as cdk from 'aws-cdk-lib';
import { Metric } from 'aws-cdk-lib/aws-cloudwatch';
import { EventField, Rule, RuleTargetInput, Schedule } from 'aws-cdk-lib/aws-events';
import { CloudWatchLogGroup, LambdaFunction, SfnStateMachine, SnsTopic } from 'aws-cdk-lib/aws-events-targets';
import { CfnCrawler, CfnDatabase, CfnJob } from 'aws-cdk-lib/aws-glue';
import { Effect, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Code, Function, Runtime } from 'aws-cdk-lib/aws-lambda';
import { LogGroup } from 'aws-cdk-lib/aws-logs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';
import { S3 } from 'aws-cdk-lib/aws-ses-actions';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { IntegrationPattern, StateMachine, TaskInput } from 'aws-cdk-lib/aws-stepfunctions';
import { CallAwsService, GlueStartJobRun } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId } from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class IacStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    

    const alarmTopic = new Topic(this, 'AlarmTopic', {
      displayName: "EnergyMarketAlarmTopic",
    });

    alarmTopic.addSubscription(new EmailSubscription('ahmed.aboelela@gmx.de'));

    const landingBucket = new Bucket(this, 'DataLandingZone', {
      bucketName: `data-landing-zone-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    landingBucket.enableEventBridgeNotification();

    const processedBucket = new Bucket(this, 'DataProcessedZone', {
        bucketName: `data-processed-zone-${this.account}-${this.region}`,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        lifecycleRules: [
          {
            expiration: cdk.Duration.days(1825),
          }
        ]
    });

    const tempBucket = new Bucket(this, 'DataTempZone', {
      bucketName: `data-temp-zone-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(1),
          prefix: 'temp/'
        }
      ]
    });

    const glueDatabase = new CfnDatabase(this, 'EnergyMarketDatabase', {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseInput: {
        name: 'energy_market_db',
        description: 'Database for German energy market price data',
      }
    });

    const glueRole = new Role(this, 'GlueJobRole', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
      description: 'Create a role that allow minimal privilages for energy market glue job.'
    });

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:ListBucket'
        ],
        resources: [
          landingBucket.arnForObjects('daa_market/*'),
          landingBucket.arnForObjects('scripts/*'),
          landingBucket.bucketArn
        ]
      })
    );


    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:PutObject',
          's3:DeleteObject'
        ],
        resources: [
          processedBucket.arnForObjects('daa_market/*'),
          processedBucket.arnForObjects('daa_market*')
        ]
      })
    );

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:PutObject',
          's3:DeleteObject'
        ],
        resources:[tempBucket.arnForObjects('temp/*')]
      })
    );

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        resources: [`arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/jobs/*`],
      })
    );

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'glue:GetJob',
          'glue:GetJobRun',
          'glue:GetJobRuns'
        ],
        resources: [
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job/energy-market-etl-job`
        ]
      })
    );


    const glueJob = new CfnJob(this, 'EnergyMarketGlueJob', {
      name: 'energy-market-etl-job',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${landingBucket.bucketName}/scripts/energy_market_etl.py`,
        pythonVersion: '3',
      },
      defaultArguments: {
        '--TempDir': `s3://${tempBucket.bucketName}/temp/`,
        '--job-language': 'python',
        '--source_bucket': landingBucket.bucketName,
        '--target_bucket': processedBucket.bucketName,
        '--database_name': 'energy_market_db',
        '--table_name': 'daa_market',
        '--modified_file': ''
      },
      maxRetries: 0,
      glueVersion: '4.0',
      workerType: 'Standard',
      numberOfWorkers: 2,
    });
    
    const glueJobFailureRule = new Rule(this, 'GlueJobFailureRule', {
      eventPattern: {
        source: ['aws.glue'],
        detailType: ["Glue Job State Change"],
        detail: {
          "state": ["FAILED"],
          "jobName": [glueJob.name]
        }
      }
    });

    glueJobFailureRule.addTarget(new SnsTopic(alarmTopic));

    new BucketDeployment(this, 'DeployGlueScript', {
      sources:[Source.asset('../scripts')],
      destinationBucket: landingBucket,
      destinationKeyPrefix: 'scripts/',
      retainOnDelete: false
    });

    const glueCrawler = new CfnCrawler(this, 'EnergyMarketCrawler', {
      name: 'energy-market-crawler',
      role: glueRole.roleArn,
      databaseName: glueDatabase.ref,
      targets: {
        s3Targets: [
          {
            path: `s3://${processedBucket.bucketName}/daa_market/`
          }
        ]
      },
      // tablePrefix: 'daa_',
      description: 'Crawls processed energy market data for Athena',
      schemaChangePolicy: { 
        updateBehavior: 'UPDATE_IN_DATABASE', 
        deleteBehavior: 'LOG' 
      },
    });

    const crawlerJobFailureRule = new Rule(this, 'CrawlerJobFailureRule', {
      eventPattern:{
        source: ['aws.glue'],
        detailType: ["Glue Crawler State Change"],
        detail: {
          "state": ["Failed"],
          "crawlerName": [glueCrawler.name]
        }
      }
    });

    crawlerJobFailureRule.addTarget(new SnsTopic(alarmTopic));

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:ListBucket'
        ],
        resources: [
          processedBucket.arnForObjects('daa_market/*'),
          processedBucket.bucketArn
        ],
      })
    );


    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogStream',
          'logs:CreateLogGroup',
          'logs:PutLogEvents'
        ],
        resources: [
          // Specific log group/stream
          `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/crawlers:*`,
          `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/crawlers:log-stream:energy-market-crawler*`
          
          // Alternatively, for broader permission (less secure):
          // 'arn:aws:logs:*:*:*'
        ],
      })
    );

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'glue:CreateTable',
          'glue:UpdateTable',
          'glue:CreatePartition',
          'glue:UpdatePartition',
          'glue:GetDatabase',
          'glue:GetPartition',
          'glue:GetPartitions',
          'glue:BatchGetPartition',
          'glue:BatchCreatePartition',
          'glue:GetTable'
        ],
        resources: [
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/energy_market_db`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/energy_market_db/daa_*`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:partition/energy_market_db/daa_*/*`
        ],
      })
    )

    const startGlueJob = new GlueStartJobRun(this, 'StartGlueJob', {
      glueJobName: glueJob.name || 'energy-market-etl-job',
      integrationPattern: IntegrationPattern.RUN_JOB,
      arguments: TaskInput.fromObject({
        '--modified_file.$': '$.modified_file'
      })
    });

    const startCrawler = new CallAwsService(this, 'StartCrawler', {
      service: 'glue',
      action: 'startCrawler',
      parameters: {
        Name: glueCrawler.name || 'energy-market-crawler'
      },
      iamResources: [ `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:crawler/energy-market-crawler` ]
    })

    const workflow = startGlueJob.next(startCrawler);

    const stateMachine = new StateMachine(this, 'EnergyMarketWorkflow', {
      definition: workflow, 
      timeout: cdk.Duration.minutes(10),
    });

    glueRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [ 'glue:StartJobRun', 'glue:StartCrawler' ],
        resources: [
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job/energy-market-etl-job`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:crawler/energy-market-crawler`,
        ]
      })
    );

    stateMachine.grantStartExecution(glueRole);

    const s3WatcherRule = new Rule(this, 'S3UpdateRule', {
      eventPattern: {
        source: ['aws.s3'],
        detailType: ["Object Created", "Object ACL Updated"],
        detail: {
          bucket: {name: [landingBucket.bucketName]},
          object: {key: [{prefix: 'daa_market/'}]}
        }
      }
    });


    s3WatcherRule.addTarget(new CloudWatchLogGroup(new LogGroup(this, 'S3EventLog', {
      logGroupName: '/aws/events/S3WatcherDebug',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    })))

    s3WatcherRule.addTarget(new SfnStateMachine(stateMachine, {
      input: RuleTargetInput.fromObject({
        // modified_file: `s3://${landingBucket.bucketName}/${EventField.fromPath('$.detail.object.key')}`,
        modified_file: EventField.fromPath('$.detail.object.key')
      })
    }));


    const lambda = new Function(this, 'DataSynchronization', {
      runtime: Runtime.PYTHON_3_12,
      handler: 'data_synchronization.data_synchronization',
      code: Code.fromAsset("../scripts"),
      timeout: cdk.Duration.minutes(3),
      environment:{
        API_URL: 'https://run.mocky.io/v3/c74eb710-9a88-4c57-8d59-03639f16ad6f',
        BUCKET_NAME: landingBucket.bucketName,
      }
    });

    landingBucket.grantWrite(lambda);

    const rule = new Rule(this, 'DailyRule', {
      schedule: Schedule.cron({ minute: '0', hour: '2' })
    });

    rule.addTarget(new LambdaFunction(lambda));
    
    new cdk.CfnOutput(this, 'LandingBucketOutput', { value: landingBucket.bucketName });
    new cdk.CfnOutput(this, 'ProcessedBucketOutput', { value: processedBucket.bucketName });
    new cdk.CfnOutput(this, 'TempBucketOutput', { value: tempBucket.bucketName });
    new cdk.CfnOutput(this, 'GlueJobName', { value: glueJob.name || 'energy-market-etl-job' });
    new cdk.CfnOutput(this, 'GlueCrawlerName', { value: glueCrawler.name || 'energy-market-crawler' });
    new cdk.CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
    new cdk.CfnOutput(this, 'S3WatcherRuleName', { value: s3WatcherRule.ruleName, description: 'Name of the S3 EventBridge rule',});
  }
}
