import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_athena as athena,
  aws_cloudwatch as cloudwatch,
  aws_cloudwatch_actions as cwActions,
  aws_dynamodb as dynamodb,
  aws_events as events,
  aws_events_targets as targets,
  aws_glue as glue,
  aws_iam as iam,
  aws_lambda as lambda,
  aws_logs as logs,
  aws_s3 as s3,
  aws_sns as sns,
  aws_stepfunctions as sfn,
  aws_stepfunctions_tasks as tasks,
} from 'aws-cdk-lib';
import * as path from 'path';
import { Construct } from 'constructs';

export class TurnstileStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const databaseName = 'turnstile_lake';
    const workgroupName = 'turnstile_wg';

    const lake = new s3.Bucket(this, 'TurnstileLake', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [
        {
          prefix: 'bronze/',
          transitions: [{ storageClass: s3.StorageClass.DEEP_ARCHIVE, transitionAfter: Duration.days(180) }],
        },
        {
          prefix: 'silver/',
          transitions: [{ storageClass: s3.StorageClass.INTELLIGENT_TIERING, transitionAfter: Duration.days(60) }],
        },
        { prefix: 'gold/', transitions: [{ storageClass: s3.StorageClass.INTELLIGENT_TIERING, transitionAfter: Duration.days(90) }] },
        { prefix: 'athena-results/', expiration: Duration.days(7) },
      ],
      removalPolicy: RemovalPolicy.RETAIN,
    });

    const workgroupArn = `arn:aws:athena:${this.region}:${this.account}:workgroup/${workgroupName}`;

    lake.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'AllowAthenaListAndLocation',
        effect: iam.Effect.ALLOW,
        principals: [new iam.ServicePrincipal('athena.amazonaws.com')],
        actions: ['s3:GetBucketLocation', 's3:ListBucket'],
        resources: [lake.bucketArn],
        conditions: {
          StringEquals: { 'aws:SourceAccount': this.account },
          ArnLike: { 'aws:SourceArn': workgroupArn },
        },
      }),
    );

    lake.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'AllowAthenaWriteResultsPrefix',
        effect: iam.Effect.ALLOW,
        principals: [new iam.ServicePrincipal('athena.amazonaws.com')],
        actions: ['s3:PutObject', 's3:GetObject', 's3:AbortMultipartUpload'],
        resources: [lake.arnForObjects('athena-results/*')],
        conditions: {
          StringEquals: { 'aws:SourceAccount': this.account },
          ArnLike: { 'aws:SourceArn': workgroupArn },
        },
      }),
    );

    new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: { name: databaseName },
    });

    const workgroup = new athena.CfnWorkGroup(this, 'AthenaWorkgroup', {
      name: workgroupName,
      description: 'Turnstile workgroup for Bronze/Silver/Gold Athena jobs',
      state: 'ENABLED',
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: false,
        resultConfiguration: { outputLocation: `s3://${lake.bucketName}/athena-results/` },
        bytesScannedCutoffPerQuery: 500 * 1024 * 1024,
        engineVersion: { selectedEngineVersion: 'Athena engine version 3' },
        publishCloudWatchMetricsEnabled: true,
      },
    });

    const runManifest = new dynamodb.Table(this, 'RunManifest', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const ingestFn = new lambda.DockerImageFunction(this, 'IngestEnvFn', {
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '..', 'lambdas', 'ingest_env')),
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.minutes(2),
      memorySize: 1024,
      description: 'Ingests external source data and lands raw parquet files in Bronze S3 prefixes.',
      environment: {
        LAKE_BUCKET: lake.bucketName,
      },
    });

    ingestFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['s3:PutObject'],
        resources: [lake.arnForObjects('bronze/*')],
      }),
    );

    ingestFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['s3:PutObject'],
        resources: [lake.arnForObjects('bronze/_manifests/*')],
      }),
    );

    const sfnLogGroup = new logs.LogGroup(this, 'TurnstileSfnLogs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const sfnRole = new iam.Role(this, 'TurnstileSfnRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      description: 'Role for the Turnstile Step Functions state machine.',
    });

    // CloudWatch Logs delivery API does not support resource-level scoping for these actions.
    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
          'logs:DescribeLogStreams',
          'logs:CreateLogDelivery',
          'logs:GetLogDelivery',
          'logs:UpdateLogDelivery',
          'logs:DeleteLogDelivery',
          'logs:ListLogDeliveries',
        ],
        resources: ['*'],
      }),
    );

    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          's3:GetBucketLocation',
          's3:ListBucket',
        ],
        resources: [lake.bucketArn],
      }),
    );

    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject'],
        resources: [lake.arnForObjects('bronze/*')],
      }),
    );

    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:PutObject', 's3:AbortMultipartUpload'],
        resources: [lake.arnForObjects('silver/*'), lake.arnForObjects('gold/*'), lake.arnForObjects('athena-results/*')],
      }),
    );

    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults', 'athena:StopQueryExecution', 'athena:GetWorkGroup'],
        resources: [
          workgroupArn,
          `arn:aws:athena:${this.region}:${this.account}:queryexecution/*`,
          `arn:aws:athena:${this.region}:${this.account}:datacatalog/*`,
        ],
      }),
    );

    // Athena list operations currently require wildcard resource.
    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['athena:ListWorkGroups'],
        resources: ['*'],
      }),
    );

    sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['glue:GetDatabase', 'glue:GetDatabases', 'glue:GetTable', 'glue:GetTables', 'glue:CreateTable', 'glue:UpdateTable', 'glue:DeleteTable'],
        resources: [
          `arn:aws:glue:${this.region}:${this.account}:catalog`,
          `arn:aws:glue:${this.region}:${this.account}:database/${databaseName}`,
          `arn:aws:glue:${this.region}:${this.account}:table/${databaseName}/*`,
        ],
      }),
    );

    runManifest.grantReadWriteData(sfnRole);
    ingestFn.grantInvoke(sfnRole);

    const bronzeUsgs = `
CREATE EXTERNAL TABLE IF NOT EXISTS ${databaseName}.bronze_usgs (
  event_id string,
  time_utc timestamp,
  mag double,
  place string,
  lon double,
  lat double,
  depth_km double
)
PARTITIONED BY (ingestion_date string)
STORED AS PARQUET
LOCATION 's3://${lake.bucketName}/bronze/usgs/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.range'='2020/01/01/00,NOW',
  'projection.ingestion_date.format'='yyyy/MM/dd/HH',
  'projection.ingestion_date.interval'='1',
  'projection.ingestion_date.interval.unit'='HOURS',
  'storage.location.template'='s3://${lake.bucketName}/bronze/usgs/ingestion_date=${'${ingestion_date}'}/'
)`;

    const bronzeWeather = `
CREATE EXTERNAL TABLE IF NOT EXISTS ${databaseName}.bronze_weather (
  city string,
  timestamp_utc timestamp,
  temp_c double,
  precip_mm double,
  wind_mps double,
  lat double,
  lon double
)
PARTITIONED BY (ingestion_date string)
STORED AS PARQUET
LOCATION 's3://${lake.bucketName}/bronze/weather/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.range'='2020/01/01/00,NOW',
  'projection.ingestion_date.format'='yyyy/MM/dd/HH',
  'projection.ingestion_date.interval'='1',
  'projection.ingestion_date.interval.unit'='HOURS',
  'storage.location.template'='s3://${lake.bucketName}/bronze/weather/ingestion_date=${'${ingestion_date}'}/'
)`;

    const silverUsgs = `
CREATE TABLE IF NOT EXISTS ${databaseName}.silver_usgs
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${lake.bucketName}/silver/usgs/',
  partitioned_by=ARRAY['p_date']
) AS
SELECT
  event_id,
  from_unixtime(to_unixtime(time_utc)) AS time_utc,
  CAST(mag AS DOUBLE) AS mag,
  place,
  CAST(lon AS DOUBLE) AS lon,
  CAST(lat AS DOUBLE) AS lat,
  CAST(depth_km AS DOUBLE) AS depth_km,
  date_format(time_utc, '%Y-%m-%d') AS p_date
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY event_id ORDER BY time_utc DESC) AS rn
  FROM ${databaseName}.bronze_usgs
)
WHERE rn = 1`;

    const silverWeather = `
CREATE TABLE IF NOT EXISTS ${databaseName}.silver_weather
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${lake.bucketName}/silver/weather/',
  partitioned_by=ARRAY['city','p_date']
) AS
SELECT
  from_unixtime(to_unixtime(timestamp_utc)) AS timestamp_utc,
  CAST(temp_c AS DOUBLE) AS temp_c,
  CAST(precip_mm AS DOUBLE) AS precip_mm,
  CAST(wind_mps AS DOUBLE) AS wind_mps,
  CAST(lat AS DOUBLE) AS lat,
  CAST(lon AS DOUBLE) AS lon,
  city,
  date_format(timestamp_utc, '%Y-%m-%d') AS p_date
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY city, timestamp_utc ORDER BY timestamp_utc DESC) AS rn
  FROM ${databaseName}.bronze_weather
)
WHERE rn = 1`;

    const goldWeatherDaily = `
CREATE TABLE IF NOT EXISTS ${databaseName}.gold_weather_daily
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${lake.bucketName}/gold/weather_daily/',
  partitioned_by=ARRAY['city','day']
) AS
SELECT
  AVG(temp_c)     AS avg_temp_c,
  MIN(temp_c)     AS min_temp_c,
  MAX(temp_c)     AS max_temp_c,
  SUM(precip_mm)  AS sum_precip_mm,
  AVG(wind_mps)   AS avg_wind_mps,
  COUNT(*)        AS readings,
  city,
  date_trunc('day', timestamp_utc) AS day
FROM ${databaseName}.silver_weather
GROUP BY city, date_trunc('day', timestamp_utc)`;

    const goldQuakesDaily = `
CREATE TABLE IF NOT EXISTS ${databaseName}.gold_quakes_daily
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${lake.bucketName}/gold/quakes_daily/',
  partitioned_by=ARRAY['day']
) AS
SELECT
  COUNT(*) AS quake_count,
  AVG(mag) AS avg_mag,
  MAX(mag) AS max_mag,
  SUM(CASE WHEN mag >= 4.0 THEN 1 ELSE 0 END) AS m4plus,
  date_trunc('day', time_utc) AS day
FROM ${databaseName}.silver_usgs
GROUP BY date_trunc('day', time_utc)`;

    const startQuery = (id: string, query: string, resultPath: string): tasks.AthenaStartQueryExecution =>
      new tasks.AthenaStartQueryExecution(this, id, {
        queryString: query,
        queryExecutionContext: { databaseName },
        workGroup: workgroupName,
        integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        resultPath,
      });

    const deriveExecutionParams = new sfn.Pass(this, 'DeriveExecutionParams', {
      parameters: {
        'date.$': "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)",
        'hour.$': "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 0)",
      },
      resultPath: '$.params',
    });

    const ensureParams = new sfn.Choice(this, 'ParamsProvided?')
      .when(
        sfn.Condition.and(
          sfn.Condition.isPresent('$.params.date'),
          sfn.Condition.isPresent('$.params.hour'),
        ),
        new sfn.Pass(this, 'UseProvidedParams', { resultPath: '$' }),
      )
      .otherwise(deriveExecutionParams);

    const manifestLookup = new tasks.DynamoGetItem(this, 'ManifestLookup', {
      table: runManifest,
      key: {
        pk: tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour')),
        ),
        sk: tasks.DynamoAttributeValue.fromString('manifest'),
      },
      resultPath: '$.manifest',
    });

    const clearManifestForReplay = new tasks.DynamoDeleteItem(this, 'ClearManifestForReplay', {
      table: runManifest,
      key: {
        pk: tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour')),
        ),
        sk: tasks.DynamoAttributeValue.fromString('manifest'),
      },
      resultPath: sfn.JsonPath.DISCARD,
    });

    const ingest = new tasks.LambdaInvoke(this, 'IngestSources', {
      lambdaFunction: ingestFn,
      payload: sfn.TaskInput.fromObject({
        lakeBucket: lake.bucketName,
      }),
      resultPath: '$.ingest',
    });

    const createBronzeUsgs = startQuery('AthenaCreateBronzeUsgs', bronzeUsgs, '$.athena.bronze.usgs');
    const createBronzeWeather = startQuery('AthenaCreateBronzeWeather', bronzeWeather, '$.athena.bronze.weather');
    const buildSilverUsgs = startQuery('AthenaBuildSilverUsgs', silverUsgs, '$.athena.silver.usgs');
    const buildSilverWeather = startQuery('AthenaBuildSilverWeather', silverWeather, '$.athena.silver.weather');
    const buildGoldWeatherDaily = startQuery('AthenaBuildGoldWeatherDaily', goldWeatherDaily, '$.athena.gold.weather_daily');
    const buildGoldQuakesDaily = startQuery('AthenaBuildGoldQuakesDaily', goldQuakesDaily, '$.athena.gold.quakes_daily');

    const recordSuccess = new tasks.DynamoPutItem(this, 'RecordManifestSuccess', {
      table: runManifest,
      item: {
        pk: tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour')),
        ),
        sk: tasks.DynamoAttributeValue.fromString('manifest'),
        status: tasks.DynamoAttributeValue.fromString('SUCCEEDED'),
        started_at: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.Execution.StartTime')),
        finished_at: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
      },
      resultPath: sfn.JsonPath.DISCARD,
    });

    const recordFailure = new tasks.DynamoPutItem(this, 'RecordManifestFailure', {
      table: runManifest,
      item: {
        pk: tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour')),
        ),
        sk: tasks.DynamoAttributeValue.fromString('manifest'),
        status: tasks.DynamoAttributeValue.fromString('FAILED'),
        started_at: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.Execution.StartTime')),
        finished_at: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
      },
      resultPath: sfn.JsonPath.DISCARD,
    });

    const pipelineFailed = new sfn.Fail(this, 'PipelineFailed', {
      error: 'PipelineExecutionFailed',
      cause: 'Bronze to Silver to Gold processing failed.',
    });

    const done = new sfn.Succeed(this, 'Done');
    const runPipeline = new sfn.Parallel(this, 'RunPipeline');
    runPipeline.branch(
      ingest
        .next(createBronzeUsgs)
        .next(createBronzeWeather)
        .next(buildSilverUsgs)
        .next(buildSilverWeather)
        .next(buildGoldWeatherDaily)
        .next(buildGoldQuakesDaily)
        .next(recordSuccess),
    );
    runPipeline.addCatch(recordFailure.next(pipelineFailed), { resultPath: '$.error' });
    runPipeline.next(done);

    const alreadyProcessedChoice = new sfn.Choice(this, 'AlreadyProcessed?')
      .when(sfn.Condition.isPresent('$.manifest.Item.pk.S'), new sfn.Succeed(this, 'SkipWindow'))
      .otherwise(runPipeline);

    const replayChoice = new sfn.Choice(this, 'ForceReplay?')
      .when(sfn.Condition.booleanEquals('$.force_replay', true), clearManifestForReplay.next(runPipeline))
      .otherwise(alreadyProcessedChoice);

    const definition = sfn.Chain.start(ensureParams.afterwards()).next(manifestLookup).next(replayChoice);

    const stateMachine = new sfn.StateMachine(this, 'TurnstileStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      tracingEnabled: true,
      role: sfnRole,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      logs: {
        destination: sfnLogGroup,
        level: sfn.LogLevel.ALL,
        includeExecutionData: true,
      },
    });

    const rule = new events.Rule(this, 'HourlyTurnstileRun', {
      schedule: events.Schedule.cron({ minute: '15', hour: '*' }),
    });
    rule.addTarget(new targets.SfnStateMachine(stateMachine));

    const opsTopic = new sns.Topic(this, 'TurnstileOpsTopic', {
      displayName: 'Turnstile Alerts',
    });

    new cloudwatch.Alarm(this, 'SfnFailedAlarm', {
      metric: stateMachine.metricFailed(),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    }).addAlarmAction(new cwActions.SnsAction(opsTopic));

    new cloudwatch.Alarm(this, 'IngestFnErrorsAlarm', {
      metric: ingestFn.metricErrors({ period: Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
    }).addAlarmAction(new cwActions.SnsAction(opsTopic));

    new CfnOutput(this, 'LakeBucketName', { value: lake.bucketName });
    new CfnOutput(this, 'GlueDatabaseName', { value: databaseName });
    new CfnOutput(this, 'AthenaWorkgroupOutput', { value: workgroup.name ?? workgroupName });
    new CfnOutput(this, 'RunManifestTable', { value: runManifest.tableName });
    new CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
    new CfnOutput(this, 'AlertsTopicArn', { value: opsTopic.topicArn });
  }
}
