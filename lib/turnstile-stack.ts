import {
  Stack, StackProps, RemovalPolicy, Duration, CfnOutput,
  aws_logs as logs,
  aws_s3 as s3,
  aws_iam as iam,
  aws_glue as glue,
  aws_stepfunctions as sfn,
  aws_stepfunctions_tasks as tasks,
  aws_athena as athena,
  aws_lambda as lambda,
  aws_ecr_assets as ecr_assets,
  aws_dynamodb as ddb,
  aws_sns as sns,
  aws_cloudwatch as cloudwatch,
  aws_cloudwatch_actions as cw_actions,
} from 'aws-cdk-lib';
import * as path from 'path';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { Construct } from 'constructs';

export class TurnstileStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // 1) Data lake bucket
    const lake = new s3.Bucket(this, 'TurnstileLake', {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [
        { prefix: 'bronze/', transitions: [{ storageClass: s3.StorageClass.DEEP_ARCHIVE, transitionAfter: Duration.days(180) }] },
        { prefix: 'silver/', transitions: [{ storageClass: s3.StorageClass.INTELLIGENT_TIERING, transitionAfter: Duration.days(60) }] },
        { prefix: 'quarantine/', expiration: Duration.days(30) },
        { prefix: 'dq_reports/', expiration: Duration.days(30) },
        { prefix: 'athena-results/', expiration: Duration.days(7) },
      ],
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // Allow Athena service principal to verify/write results in our bucket/prefix
    lake.addToResourcePolicy(new iam.PolicyStatement({
      sid: 'AllowAthenaListAndLocation',
      effect: iam.Effect.ALLOW,
      principals: [ new iam.ServicePrincipal('athena.amazonaws.com') ],
      actions: ['s3:GetBucketLocation', 's3:ListBucket'],
      resources: [lake.bucketArn],
      conditions: {
        StringEquals: { 'aws:SourceAccount': this.account },
        ArnLike: { 'aws:SourceArn': `arn:aws:athena:${this.region}:${this.account}:workgroup/turnstile_wg` }
      }
    }));
    lake.addToResourcePolicy(new iam.PolicyStatement({
      sid: 'AllowAthenaWriteResultsPrefix',
      effect: iam.Effect.ALLOW,
      principals: [ new iam.ServicePrincipal('athena.amazonaws.com') ],
      actions: ['s3:PutObject', 's3:GetObject', 's3:AbortMultipartUpload'],
      resources: [`${lake.bucketArn}/athena-results/*`],
      conditions: {
        StringEquals: { 'aws:SourceAccount': this.account },
        ArnLike: { 'aws:SourceArn': `arn:aws:athena:${this.region}:${this.account}:workgroup/turnstile_wg` }
      }
    }));

    // 2) Glue database
    new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: { name: 'turnstile_lake' },
    });

    // 3) Athena WorkGroup
    const workgroup = new athena.CfnWorkGroup(this, 'AthenaWorkgroup', {
      name: 'turnstile_wg',
      description: 'Turnstile workgroup; CTAS allowed to write to lake prefixes',
      state: 'ENABLED',
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: false,
        resultConfiguration: { outputLocation: `s3://${lake.bucketName}/athena-results/` },
        bytesScannedCutoffPerQuery: 500 * 1024 * 1024,
        engineVersion: { selectedEngineVersion: 'Athena engine version 3' },
        publishCloudWatchMetricsEnabled: true,
      },
    });

    // 4) Step Functions logs
    const sfnLogGroup = new logs.LogGroup(this, 'TurnstileSfnLogs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // 5) State Machine role
    const sfnRole = new iam.Role(this, 'TurnstileSfnRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      description: 'Role for Turnstile DAG',
    });
    lake.grantReadWrite(sfnRole);
    sfnRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents','logs:DescribeLogStreams',
        'logs:CreateLogDelivery','logs:GetLogDelivery','logs:UpdateLogDelivery',
        'logs:DeleteLogDelivery','logs:ListLogDeliveries',
      ],
      resources: ['*'],
    }));
    sfnRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'athena:StartQueryExecution','athena:GetQueryExecution','athena:GetQueryResults',
        'athena:StopQueryExecution','athena:GetWorkGroup','athena:ListWorkGroups',
      ],
      resources: ['*'],
    }));
    sfnRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:GetDatabase','glue:GetDatabases','glue:GetTable','glue:GetTables',
        'glue:CreateTable','glue:UpdateTable','glue:DeleteTable'
      ],
      resources: [
        `arn:aws:glue:${this.region}:${this.account}:catalog`,
        `arn:aws:glue:${this.region}:${this.account}:database/turnstile_lake`,
        `arn:aws:glue:${this.region}:${this.account}:table/turnstile_lake/*`,
      ],
    }));

    // 6) Run Manifest (DynamoDB)
    const runManifest = new ddb.Table(this, 'RunManifest', {
      partitionKey: { name: 'pk', type: ddb.AttributeType.STRING },
      sortKey: { name: 'sk', type: ddb.AttributeType.STRING },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    sfnRole.addToPolicy(new iam.PolicyStatement({
      actions: ['dynamodb:GetItem','dynamodb:PutItem'],
      resources: [runManifest.tableArn],
    }));

    // 7) Lambdas
    const ingestFn = new lambda.DockerImageFunction(this, 'IngestEnvFn', {
      code: lambda.DockerImageCode.fromImageAsset(
        path.join(__dirname, '..', 'lambdas', 'ingest_env'),
        { platform: ecr_assets.Platform.LINUX_ARM64 }
      ),
      architecture: lambda.Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(2),
      environment: { LAKE_BUCKET: lake.bucketName },
    });
    lake.grantReadWrite(ingestFn);
    ingestFn.grantInvoke(sfnRole);

    const computeChecksumFn = new lambda.Function(this, 'ComputeChecksumFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'compute_checksum')),
      memorySize: 256,
      timeout: Duration.seconds(30),
      environment: { LAKE_BUCKET: lake.bucketName },
    });
    lake.grantRead(computeChecksumFn);
    computeChecksumFn.grantInvoke(sfnRole);

    const deriveWindowFn = new lambda.Function(this, 'DeriveWindowFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'derive_window')),
      memorySize: 128,
      timeout: Duration.seconds(10),
    });

    const dqCheckFn = new lambda.Function(this, 'DqCheckFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'dq_check')),
      memorySize: 512,
      timeout: Duration.minutes(2),
      environment: {
        LAKE_BUCKET: lake.bucketName,
        ATHENA_DB: 'turnstile_lake',
        ATHENA_WG: 'turnstile_wg',
      },
    });
    lake.grantReadWrite(dqCheckFn);
    dqCheckFn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'athena:StartQueryExecution','athena:GetQueryExecution','athena:GetQueryResults',
        'athena:StopQueryExecution','athena:GetWorkGroup'
      ],
      resources: ['*'],
    }));
    dqCheckFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['glue:GetDatabase','glue:GetDatabases','glue:GetTable','glue:GetTables','glue:GetPartition','glue:GetPartitions'],
      resources: ['*'],
    }));
    dqCheckFn.grantInvoke(sfnRole);

    // Quarantine writer — Node 20 + AWS SDK v3 inside (your file)
    const quarantineWriterFn = new lambda.Function(this, 'QuarantineWriterFn', {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambdas', 'quarantine_writer')),
      memorySize: 256,
      timeout: Duration.seconds(30),
      environment: { LAKE_BUCKET: lake.bucketName },
    });
    lake.grantWrite(quarantineWriterFn);
    quarantineWriterFn.grantInvoke(sfnRole);

    // ---------- Athena SQL ----------
    const bucket = lake.bucketName;

    const bronzeUsgs = `
CREATE EXTERNAL TABLE IF NOT EXISTS turnstile_lake.bronze_usgs (
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
LOCATION 's3://${bucket}/bronze/usgs/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.range'='2020/01/01/00,NOW',
  'projection.ingestion_date.format'='yyyy/MM/dd/HH',
  'projection.ingestion_date.interval'='1',
  'projection.ingestion_date.interval.unit'='HOURS',
  'storage.location.template'='s3://${bucket}/bronze/usgs/ingestion_date=${'${ingestion_date}'}/'
)`;

    const bronzeWeather = `
CREATE EXTERNAL TABLE IF NOT EXISTS turnstile_lake.bronze_weather (
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
LOCATION 's3://${bucket}/bronze/weather/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.ingestion_date.type'='date',
  'projection.ingestion_date.range'='2020/01/01/00,NOW',
  'projection.ingestion_date.format'='yyyy/MM/dd/HH',
  'projection.ingestion_date.interval'='1',
  'projection.ingestion_date.interval.unit'='HOURS',
  'storage.location.template'='s3://${bucket}/bronze/weather/ingestion_date=${'${ingestion_date}'}/'
)`;

    const silverUsgs = `
CREATE TABLE IF NOT EXISTS turnstile_lake.silver_usgs
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${bucket}/silver/usgs/',
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
  FROM turnstile_lake.bronze_usgs
)
WHERE rn = 1
`;

    const silverWeather = `
CREATE TABLE IF NOT EXISTS turnstile_lake.silver_weather
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${bucket}/silver/weather/',
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
  FROM turnstile_lake.bronze_weather
)
WHERE rn = 1
`;

    // ---------- Gold CTAS ----------
    const goldWeatherDaily = `
CREATE TABLE IF NOT EXISTS turnstile_lake.gold_weather_daily
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${bucket}/gold/weather_daily/',
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
FROM turnstile_lake.silver_weather
GROUP BY city, date_trunc('day', timestamp_utc)
`;

    const goldQuakesDaily = `
CREATE TABLE IF NOT EXISTS turnstile_lake.gold_quakes_daily
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${bucket}/gold/quakes_daily/',
  partitioned_by=ARRAY['day']
) AS
SELECT
  COUNT(*) AS quake_count,
  AVG(mag) AS avg_mag,
  MAX(mag) AS max_mag,
  SUM(CASE WHEN mag >= 4.0 THEN 1 ELSE 0 END) AS m4plus,
  date_trunc('day', time_utc) AS day
FROM turnstile_lake.silver_usgs
GROUP BY date_trunc('day', time_utc)
`;

    const goldQuakesWeatherDaily = `
CREATE TABLE IF NOT EXISTS turnstile_lake.gold_quakes_weather_daily
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='s3://${bucket}/gold/quakes_weather_daily/',
  partitioned_by=ARRAY['city','day']
) AS
SELECT
  w.avg_temp_c,
  w.min_temp_c,
  w.max_temp_c,
  w.sum_precip_mm,
  w.avg_wind_mps,
  q.quake_count,
  q.avg_mag,
  q.max_mag,
  q.m4plus,
  w.city,
  w.day
FROM turnstile_lake.gold_weather_daily w
LEFT JOIN turnstile_lake.gold_quakes_daily q
  ON q.day = w.day
`;

    // Helper to create Athena tasks that wait for completion
    const startQuery = (id: string, query: string, resultPath: string) =>
      new tasks.AthenaStartQueryExecution(this, id, {
        queryString: query,
        queryExecutionContext: { databaseName: 'turnstile_lake' },
        workGroup: 'turnstile_wg',
        integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        resultPath,
      });

    // Bronze + Silver tasks
    const createBronzeUsgs = startQuery('AthenaCreateBronzeUsgs', bronzeUsgs, '$.athena.bronze.usgs');
    const createBronzeWeather = startQuery('AthenaCreateBronzeWeather', bronzeWeather, '$.athena.bronze.weather');
    const buildSilverUsgs = startQuery('AthenaBuildSilverUsgs', silverUsgs, '$.athena.silver.usgs');
    const buildSilverWeather = startQuery('AthenaBuildSilverWeather', silverWeather, '$.athena.silver.weather');

    // Gold tasks
    const buildGoldWeatherDaily = startQuery('AthenaBuildGoldWeatherDaily', goldWeatherDaily, '$.athena.gold.weather_daily');
    const buildGoldQuakesDaily = startQuery('AthenaBuildGoldQuakesDaily', goldQuakesDaily, '$.athena.gold.quakes_daily');
    const buildGoldQuakesWeatherDaily = startQuery('AthenaBuildGoldQuakesWeatherDaily', goldQuakesWeatherDaily, '$.athena.gold.quakes_weather_daily');

    // ----- State machine nodes -----

    // 1) derive {date,hour} if missing
    const deriveWindow = new tasks.LambdaInvoke(this, 'DeriveWindow', {
      lambdaFunction: deriveWindowFn,
      resultPath: '$.derived',
    });

    // 2) adopt derived into $.params
    const adoptDerivedParams = new sfn.Pass(this, 'AdoptDerivedParams', {
      parameters: {
        params: {
          'date.$': '$.derived.Payload.date',
          'hour.$': '$.derived.Payload.hour',
        },
      },
      resultPath: '$',
    });

    // 3) no-op when params provided
    const useProvidedParams = new sfn.Pass(this, 'UseProvidedParams', { resultPath: '$' });

    // 4) choice to ensure params exist
    const ensureParams = new sfn.Choice(this, 'ParamsProvided?')
      .when(
        sfn.Condition.and(
          sfn.Condition.isPresent('$.params.date'),
          sfn.Condition.isPresent('$.params.hour')
        ),
        useProvidedParams
      )
      .otherwise(deriveWindow.next(adoptDerivedParams));

    const ingest = new tasks.LambdaInvoke(this, 'IngestSources', {
      lambdaFunction: ingestFn,
      payload: sfn.TaskInput.fromObject({ lakeBucket: lake.bucketName }),
      resultPath: '$.ingestResult',
    });

    const computeChecksum = new tasks.LambdaInvoke(this, 'ComputeChecksum', {
      lambdaFunction: computeChecksumFn,
      payload: sfn.TaskInput.fromObject({
        lakeBucket: lake.bucketName,
        source: 'weather',
        window: { date: sfn.JsonPath.stringAt('$.params.date'), hour: sfn.JsonPath.stringAt('$.params.hour') }
      }),
      resultPath: '$.checksum',
    });

    const manifestLookup = new tasks.DynamoGetItem(this, 'ManifestLookup', {
      table: runManifest,
      key: {
        'pk': tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour'))
        ),
        'sk': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$.checksum.Payload.checksum')),
      },
      resultPath: '$.manifest',
    });

    // -------------------- DQ section --------------------
    // Capture raw Lambda result with wrapper
    const dqChecks = new tasks.LambdaInvoke(this, 'DQChecks', {
      lambdaFunction: dqCheckFn,
      payload: sfn.TaskInput.fromObject({
        lakeBucket: lake.bucketName,
        db: 'turnstile_lake',
        wg: 'turnstile_wg',
        source: 'weather',
        window: { date: sfn.JsonPath.stringAt('$.params.date'), hour: sfn.JsonPath.stringAt('$.params.hour') }
      }),
      resultPath: '$.dqRaw',
    });

    // Map $.dqRaw.Payload.passed => $.dq.pass (and carry breadcrumbs)
    const projectDQ = new sfn.Pass(this, 'ProjectDQ', {
  parameters: {
    // take the exact boolean your lambda returns
    'pass.$': '$.dqRaw.Payload.passed',

    // keep the array fields simple; we don't need to format strings here
    reasons: [],
    offenders: [],

    // preserve useful context without intrinsics
    meta: {
      'reportKey.$': '$.dqRaw.Payload.reportKey',
      'count.$': '$.dqRaw.Payload.count',
    },
  },
  resultPath: '$.dq',
});

    // Gate & quarantine
    const dqGate = new sfn.Choice(this, 'DQGate (block on fail)');

    const writeQuarantine = new tasks.LambdaInvoke(this, 'WriteQuarantine', {
      lambdaFunction: quarantineWriterFn,
      payload: sfn.TaskInput.fromObject({
        lakeBucket: lake.bucketName,
        source: 'weather',
        window: {
          'date.$': '$.params.date',
          'hour.$': '$.params.hour'
        },
        'dq.$': '$.dq'
      }),
      resultPath: sfn.JsonPath.DISCARD,
    });

    const dqFailed = new sfn.Fail(this, 'DQFailed', {
      error: 'DQQualityError',
      cause: 'DQ checks failed; wrote to quarantine',
    });

    const processBranch =
      (() => createBronzeUsgs
        .next(createBronzeWeather)
        .next(buildSilverUsgs)
        .next(buildSilverWeather)
        .next(dqChecks)
        .next(projectDQ)
        .next(
          dqGate
            .when(sfn.Condition.booleanEquals('$.dq.pass', true),
              buildGoldWeatherDaily
                .next(buildGoldQuakesDaily)
                .next(buildGoldQuakesWeatherDaily)
                .next(new tasks.DynamoPutItem(this, 'RecordManifestSuccess', {
                  table: runManifest,
                  item: {
                    'pk': tasks.DynamoAttributeValue.fromString(
                      sfn.JsonPath.format('weather#{}T{}:00Z', sfn.JsonPath.stringAt('$.params.date'), sfn.JsonPath.stringAt('$.params.hour'))
                    ),
                    'sk': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$.checksum.Payload.checksum')),
                    'status': tasks.DynamoAttributeValue.fromString('SUCCEEDED'),
                    'started_at': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.Execution.StartTime')),
                    'finished_at': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
                  },
                  resultPath: sfn.JsonPath.DISCARD,
                }))
                .next(new sfn.Succeed(this, 'Done'))
            )
            .otherwise(writeQuarantine.next(dqFailed))
        ))();

    const choiceSkip =
      new sfn.Choice(this, 'AlreadyProcessed?')
        .when(sfn.Condition.isPresent('$.manifest.Item.pk.S'), new sfn.Succeed(this, 'SkipWindow'))
        .otherwise(processBranch);

    // IMPORTANT: continue AFTER the choice using .afterwards()
    const definition = sfn.Chain
      .start(ensureParams.afterwards())
      .next(ingest)
      .next(computeChecksum)
      .next(manifestLookup)
      .next(choiceSkip);

    const stateMachine = new sfn.StateMachine(this, 'TurnstileStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      tracingEnabled: true,
      role: sfnRole,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      logs: { destination: sfnLogGroup, level: sfn.LogLevel.ALL, includeExecutionData: true },
    });

    // ---- EventBridge schedule ----
    const rule = new events.Rule(this, 'HourlyTurnstileRun', {
      schedule: events.Schedule.cron({ minute: '15', hour: '*' }),
    });
    rule.addTarget(new targets.SfnStateMachine(stateMachine));

    // ---- Alerts ----
    const opsTopic = new sns.Topic(this, 'TurnstileOpsTopic', {
      displayName: 'Turnstile Alerts',
    });

    new cloudwatch.Alarm(this, 'SfnFailedAlarm', {
      metric: stateMachine.metricFailed(),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    }).addAlarmAction(new cw_actions.SnsAction(opsTopic));

    new cloudwatch.Alarm(this, 'IngestFnErrorsAlarm', {
      metric: ingestFn.metricErrors({ period: Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
    }).addAlarmAction(new cw_actions.SnsAction(opsTopic));

    new cloudwatch.Alarm(this, 'DqFnErrorsAlarm', {
      metric: dqCheckFn.metricErrors({ period: Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
    }).addAlarmAction(new cw_actions.SnsAction(opsTopic));

    // ---- Outputs ----
    new CfnOutput(this, 'LakeBucketName', { value: lake.bucketName });
    new CfnOutput(this, 'GlueDatabaseName', { value: 'turnstile_lake' });
    new CfnOutput(this, 'AthenaWorkgroupOutput', { value: workgroup.name ?? 'turnstile_wg' });
    new CfnOutput(this, 'RunManifestTable', { value: runManifest.tableName });
    new CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
  }
}
