local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['daily-stats'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'daily-stats',
    namespace: 'flows',
    labels: {
      app: 'daily-stats',
      version: version,
    },
  },
  spec: {
    version: version,
    cron: {
      schedule: '*/10 * * * *',
      concurrencyPolicy: 'Allow',
    },
    jobManagerUrl: 'flink-jobmanager.flink:8081',
    jarImage: 'gcr.io/' + params.GCP_PROJECT + '/flow:' + version,
    jarPath: '/jars/dav-flow-0.1.jar',
    mainClass: 'org.dav.vehicle_rider.owner_stats.DailyOwnerStatsFlow',
    props: params.props,
  },
}
