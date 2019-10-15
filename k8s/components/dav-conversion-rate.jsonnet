local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['dav-conversion-rate'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'dav-conversion-rate',
    namespace: 'flows',
    labels: {
      app: 'dav-conversion-rate',
      version: version,
    },
  },
  spec: {
    cron: {
      schedule: '*/6 * * * *',
      concurrencyPolicy: 'Allow',
    },
    jobManagerUrl: 'flink-jobmanager.flink:8081',
    jarImage: 'gcr.io/' + params.GCP_PROJECT + '/flow:' + version,
    jarPath: '/jars/dav-flow-0.1.jar',
    mainClass: 'org.dav.vehicle_rider.rider_payment.DavConversionRates',
    props: params.props + [
      {
        key: 'CMC_API_KEY',
        valueFrom: {
          secretKeyRef: {
            name: 'dav-rate-update-job',
            key: 'CMC_API_KEY',
          },
        },
      }
    ],
  },
}
