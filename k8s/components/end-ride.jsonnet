local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['end-ride'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'end-ride',
    namespace: 'flows',
    labels: {
      app: 'end-ride',
      version: version,
    },
  },
  spec: {
    version: version,
    streaming: {
      replicas: 1,
    },
    jobManagerUrl: 'flink-jobmanager.flink:8081',
    jarImage: 'gcr.io/' + params.GCP_PROJECT + '/flow:' + version,
    jarPath: '/jars/dav-flow-0.1.jar',
    mainClass: 'org.dav.vehicle_rider.EndRideFlow',
    env: [
      {
        name: 'GOOGLE_APPLICATION_CREDENTIALS',
        value: '/gcp-credentials/' + params.GCP_CREDENTIALS_FILE_NAME,
      },
    ],
    props: params.props + [
      {
        key: 'timezone_api_key',
        valueFrom: {
          secretKeyRef: {
            name: 'timezone',
            key: 'TIMEZONE_API_KEY',
          },
        },
      },
      {
        key: 'bluesnap_api_user',
        valueFrom: {
          secretKeyRef: {
            name: 'payment-service',
            key: 'bluesnap_api_user',
          },
        },
      },
      {
        key: 'bluesnap_api_pass',
        valueFrom: {
          secretKeyRef: {
            name: 'payment-service',
            key: 'bluesnap_api_pass',
          },
        },
      },
      {
        key: 'bluesnap_url',
        value: params.bluesnapUrl,
      },
      {
        key: 'gcs_bucket',
        value: params.gcsBucket,
      },
    ],
  },
}
