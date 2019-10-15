local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['unlock-vehicle'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'unlock-vehicle',
    namespace: 'flows',
    labels: {
      app: 'unlock-vehicle',
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
    mainClass: 'org.dav.vehicle_rider.UnlockVehicleFlow',
    props: params.props + [
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
        key: 'default_auth_amount_minutes',
        value: '5.0',
      },
    ],
  },
}
