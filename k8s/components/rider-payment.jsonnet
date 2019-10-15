local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['rider-payment'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'rider-payment',
    namespace: 'flows',
    labels: {
      app: 'rider-payment',
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
    mainClass: 'org.dav.vehicle_rider.rider_payment.RiderPaymentFlow',
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
        key: 'payment_interval_millis',
        value: '600000',
      },
    ],
  },
}
