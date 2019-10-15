local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['token-txn'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'token-txn',
    namespace: 'flows',
    labels: {
      app: 'token-txn',
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
    mainClass: 'org.dav.vehicle_rider.token_payment.TokenPaymentFlow',
    props: params.props + [
      {
        key: 'contract_address',
        value: params.contractAddress,
      },
      {
        key: 'eth_node_url',
        value: params.ethNodeUrl,
      },
      {
        key: 'rider_private_key',
        valueFrom: {
          secretKeyRef: {
            name: 'private-keys',
            key: 'rider_private_key',
          },
        },
      },
      {
        key: 'network_operator_private_key',
        valueFrom: {
          secretKeyRef: {
            name: 'private-keys',
            key: 'network_operator_private_key',
          },
        },
      },
    ],
  },
}
