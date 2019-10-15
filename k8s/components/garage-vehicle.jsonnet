local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['garage-vehicle'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'garage-vehicle',
    namespace: 'flows',
    labels: {
      app: 'garage-vehicle',
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
    mainClass: 'org.dav.vehicle_rider.GarageFlow',
    props: params.props,
  },
}
