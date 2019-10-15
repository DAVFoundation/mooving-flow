local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['search-vehicles'];
local version = std.extVar('IMAGE_VERSION');

{
  apiVersion: 'operators.srfrnk.com/v1',
  kind: 'FlinkJob',
  metadata: {
    name: 'search-vehicles',
    namespace: 'flows',
    labels: {
      app: 'search-vehicles',
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
    mainClass: 'org.dav.vehicle_rider.SearchVehiclesFlow',
    props: params.props,
  },
}
