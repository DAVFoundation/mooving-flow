local params = std.extVar('__ksonnet/params');
local globals = import 'globals.libsonnet';
local GCP_PROJECT = '<<GCP PROJECT>>';
local jobProps = [
  { key: 'CASSANDRA_SEEDS', value: 'cassandra-0.cassandra.cassandra.svc.cluster.local' },
  { key: 'CASSANDRA_PORT', value: '9042' },
  { key: 'KAFKA_SEEDS', value: 'broker.kafka.svc.cluster.local' },
  { key: 'KAFKA_PORT', value: '9092' },
  { key: 'GCP_PROJECT', value: GCP_PROJECT },
  { key: 'GCP_BUCKET_PATH', value: 'gs://<<GCS PATH>>' },
];

local envParams = params {
  components+: {
    'rider-payment'+: {
      GCP_PROJECT: GCP_PROJECT,
      bluesnapUrl: 'https://sandbox.bluesnap.com',
      props: jobProps,
    },
    'daily-stats'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'end-ride'+: {
      GCP_PROJECT: GCP_PROJECT,
      bluesnapUrl: 'https://sandbox.bluesnap.com',
      gcsBucket: '<<GCS BUCKET NAME>>',
      GCP_CREDENTIALS_FILE_NAME: 'service-account.json',
      props: jobProps,
    },
    'lock-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'unlock-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      bluesnapUrl: 'https://sandbox.bluesnap.com',
      props: jobProps,
    },
    'search-vehicles'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'cordon-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'uncordon-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'garage-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'ungarage-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'cordon-garage-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'ungarage-uncordon-vehicle'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'dav-conversion-rate'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'update-dav-balance'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
    'token-txn'+: {
      GCP_PROJECT: GCP_PROJECT,
      contractAddress: '<<YOUR ETH WALLET ADDRS>>',
      ethNodeUrl: 'https://rinkeby.infura.io/v3/5153bcce69374a518c4afe33b05fe4d6',
      props: jobProps,
    },
    'vehicle-updates'+: {
      GCP_PROJECT: GCP_PROJECT,
      props: jobProps,
    },
  },
};

{
  components: {
    [x]: envParams.components[x] + globals
    for x in std.objectFields(envParams.components)
  },
}
