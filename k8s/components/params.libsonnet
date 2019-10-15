{
  global: {
    // User-defined global parameters; accessible to all component and environments, Ex:
    // replicas: 4,
  },
  components: {
    'lock-vehicle': { GCP_PROJECT: '', props: [] },
    'unlock-vehicle': { GCP_PROJECT: '', bluesnapUrl: '', props: [] },
    'search-vehicles': { GCP_PROJECT: '', props: [] },
    'cordon-vehicle': { GCP_PROJECT: '', props: [] },
    'uncordon-vehicle': { GCP_PROJECT: '', props: [] },
    'garage-vehicle': { GCP_PROJECT: '', props: [] },
    'ungarage-vehicle': { GCP_PROJECT: '', props: [] },
    'cordon-garage-vehicle': { GCP_PROJECT: '', props: [] },
    'ungarage-uncordon-vehicle': { GCP_PROJECT: '', props: [] },
    'daily-stats': { GCP_PROJECT: '', props: [] },
    'cassandra-backup': { GCP_PROJECT: '', GCP_CREDENTIALS_FILE_NAME: '', props: [] },
    'rider-payment': { GCP_PROJECT: '', props: [], bluesnapUrl: '' },
    'vehicle-updates': { GCP_PROJECT: '', props: [] },
    'update-dav-balance': { GCP_PROJECT: '', props: [] },
    // Component-level parameters, defined initially from 'ks prototype use ...'
    // Each object below should correspond to a component in the components/ directory
  },
}
