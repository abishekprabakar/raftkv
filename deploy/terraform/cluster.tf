# The Kubernetes cluster: one kind node, which is one Docker container.

locals {
  # Kept out of ~/.kube/config on purpose, so this never touches the kubeconfig
  # of any other cluster. Use it with `kubectl --kubeconfig` or KUBECONFIG.
  kubeconfig_path = abspath("${path.module}/kubeconfig")

  # NodePorts inside the cluster. Docker maps each one to a port on 127.0.0.1,
  # which is the only way in from the host on macOS, where the node's own
  # address can't be reached.
  client_node_port = 30700
  pod_node_ports   = [for i in range(var.replicas) : 30701 + i]
  client_host_port = var.host_port
  pod_host_ports   = [for i in range(var.replicas) : var.host_port + 1 + i]
}

resource "kind_cluster" "this" {
  name            = var.cluster_name
  node_image      = var.node_image
  kubeconfig_path = local.kubeconfig_path
  wait_for_ready  = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      extra_port_mappings {
        container_port = local.client_node_port
        host_port      = local.client_host_port
        listen_address = "127.0.0.1"
      }

      dynamic "extra_port_mappings" {
        for_each = range(var.replicas)
        content {
          container_port = local.pod_node_ports[extra_port_mappings.value]
          host_port      = local.pod_host_ports[extra_port_mappings.value]
          listen_address = "127.0.0.1"
        }
      }
    }
  }
}

# The kubernetes provider gets its credentials straight from the cluster above.
# Nothing is read from a kubeconfig file, so a first apply can create the cluster
# and deploy into it in one run.
provider "kubernetes" {
  host                   = kind_cluster.this.endpoint
  client_certificate     = kind_cluster.this.client_certificate
  client_key             = kind_cluster.this.client_key
  cluster_ca_certificate = kind_cluster.this.cluster_ca_certificate
}
