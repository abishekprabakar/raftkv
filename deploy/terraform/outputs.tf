output "kubeconfig" {
  description = "Use with `kubectl --kubeconfig` or KUBECONFIG."
  value       = local.kubeconfig_path
}

output "namespace" {
  value = var.namespace
}

output "statefulset" {
  value = local.name
}

output "image" {
  value = local.image
}

output "node_container" {
  description = "The Docker container that is the Kubernetes node."
  value       = "${var.cluster_name}-control-plane"
}

output "client_address" {
  description = "The client Service, as the host reaches it."
  value       = "127.0.0.1:${local.client_host_port}"
}

output "client_address_in_cluster" {
  description = "The client Service, as a pod reaches it."
  value       = "${local.name}-client.${var.namespace}.svc.cluster.local:${local.grpc_port}"
}

output "nodes" {
  description = "Each raftkv node: its pod, its Raft ID, the address nodes hand out for it, and the address the host reaches it at."
  value = [
    for i, pod in local.pod_names : {
      pod          = pod
      id           = i + 1
      address      = local.pod_addresses[i]
      host_address = "127.0.0.1:${local.pod_host_ports[i]}"
    }
  ]
}
