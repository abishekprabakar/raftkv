# raftkv on Kubernetes: a StatefulSet, the Services around it, and a
# PodDisruptionBudget.

locals {
  name   = "raftkv"
  labels = { "app.kubernetes.io/name" = "raftkv" }

  grpc_port   = 7000
  health_port = 8080

  # A pod of a StatefulSet gets a DNS name through the headless Service:
  # <pod>.<service>.<namespace>.svc.cluster.local. It stays the same when the
  # pod is replaced, though the pod's IP address changes every time.
  pod_names = [for i in range(var.replicas) : "${local.name}-${i}"]
  pod_addresses = [
    for pod in local.pod_names :
    "${pod}.${local.name}-peers.${var.namespace}.svc.cluster.local:${local.grpc_port}"
  ]

  # The same list for every node: "1=<address of raftkv-0>,2=...". Node IDs start
  # at 1, and --id-from-hostname makes pod raftkv-N node N+1.
  peers = join(",", [for i, addr in local.pod_addresses : "${i + 1}=${addr}"])
}

resource "kubernetes_namespace_v1" "raftkv" {
  metadata {
    name = var.namespace
  }
}

# Headless: no virtual IP and no load balancing. It exists to give each pod a DNS
# name of its own, which is how the nodes find each other.
resource "kubernetes_service_v1" "peers" {
  metadata {
    name      = "${local.name}-peers"
    namespace = kubernetes_namespace_v1.raftkv.metadata[0].name
    labels    = local.labels
  }

  spec {
    cluster_ip = "None"
    selector   = local.labels

    # Without this, a pod's name resolves only once the pod is ready. A node is
    # ready only after it has joined the cluster, and it can't join a cluster it
    # can't find, so nothing would ever start.
    publish_not_ready_addresses = true

    port {
      name        = "grpc"
      port        = local.grpc_port
      target_port = "grpc"
    }
  }
}

# For clients: one stable name that reaches any ready node. A node that isn't the
# leader answers with the leader's address, and the client library goes there.
# NodePort so that the host can use it too, through the port kind maps.
resource "kubernetes_service_v1" "client" {
  metadata {
    name      = "${local.name}-client"
    namespace = kubernetes_namespace_v1.raftkv.metadata[0].name
    labels    = local.labels
  }

  spec {
    type     = "NodePort"
    selector = local.labels

    port {
      name        = "grpc"
      port        = local.grpc_port
      target_port = "grpc"
      node_port   = local.client_node_port
    }
  }
}

# One Service per pod, only so the host can reach a particular node: a client
# outside the cluster that is redirected to the leader has to get to that pod and
# no other. Inside the cluster nobody needs these: the DNS names above are enough.
resource "kubernetes_service_v1" "pod" {
  count = var.replicas

  metadata {
    name      = "${local.pod_names[count.index]}-host"
    namespace = kubernetes_namespace_v1.raftkv.metadata[0].name
    labels    = local.labels
  }

  spec {
    type = "NodePort"
    # The StatefulSet controller puts this label on each pod it creates.
    selector = merge(local.labels, {
      "statefulset.kubernetes.io/pod-name" = local.pod_names[count.index]
    })
    # A node that isn't ready still answers Status, and the tests ask it.
    publish_not_ready_addresses = true

    port {
      name        = "grpc"
      port        = local.grpc_port
      target_port = "grpc"
      node_port   = local.pod_node_ports[count.index]
    }
  }
}

resource "kubernetes_stateful_set_v1" "raftkv" {
  metadata {
    name      = local.name
    namespace = kubernetes_namespace_v1.raftkv.metadata[0].name
    labels    = local.labels
  }

  # apply returns only when every pod is ready, on the first apply and after
  # every change. With the readiness rule below, that means a working cluster.
  wait_for_rollout = true

  spec {
    service_name = kubernetes_service_v1.peers.metadata[0].name
    replicas     = var.replicas

    # Start all pods together. The default starts pod 0 and waits for it to be
    # ready before starting pod 1, and one node of three can never be ready: it
    # has no majority to elect a leader with.
    pod_management_policy = "Parallel"

    # Updates still go one pod at a time, highest ordinal first, and wait for
    # each replaced pod to be ready before touching the next.
    update_strategy {
      type = "RollingUpdate"
    }

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels

        # The image is rebuilt under the same tag when the source changes, and a
        # pod template that still says "raftkv:v1" would not roll. Recording the
        # source's hash here makes a rebuild a change to the template, so the
        # running pods are replaced with the new build.
        annotations = {
          "raftkv/source-hash" = local.source_hash
        }
      }

      spec {
        # raftkvd closes its files and exits within milliseconds of SIGTERM.
        termination_grace_period_seconds = 10

        security_context {
          run_as_non_root = true
          run_as_user     = 65532
          run_as_group    = 65532
          fs_group        = 65532

          seccomp_profile {
            type = "RuntimeDefault"
          }
        }

        # Spread the nodes over machines when there is more than one. A kind
        # cluster has one, so this changes nothing here, and on a real cluster
        # it keeps one machine's failure from taking a majority.
        topology_spread_constraint {
          max_skew           = 1
          topology_key       = "kubernetes.io/hostname"
          when_unsatisfiable = "ScheduleAnyway"

          label_selector {
            match_labels = local.labels
          }
        }

        container {
          name              = "raftkvd"
          image             = local.image
          image_pull_policy = "IfNotPresent"

          args = [
            "--id-from-hostname",
            "--peers=${local.peers}",
            "--data=/data",
            "--health=:${local.health_port}",
          ]

          port {
            name           = "grpc"
            container_port = local.grpc_port
          }

          port {
            name           = "health"
            container_port = local.health_port
          }

          # Startup: up to 60 s to open the files and replay the WAL. Until it
          # passes, the other two probes don't run, so a long recovery isn't
          # mistaken for a hang and restarted in the middle.
          startup_probe {
            http_get {
              path = "/livez"
              port = "health"
            }
            period_seconds    = 1
            timeout_seconds   = 2
            failure_threshold = 60
          }

          # Liveness: is the process working? Fails only if the node's loop
          # stops answering, and then the kubelet restarts the container.
          liveness_probe {
            http_get {
              path = "/livez"
              port = "health"
            }
            period_seconds    = 5
            timeout_seconds   = 2
            failure_threshold = 3
          }

          # Readiness: is the node useful to the cluster right now? A follower
          # must be following a leader and caught up; a leader must be confirmed
          # by a majority. This gates the client Service, the rolling update and
          # the disruption budget. A failure here never restarts anything.
          readiness_probe {
            http_get {
              path = "/readyz"
              port = "health"
            }
            period_seconds    = 1
            timeout_seconds   = 2
            failure_threshold = 3
            success_threshold = 1
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              memory = "512Mi"
            }
          }

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true

            capabilities {
              drop = ["ALL"]
            }
          }

          volume_mount {
            name       = "data"
            mount_path = "/data"
          }
        }

        # Only with persistent = false: storage that is deleted with the pod.
        dynamic "volume" {
          for_each = var.persistent ? [] : [1]
          content {
            name = "data"
            empty_dir {}
          }
        }
      }
    }

    # One claim per pod, named data-raftkv-N. The claim outlives the pod, and the
    # replacement pod of the same name gets the same claim back: the Raft log and
    # the SSTables are still there. kind's default StorageClass keeps the files
    # in a directory on the node.
    dynamic "volume_claim_template" {
      for_each = var.persistent ? [1] : []
      content {
        metadata {
          name = "data"
        }

        spec {
          access_modes = ["ReadWriteOnce"]

          resources {
            requests = {
              storage = "1Gi"
            }
          }
        }
      }
    }
  }

  # The image has to be in the node before a pod asks for it.
  depends_on = [kind_load.image]
}

# Voluntary disruptions (a node drain, a cluster upgrade) may take one raftkv pod
# at a time, which always leaves a majority of three. It does nothing about
# crashes, and nothing about a pod that is deleted directly.
resource "kubernetes_pod_disruption_budget_v1" "raftkv" {
  metadata {
    name      = local.name
    namespace = kubernetes_namespace_v1.raftkv.metadata[0].name
    labels    = local.labels
  }

  spec {
    max_unavailable = "1"

    selector {
      match_labels = local.labels
    }
  }
}
