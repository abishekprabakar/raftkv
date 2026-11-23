variable "cluster_name" {
  description = "Name of the kind cluster. The node's container is <name>-control-plane."
  type        = string
  default     = "raftkv"
}

variable "node_image" {
  description = "The kind node image, pinned by digest. It decides the Kubernetes version."
  type        = string
  default     = "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
}

variable "namespace" {
  description = "Namespace for everything raftkv."
  type        = string
  default     = "raftkv"
}

variable "replicas" {
  description = "Number of raftkv nodes. Membership is fixed: this number is written into every node's --peers list, so changing it means a new cluster, not a scale-up."
  type        = number
  default     = 3

  validation {
    condition     = var.replicas >= 1 && var.replicas % 2 == 1
    error_message = "Use an odd number of nodes: an even number tolerates no more failures than the odd number below it."
  }
}

variable "image_tag" {
  description = "Tag of the raftkv image to build and run. It is also the version the binary reports. Changing it rolls the StatefulSet."
  type        = string
  default     = "v1"
}

variable "build_context" {
  description = "Directory to build the image from. Default: this repository. Tests point it at a copy with a bug planted."
  type        = string
  default     = null
}

variable "persistent" {
  description = "Keep each node's data on a PersistentVolumeClaim. false puts it on an emptyDir, which dies with the pod: only a test that shows why the claims matter should ever set it."
  type        = bool
  default     = true
}

variable "host_port" {
  description = "First of replicas+1 ports on 127.0.0.1. The client Service gets this one, and pod N gets host_port+1+N."
  type        = number
  default     = 17000
}
