# Exact versions, so the same configuration builds the same cluster on every
# machine and in CI. .terraform.lock.hcl, committed beside this file, pins the
# providers' checksums too.
terraform {
  required_version = ">= 1.9.0"

  required_providers {
    # Creates the kind cluster. kind runs a whole Kubernetes node inside one
    # Docker container, and this provider has kind built in, so the kind
    # command-line tool is not needed.
    kind = {
      source  = "tehcyx/kind"
      version = "0.13.0"
    }
    # Creates the Kubernetes objects inside it.
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "3.5.0"
    }
  }
}
