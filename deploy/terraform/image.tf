# The raftkv image: built with Docker on the host, then copied into the kind
# node. The node has its own image store and can't see the host's, and there is
# no registry, so the image has to be handed over.

locals {
  build_context = abspath(coalesce(var.build_context, "${path.module}/../.."))
  image         = "raftkv:${var.image_tag}"

  # Everything the image is built from. If any of it changes, the image is built
  # and loaded again on the next apply, and the pods are replaced. Tests are left
  # out: they are not in the binaries, so changing one should not roll the pods.
  source_files = sort([
    for f in setunion(
      fileset(local.build_context, "cmd/**"),
      fileset(local.build_context, "internal/**"),
      ["go.mod", "go.sum", "Dockerfile"],
    ) : f if !endswith(f, "_test.go")
  ])
  source_hash = sha256(join("\n", [for f in local.source_files : "${f} ${filesha256("${local.build_context}/${f}")}"]))
}

resource "terraform_data" "image" {
  triggers_replace = {
    image  = local.image
    source = local.source_hash
  }

  provisioner "local-exec" {
    command = "docker build --quiet --build-arg VERSION=${var.image_tag} --tag ${local.image} ${local.build_context}"
  }
}

# What `kind load docker-image` does: save the image and import it into the
# node's containerd. Destroying this leaves the image in the node, so the tag a
# rolling update moves away from is still there for any pod that needs it.
resource "kind_load" "image" {
  cluster_name = kind_cluster.this.name
  image        = local.image

  lifecycle {
    replace_triggered_by = [terraform_data.image]
  }
}
