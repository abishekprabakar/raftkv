#!/bin/sh
# Runs terraform in deploy/terraform and hides the cluster's credentials in what
# it prints:
#
#   scripts/k8s/terraform.sh apply -auto-approve
#
# The kind provider does not mark the cluster's client key as sensitive, so
# `terraform destroy` prints it in full when it lists what it is about to remove.
# The key only opens a local cluster that is gone a few seconds later, but a
# private key has no place in a terminal scrollback, a CI log or a file that might
# be committed. Output is passed on line by line as it arrives, and the exit
# status is terraform's own.
set -u

dir=$(cd "$(dirname "$0")/../../deploy/terraform" && pwd)
status=$(mktemp "${TMPDIR:-/tmp}/raftkv-tf.XXXXXX")
trap 'rm -f "$status"' EXIT

{
	terraform -chdir="$dir" "$@" 2>&1
	echo $? >"$status"
} | awk '
	# A PEM block, whatever it holds, becomes one line.
	/-----BEGIN [A-Z ]*-----/ {
		if (!hidden) { sub(/-----BEGIN.*/, "[redacted]"); print; fflush() }
		hidden = 1
		next
	}
	hidden && /-----END [A-Z ]*-----/ { hidden = 0; next }
	hidden { next }
	# The same material inside a kubeconfig, as base64 on one line.
	/(client-key|client-certificate|certificate-authority)-data:/ { sub(/-data:.*/, "-data: [redacted]") }
	{ print; fflush() }
'
exit "$(cat "$status")"
