provider "github" {
    owner = "Mobivity"
    token = var.gh_argo_pat
}

locals {
    service_name = "broadcast-builder-app"
    environment = "${terraform.workspace}"
    namespace = "${local.service_name}"
}