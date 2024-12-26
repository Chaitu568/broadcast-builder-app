locals {
    helm_values_files = [
        "values.yaml",
        "${terraform.workspace}.yaml"
    ]

    argo_dest_manifest = "${terraform.workspace}/${local.service_name}/manifest.yaml"

    # values_files = [
    #     for file in local.helm_values_files : file("${path.module}/../k8s/${file}")
    # ]

    values_files = [
        for file_name in local.helm_values_files :
        file("${path.module}/../k8s/${file_name}")
        if fileexists("${path.module}/../k8s/${file_name}")
    ]

    secrets = [

    ]

    app_config = {
        app = {
            container = {
                env = concat(
                [
                    for secret in local.secrets : {
                        name  = upper("SECRET_${split("/", secret.name)[2]}_ARN")
                        value = substr(secret.arn, 0, length(secret.arn) - 7)
                    }
                ]
                )
            }
        }
    }

    all_values = concat(
        local.values_files, 
        # [
        #     resource.local_file.tf_values_yaml.content
        # ]
    )
}

# resource "local_file" "tf_values_yaml" {
#     content = yamlencode(local.app_config)
#     filename = "${path.module}/../k8s/tf-values.yaml"
# }
# added comments for demonstration
# more comments
#
#

data "helm_template" "local_chart" {
    name = "${local.service_name}"
    chart = "../k8s"
    values = local.all_values
}

resource "local_file" "helm_chart_manifest" {
    content = data.helm_template.local_chart.manifest
    filename = "${path.module}/../k8s/manifest.yaml"
}

# resource "github_repository" "app_infra" {
#   name      = "Mobivity/application-infrastructure"
#   auto_init = true
# }

# resource "github_repository_file" "foo" {
#   repository          = github_repository.app_infra.name
#   branch              = "main"
#   file                = ".gitignore"
#   content             = "**/*.tfstate"
#   commit_message      = "Managed by Terraform"
#   commit_author       = "Terraform User"
#   commit_email        = "terraform@example.com"
#   overwrite_on_create = true
# }

resource "github_repository_file" "manifest_file" {
#   provider            = github  # Ensure you're using the correct GitHub provider
  repository          = "application-infrastructure"  # Name of the existing repo
  branch              = "current"  # Or the branch you want to commit to
  file                = "${terraform.workspace}/builderapp/manifest.yaml"  # Desired path in the repo
#   content             = file("${path.module}/../k8s/manifest.yaml")
    content             = data.helm_template.local_chart.manifest
  commit_message      = "Add generated manifest for broadcast-builder"
  commit_author       = "Chaitanya"
  commit_email        = "chaitanya.poluri@mobivity.com"
  overwrite_on_create = true
}
