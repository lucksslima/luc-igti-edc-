provider "aws" {
    region= var.aws.region
}
  
#centralizar o arquivo de controle de estado do terraform

terraform  {
    backend "s3" {
        bucket = "terraform-state-igti-luc"
        key = "state/igte/edc/mod1/terraform.tfstate"
        region = "us-east-1"
    }
}