region = "us-east-2"

#### Networking variables ####
vpc_cidr = "10.0.0.0/16"

public_subnets_cidr = "10.0.0.0/24"

private_subnets_cidr = [
    "10.0.1.0/24",
    "10.0.2.0/24"]

availability_zone = [ 
    "us-east-2a",
    "us-east-2b"]

#### EKS variables ####
cluster_name = "airflow-eks-data-bootcamp"

cluster_version = "1.20"

instance_type_group1 = "t2.small"

instance_type_group2 = "t2.medium"

asg_desired_capacity_group1 = 2

asg_desired_capacity_group2 = 1

#### EC2 variables ####
number_of_instances = 5

ec2_name = "Bastion Host"

ec2_security_group_name = "Bastion Host SG"

ec2_security_group_description = "Bastion Host instances SG"

ec2_ami = "ami-00dfe2c7ce89a450b" # Amazon Linux 2 AMI (64-bit x86)  Free Tier eligible

ec2_instance_type = "t2.micro" # Free Tier eligible

 ec2_ssh_key_name = "bastion-host-key"

 ec2_ssh_public_key_path = "./bastion-host-key"


#### RDS ####
allocated_storage = 20

db_engine = "postgres"

db_port = 5432

engine_version = "12.8"

instance_type = "db.t2.micro"

database_name = "Schemadb"

db_username = "Login"

db_password = "Password"

publicly_accessible = true

privileges  = ["ALL"]

#### S3 variables ####
bucket_prefix = "s3-data-bootcamp-megc"

acl = "private"

versioning = false

