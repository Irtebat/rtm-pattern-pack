output "vpc_id" {
  value = data.aws_vpc.workspace.id
}

output "vpc_cidr" {
  value = data.aws_vpc.workspace.cidr_block
}

output "private_subnet_ids" {
  description = "Private subnets ordered by availability zone."
  value       = local.private_subnet_ids
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}

output "flink_security_group_id" {
  value = aws_security_group.flink.id
}

output "loadgen_security_group_id" {
  value = aws_security_group.loadgen.id
}

output "workspace_security_group_id" {
  value = data.aws_security_group.workspace.id
}

output "availability_zone_count" {
  value = length(local.private_subnet_ids)
}
