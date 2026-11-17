# Keepalived — VRRP for HAProxy failover across MULTIPLE hosts.
#
# NOTE: on a single Docker host a VIP adds nothing (there is nothing to fail
# over to), so this service is NOT part of the default stack. If you later run
# a second identical host (docker compose up haproxy + keepalived on both),
# enable it:  ./lakehouse.sh up ops-ha
#
# Requirements: NET_ADMIN capability, host networking, unique VRRP priority
# per host (100 on the primary, 90 on the standby).
